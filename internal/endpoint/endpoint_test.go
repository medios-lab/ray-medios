package endpoint

// Contract tests for the endpoint over HTTPS. Each test names the item of
// docs/endpoint-contract.md it checks.

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/medios-lab/ray-medios/internal/store"
)

const (
	clusterURL = "/apis/ray.io/v1/namespaces/default/rayclusters/test"
	podsURL    = "/api/v1/namespaces/default/pods?labelSelector=ray.io/cluster%3Dtest"
	record     = `{"apiVersion":"ray.io/v1","kind":"RayCluster","metadata":{"name":"test","namespace":"default"},
		"spec":{"headGroupSpec":{"rayStartParams":{"num-cpus":"0"}},
		"workerGroupSpecs":[{"groupName":"small","replicas":1,"minReplicas":0,"maxReplicas":3}]}}`
)

type pki struct {
	files  TLSFiles
	pool   *x509.CertPool
	client tls.Certificate
}

func issue(t *testing.T, dir, name string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, tmpl *x509.Certificate) (string, string, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl.SerialNumber, _ = rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl.NotBefore, tmpl.NotAfter = time.Now().Add(-time.Minute), time.Now().Add(time.Hour)
	parent, parentKey := ca, caKey
	if ca == nil {
		parent, parentKey = tmpl, key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	certFile, keyFile := filepath.Join(dir, name+".pem"), filepath.Join(dir, name+".key")
	_ = os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	_ = os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600)
	cert, _ := x509.ParseCertificate(der)
	return certFile, keyFile, cert, key
}

func newPKI(t *testing.T) pki {
	t.Helper()
	dir := t.TempDir()
	caFile, _, ca, caKey := issue(t, dir, "ca", nil, nil, &x509.Certificate{
		Subject: pkix.Name{CommonName: "test CA"}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign})
	serverCert, serverKey, _, _ := issue(t, dir, "server", ca, caKey, &x509.Certificate{
		Subject: pkix.Name{CommonName: "endpoint"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	clientCert, clientKey, _, _ := issue(t, dir, "client", ca, caKey, &x509.Certificate{
		Subject: pkix.Name{CommonName: "autoscaler"}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	client, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		t.Fatal(err)
	}
	return pki{TLSFiles{serverCert, serverKey, caFile}, pool, client}
}

type fixture struct {
	t     *testing.T
	store *store.Memory
	base  string
	addr  string
	pki   pki
}

func start(t *testing.T) *fixture {
	t.Helper()
	p := newPKI(t)
	s := store.NewMemory()
	if _, err := s.PutCluster([]byte(record)); err != nil {
		t.Fatal(err)
	}
	server, listener, err := Listen(s, "127.0.0.1:0", p.files)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return &fixture{t, s, "https://" + listener.Addr().String(), listener.Addr().String(), p}
}

func (f *fixture) call(method, path, contentType string, body []byte, withCert bool) (int, map[string]any, error) {
	config := &tls.Config{RootCAs: f.pki.pool}
	if withCert {
		config.Certificates = []tls.Certificate{f.pki.client}
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: config}}
	req, _ := http.NewRequest(method, f.base+path, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out, nil
}

func (f *fixture) get(path string) (int, map[string]any) {
	f.t.Helper()
	code, body, err := f.call("GET", path, "", nil, true)
	if err != nil {
		f.t.Fatal(err)
	}
	return code, body
}

func (f *fixture) patch(operations string) (int, map[string]any) {
	f.t.Helper()
	code, body, err := f.call("PATCH", clusterURL, JSONPatch, []byte(operations), true)
	if err != nil {
		f.t.Fatal(err)
	}
	return code, body
}

func group0(body map[string]any) map[string]any {
	return body["spec"].(map[string]any)["workerGroupSpecs"].([]any)[0].(map[string]any)
}

func version(body map[string]any) string {
	v, _ := body["metadata"].(map[string]any)["resourceVersion"].(string)
	return v
}

func pod(name, cluster, kind, group string) store.Pod {
	return store.Pod{APIVersion: "v1", Kind: "Pod",
		Metadata: store.PodMetadata{Name: name, Namespace: "default",
			Labels: map[string]string{"ray.io/cluster": cluster, "ray.io/node-type": kind, "ray.io/group": group}},
		Status: store.PodStatus{ContainerStatuses: []store.ContainerStatus{{State: map[string]struct{}{"running": {}}}}}}
}

func TestGetClusterCarriesFieldsTheConfigProducerRequires(t *testing.T) {
	f := start(t)
	code, body := f.get(clusterURL)
	meta := body["metadata"].(map[string]any)
	if code != 200 || meta["namespace"] != "default" || meta["name"] != "test" || version(body) == "" {
		t.Errorf("%d %v", code, body)
	}
	if group0(body)["replicas"] != float64(1) {
		t.Error(group0(body))
	}
	// Stored worker groups always have scaleStrategy.
	if s, ok := group0(body)["scaleStrategy"].(map[string]any); !ok || len(s) != 0 {
		t.Errorf("scaleStrategy = %v", group0(body)["scaleStrategy"])
	}
}

func TestUnknownPathsAre404(t *testing.T) {
	f := start(t)
	for _, path := range []string{
		strings.Replace(clusterURL, "/test", "/absent", 1),   // unknown cluster
		strings.Replace(clusterURL, "/v1/", "/v1alpha1/", 1), // other API version
	} {
		if code, _ := f.get(path); code != 404 {
			t.Errorf("%s: %d", path, code)
		}
	}
}

func TestScaleUpPatchReturns200AndPersists(t *testing.T) {
	f := start(t)
	_, before := f.get(clusterURL)
	code, body := f.patch(`[{"op":"replace","path":"/spec/workerGroupSpecs/0/replicas","value":3}]`)
	if code != 200 || group0(body)["replicas"] != float64(3) {
		t.Fatalf("%d %v", code, body)
	}
	_, after := f.get(clusterURL)
	if group0(after)["replicas"] != float64(3) || version(after) == version(before) {
		t.Errorf("%v", after)
	}
}

func TestDeleteWorkersPatchSetsReplicasAndDeleteList(t *testing.T) {
	f := start(t)
	code, _ := f.patch(`[{"op":"replace","path":"/spec/workerGroupSpecs/0/replicas","value":0},
		{"op":"replace","path":"/spec/workerGroupSpecs/0/scaleStrategy","value":{"workersToDelete":["w1"]}}]`)
	_, body := f.get(clusterURL)
	doomed := group0(body)["scaleStrategy"].(map[string]any)["workersToDelete"].([]any)
	if code != 200 || group0(body)["replicas"] != float64(0) || len(doomed) != 1 || doomed[0] != "w1" {
		t.Errorf("%d %v", code, group0(body))
	}
}

func TestPatchWithTheCurrentResourceVersionIsApplied(t *testing.T) {
	f := start(t)
	_, before := f.get(clusterURL)
	code, _ := f.patch(`[{"op":"test","path":"/metadata/resourceVersion","value":"` + version(before) + `"},
		{"op":"replace","path":"/spec/workerGroupSpecs/0/replicas","value":2}]`)
	if code != 200 {
		t.Errorf("%d", code)
	}
}

func TestRejectedPatchesLeaveTheRecordUnchanged(t *testing.T) {
	f := start(t)
	for name, operations := range map[string]string{
		"failed test operation": `[{"op":"test","path":"/metadata/resourceVersion","value":"stale"},
			{"op":"replace","path":"/spec/workerGroupSpecs/0/replicas","value":2}]`,
		"missing array element":    `[{"op":"replace","path":"/spec/workerGroupSpecs/7/replicas","value":2}]`,
		"replace of absent member": `[{"op":"replace","path":"/spec/workerGroupSpecs/0/absent","value":2}]`,
		"rename":                   `[{"op":"replace","path":"/metadata/name","value":"other"}]`,
		"removed spec":             `[{"op":"remove","path":"/spec"}]`,
		"unknown operation":        `[{"op":"increment","path":"/spec/workerGroupSpecs/0/replicas"}]`,
	} {
		if code, _ := f.patch(operations); code != 422 {
			t.Errorf("%s: %d", name, code)
		}
	}
	code, body := f.get(clusterURL)
	if code != 200 || group0(body)["replicas"] != float64(1) {
		t.Errorf("%d %v", code, body)
	}
}

func TestBodiesThatAreNotAJSONPatchAreRefused(t *testing.T) {
	f := start(t)
	for body, want := range map[string]int{`{"op":"replace"}`: 400, `not json`: 400, `null`: 400} {
		if code, _ := f.patch(body); code != want {
			t.Errorf("%s: %d", body, code)
		}
	}
	// Merge patch, which the stock autoscaler sends only for
	// idleTerminationOptions policy Suspend.
	code, _, err := f.call("PATCH", clusterURL, "application/merge-patch+json", []byte(`{"spec":{"idleSuspend":true}}`), true)
	if err != nil || code != 415 {
		t.Errorf("merge patch: %d %v", code, err)
	}
	code, _, err = f.call("PATCH", clusterURL, JSONPatch, bytes.Repeat([]byte(" "), MaxPatchBytes+1), true)
	if err != nil || code != 413 {
		t.Errorf("oversized patch: %d %v", code, err)
	}
	if _, body := f.get(clusterURL); body["spec"].(map[string]any)["idleSuspend"] != nil {
		t.Error("the merge patch was applied")
	}
}

func TestDeleteClusterIsRefused(t *testing.T) {
	f := start(t)
	if code, _, _ := f.call("DELETE", clusterURL, "", nil, true); code != 405 {
		t.Error(code)
	}
	if code, _ := f.get(clusterURL); code != 200 {
		t.Error(code)
	}
}

func TestPodListHoldsOnlyTheSelectedCluster(t *testing.T) {
	f := start(t)
	for _, p := range []store.Pod{pod("head-1", "test", "head", "headgroup"), pod("w1", "test", "worker", "small"), pod("other-w1", "other", "worker", "small")} {
		if err := f.store.PutPod(p); err != nil {
			t.Fatal(err)
		}
	}
	code, body := f.get(podsURL)
	var names []string
	for _, item := range body["items"].([]any) {
		names = append(names, item.(map[string]any)["metadata"].(map[string]any)["name"].(string))
	}
	sort.Strings(names)
	if code != 200 || version(body) == "" || strings.Join(names, ",") != "head-1,w1" {
		t.Errorf("%d %v", code, body)
	}
	if err := f.store.RemovePod("default", "w1"); err != nil {
		t.Fatal(err)
	}
	if _, body := f.get(podsURL); len(body["items"].([]any)) != 1 {
		t.Error("the removed pod is still listed")
	}
}

func TestEmptyPodListIsAnEmptyArray(t *testing.T) {
	f := start(t)
	if _, body := f.get(podsURL); body["items"] == nil {
		t.Error("items is null")
	}
}

func TestPodListWithoutTheClusterSelectorIsRefused(t *testing.T) {
	f := start(t)
	for _, path := range []string{"/api/v1/namespaces/default/pods", "/api/v1/namespaces/default/pods?labelSelector=app%3Dx"} {
		if code, _ := f.get(path); code != 400 {
			t.Errorf("%s: %d", path, code)
		}
	}
}

func TestClientWithoutCertificateCannotMakeARequest(t *testing.T) {
	f := start(t)
	if code, _, err := f.call("GET", clusterURL, "", nil, false); err == nil {
		t.Errorf("request without a client certificate returned %d", code)
	}
}

func TestMissingTLSFileFailsWithTheFieldName(t *testing.T) {
	p := newPKI(t)
	p.files.ServerKey = filepath.Join(t.TempDir(), "absent.key")
	if _, _, err := Listen(store.NewMemory(), "127.0.0.1:0", p.files); err == nil || !strings.Contains(err.Error(), "server key") {
		t.Error(err)
	}
}

func TestSilentConnectionDoesNotBlockOthers(t *testing.T) {
	f := start(t)
	conn, err := net.Dial("tcp", f.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if code, _ := f.get(clusterURL); code != 200 {
		t.Error(code)
	}
}

func TestRecordsTheStoreRefuses(t *testing.T) {
	for message, raw := range map[string]string{
		"namespace":              `{"metadata":{"name":"test"},"spec":{}}`,
		"idleTerminationOptions": `{"metadata":{"name":"test","namespace":"default"},"spec":{"idleTerminationOptions":{"timeoutSeconds":60}}}`,
		"no spec":                `{"metadata":{"name":"test","namespace":"default"}}`,
	} {
		if _, err := store.NewMemory().PutCluster([]byte(raw)); err == nil || !strings.Contains(err.Error(), message) {
			t.Errorf("want %q, got %v", message, err)
		}
	}
	p := pod("w1", "test", "worker", "small")
	delete(p.Metadata.Labels, "ray.io/node-type")
	if err := store.NewMemory().PutPod(p); err == nil || !strings.Contains(err.Error(), "node-type") {
		t.Error(err)
	}
}
