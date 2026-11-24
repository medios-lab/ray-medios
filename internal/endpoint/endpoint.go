// Package endpoint is the HTTPS endpoint that the stock `ray
// kuberay-autoscaler` command talks to.
//
// It answers the requests listed in docs/endpoint-contract.md from a
// store.Store. It starts no tasks and calls no Medios API.
//
// Access rule: any client presenting a certificate signed by the client CA
// may read and patch every cluster in the store. Scoping a certificate to one
// cluster is not implemented.
package endpoint

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/medios-lab/ray-medios/internal/store"
)

// RayClusterAPIVersion is the RayCluster API version served. The stock
// client's KUBERAY_CRD_VER defaults to v1; v1alpha1 is frozen upstream and is
// not served.
const RayClusterAPIVersion = "v1"

// JSONPatch is the only PATCH content type accepted.
const JSONPatch = "application/json-patch+json"

// MaxPatchBytes is the largest PATCH body accepted. Type: default,
// provisional. Typical scale patches are under 300 bytes; the bound exists so
// a client cannot make the endpoint buffer an arbitrary body.
const MaxPatchBytes = 1024 * 1024

// ConnectionIdleTimeout is how long a connection may sit without sending
// bytes, TLS handshake and request headers included, before the endpoint
// closes it. Type: default, provisional; no measurement behind it.
const ConnectionIdleTimeout = 30 * time.Second

var (
	clusterPath = regexp.MustCompile(`^/apis/ray\.io/` + RayClusterAPIVersion + `/namespaces/([^/]+)/rayclusters/([^/]+)$`)
	podsPath    = regexp.MustCompile(`^/api/v1/namespaces/([^/]+)/pods$`)
)

// TLSFiles names the PEM files for mutual TLS. All three are required; none
// is generated.
type TLSFiles struct {
	ServerCert string
	ServerKey  string
	ClientCA   string
}

func (f TLSFiles) config() (*tls.Config, error) {
	for _, file := range []struct{ field, path string }{
		{"server certificate", f.ServerCert}, {"server key", f.ServerKey}, {"client CA", f.ClientCA},
	} {
		if info, err := os.Stat(file.path); err != nil || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("TLS %s is not a file: %q", file.field, file.path)
		}
	}
	cert, err := tls.LoadX509KeyPair(f.ServerCert, f.ServerKey)
	if err != nil {
		return nil, fmt.Errorf("TLS server certificate and key: %w", err)
	}
	pem, err := os.ReadFile(f.ClientCA)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("TLS client CA holds no certificate: %q", f.ClientCA)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

type refusal struct {
	code    int
	reason  string
	message string
}

func (r *refusal) Error() string { return r.message }

type handler struct{ store store.Store }

func send(w http.ResponseWriter, code int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

func refuse(w http.ResponseWriter, r *refusal) {
	// The stock client reads only the status code of an error response and
	// treats every 4xx and 5xx alike. The codes and this body are this
	// endpoint's own choice, shaped like a Kubernetes Status object; they
	// are not checked against what a Kubernetes API server returns.
	body, _ := json.Marshal(map[string]any{
		"kind": "Status", "apiVersion": "v1", "status": "Failure",
		"reason": r.reason, "message": r.message, "code": r.code,
	})
	// The request body may be unread, so the connection is not reused.
	w.Header().Set("Connection", "close")
	send(w, r.code, body)
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := h.dispatch(r)
	var ref *refusal
	switch {
	case err == nil:
		send(w, http.StatusOK, body)
	case errors.As(err, &ref):
		refuse(w, ref)
	case errors.Is(err, store.ErrClusterNotFound):
		refuse(w, &refusal{404, "NotFound", err.Error()})
	case errors.Is(err, store.ErrPatchRejected):
		refuse(w, &refusal{422, "Invalid", err.Error()})
	default:
		log.Printf("endpoint: %s %s failed: %v", r.Method, r.URL.Path, err)
		refuse(w, &refusal{500, "InternalError", "internal error"})
	}
	log.Printf("endpoint: %s %s %s", r.RemoteAddr, r.Method, r.URL.RequestURI())
}

func (h handler) dispatch(r *http.Request) ([]byte, error) {
	cluster := clusterPath.FindStringSubmatch(r.URL.Path)
	pods := podsPath.FindStringSubmatch(r.URL.Path)
	switch {
	case cluster != nil && r.Method == http.MethodGet:
		return h.store.GetCluster(cluster[1], cluster[2])
	case cluster != nil && r.Method == http.MethodPatch:
		return h.patchCluster(r, cluster[1], cluster[2])
	case pods != nil && r.Method == http.MethodGet:
		return h.listPods(r, pods[1])
	case cluster != nil || pods != nil:
		// Includes DELETE on a RayCluster, which the stock autoscaler sends
		// only for idleTerminationOptions policy Delete.
		return nil, &refusal{405, "MethodNotAllowed", fmt.Sprintf("%s is not supported on %s", r.Method, r.URL.Path)}
	}
	return nil, &refusal{404, "NotFound", "no resource at " + r.URL.Path}
}

func (h handler) patchCluster(r *http.Request, namespace, name string) ([]byte, error) {
	contentType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if contentType != JSONPatch {
		// Includes application/merge-patch+json, which the stock autoscaler
		// sends only for idleTerminationOptions policy Suspend.
		return nil, &refusal{415, "UnsupportedMediaType", fmt.Sprintf("PATCH requires %s, got %q", JSONPatch, contentType)}
	}
	if r.ContentLength < 0 {
		return nil, &refusal{411, "LengthRequired", "PATCH requires Content-Length"}
	}
	if r.ContentLength > MaxPatchBytes {
		return nil, &refusal{413, "RequestEntityTooLarge", fmt.Sprintf("PATCH body over %d bytes", MaxPatchBytes)}
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, &refusal{400, "BadRequest", "PATCH body could not be read"}
	}
	var operations []json.RawMessage
	if !json.Valid(body) {
		return nil, &refusal{400, "BadRequest", "PATCH body is not JSON"}
	}
	if err := json.Unmarshal(body, &operations); err != nil || operations == nil {
		return nil, &refusal{400, "BadRequest", "a JSON Patch body is a list of operations"}
	}
	patched, err := h.store.PatchCluster(namespace, name, body)
	if err == nil {
		// The body is the autoscaler's whole decision: replica counts and the
		// names of workers to delete. It is at most MaxPatchBytes long.
		log.Printf("endpoint: patch applied %s", body)
	}
	return patched, err
}

func (h handler) listPods(r *http.Request, namespace string) ([]byte, error) {
	selectors := r.URL.Query()["labelSelector"]
	prefix := store.ClusterLabel + "="
	if len(selectors) != 1 || !strings.HasPrefix(selectors[0], prefix) || strings.Contains(selectors[0], ",") {
		return nil, &refusal{400, "BadRequest", "pods must be listed with labelSelector=" + prefix + "<cluster>"}
	}
	items, version := h.store.ListPods(namespace, strings.TrimPrefix(selectors[0], prefix))
	return json.Marshal(map[string]any{
		"kind": "PodList", "apiVersion": "v1",
		"metadata": map[string]string{"resourceVersion": version},
		"items":    items,
	})
}

// Listen binds the endpoint on address ("host:port"). The caller runs
// Serve(listener) on the returned server and Close to stop it.
func Listen(s store.Store, address string, files TLSFiles) (*http.Server, net.Listener, error) {
	config, err := files.config()
	if err != nil {
		return nil, nil, err
	}
	listener, err := tls.Listen("tcp", address, config)
	if err != nil {
		return nil, nil, err
	}
	// The handshake happens in the connection's own goroutine, so a client
	// that connects and stays silent does not hold up others.
	server := &http.Server{
		Handler:           handler{s},
		ReadHeaderTimeout: ConnectionIdleTimeout,
		ReadTimeout:       ConnectionIdleTimeout,
		IdleTimeout:       ConnectionIdleTimeout,
		ErrorLog:          log.Default(),
	}
	return server, listener, nil
}
