package schedule

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/medios-lab/medios-sdk-go/medios"

	"github.com/medios-lab/ray-medios/internal/store"
	"github.com/medios-lab/ray-medios/internal/tasks"
)

func group(name string, replicas int, extra string, doomed ...string) string {
	d, _ := json.Marshal(append([]string{}, doomed...))
	return fmt.Sprintf(`{"groupName":%q,"replicas":%d,"scaleStrategy":{"workersToDelete":%s}%s,
		"template":{"spec":{"containers":[{"resources":{"limits":{"cpu":"2","memory":"4Gi"}}}]}}}`, name, replicas, d, extra)
}

func record(t *testing.T, groups ...string) Record {
	t.Helper()
	r, err := ParseRecord([]byte(`{"metadata":{"name":"c1","namespace":"ns"},"spec":{
		"headGroupSpec":{"template":{"spec":{"containers":[{"resources":{"limits":{"cpu":"1","memory":"2Gi"}}}]}}},
		"workerGroupSpecs":[` + strings.Join(groups, ",") + `]}}`))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

var head = Launched{InstanceID: "c1-head", Group: HeadGroup, IsHead: true, Running: true, Address: "node-a", GCSPort: 31000}

func worker(i int, running bool) Launched {
	return Launched{InstanceID: fmt.Sprintf("c1-small-%d", i), Group: "small", Running: running, Address: "node-a"}
}

func offer(attributes map[string]string, cpus, mem float64, firstPort, lastPort uint64) medios.Offer {
	o := medios.Offer{ID: medios.ID{Value: "o1"}, NodeID: medios.ID{Value: "n1"}, Hostname: "node-a", Resources: []medios.Resource{
		{Name: "cpus", Type: "SCALAR", Scalar: &medios.Scalar{Value: cpus}},
		{Name: "mem", Type: "SCALAR", Scalar: &medios.Scalar{Value: mem}},
		{Name: "ports", Type: "RANGES", Ranges: &medios.Ranges{Range: []medios.Range{{Begin: firstPort, End: lastPort}}}},
	}}
	for k, v := range attributes {
		o.Attributes = append(o.Attributes, medios.Attribute{Name: k, Type: "TEXT", Text: &medios.AttributeText{Value: v}})
	}
	return o
}

func TestQuantities(t *testing.T) {
	for text, want := range map[string]int{"500m": 1, "2": 2, "1.5": 2} {
		if got, err := ParseCPU(text); err != nil || got != want {
			t.Errorf("ParseCPU(%q) = %d, %v", text, got, err)
		}
	}
	for text, want := range map[string]int{"4Gi": 4096, "512Mi": 512} {
		if got, err := ParseMemoryMB(text); err != nil || got != want {
			t.Errorf("ParseMemoryMB(%q) = %d, %v", text, got, err)
		}
	}
	for _, bad := range []string{"4GiB", "lots", "1G", "0"} {
		if _, err := ParseMemoryMB(bad); err == nil {
			t.Errorf("ParseMemoryMB(%q) was accepted", bad)
		}
	}
}

func decide(t *testing.T, r Record, launched ...Launched) Decision {
	t.Helper()
	d, err := Decide(r, launched)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestOnlyTheHeadIsLaunchedFirst(t *testing.T) {
	d := decide(t, record(t, group("small", 1, "")))
	if !reflect.DeepEqual(d.Launch, []Wanted{{HeadGroup, true, 1, 2048, 0, nil}}) || len(d.Kill) != 0 {
		t.Errorf("%+v", d)
	}
}

func TestWorkersWaitUntilTheHeadTaskIsRunning(t *testing.T) {
	starting := head
	starting.Running = false
	if d := decide(t, record(t, group("small", 1, "")), starting); len(d.Launch) != 0 {
		t.Errorf("%+v", d)
	}
	d := decide(t, record(t, group("small", 2, "")), head)
	w := Wanted{"small", false, 2, 4096, 0, nil}
	if !reflect.DeepEqual(d.Launch, []Wanted{w, w}) {
		t.Errorf("%+v", d)
	}
}

func TestNothingToDoWhenReplicasAreMet(t *testing.T) {
	d := decide(t, record(t, group("small", 1, "")), head, worker(1, false))
	if len(d.Launch) != 0 || len(d.Kill) != 0 {
		t.Errorf("%+v", d)
	}
}

func TestWorkerNamedInWorkersToDeleteIsKilledAndNotReplaced(t *testing.T) {
	d := decide(t, record(t, group("small", 1, "", "c1-small-2")), head, worker(1, true), worker(2, true))
	if !reflect.DeepEqual(d.Kill, []string{"c1-small-2"}) || len(d.Launch) != 0 {
		t.Errorf("%+v", d)
	}
}

func TestWorkerAlreadyBeingKilledIsNotKilledAgain(t *testing.T) {
	dying := worker(2, true)
	dying.KillRequestedAt = "2026-11-17T19:00:00Z"
	d := decide(t, record(t, group("small", 1, "", "c1-small-2")), head, worker(1, true), dying)
	if len(d.Kill) != 0 || len(d.Launch) != 0 {
		t.Errorf("%+v", d)
	}
}

func TestGroupsTheFrameworkCannotSizeAreRefused(t *testing.T) {
	noCPU := strings.Replace(group("small", 1, ""), `"cpu":"2",`, "", 1)
	for message, g := range map[string]string{
		"no cpu":         noCPU,
		"not supported":  group("small", 1, `,"resources":{"CPU":"4"}`),
		"rayStartParams": group("small", 1, `,"rayStartParams":{"num-cpus":"4"}`),
	} {
		if _, err := Decide(record(t, g), []Launched{head}); err == nil || !strings.Contains(err.Error(), message) {
			t.Errorf("want %q, got %v", message, err)
		}
	}
}

func TestGPUCountComesFromAKeyEndingInGPU(t *testing.T) {
	g := strings.Replace(group("small", 1, ""), `"cpu":"2"`, `"cpu":"2","nvidia.com/gpu":1`, 1)
	if d := decide(t, record(t, g), head); d.Launch[0].GPUs != 1 {
		t.Errorf("%+v", d)
	}
}

var cluster = Cluster{Name: "c1", Containerizer: tasks.Medios, NodeAttributes: map[string]string{"gpu": "gb10"}, AuthToken: "tok", WorkerPorts: 4}
var good = map[string]string{"containerizer": "medios", "gpu": "gb10"}

func TestGroupTooSmallForRaysObjectStoreIsRefused(t *testing.T) {
	_, err := Place(Wanted{"small", false, 1, 128, 0, nil}, offer(good, 8, 16384, 31000, 31099), cluster, "x", &head)
	if err == nil || !strings.Contains(err.Error(), "Ray requires 75") {
		t.Error(err)
	}
}

func TestHeadIsPlacedOnAMatchingOfferWithPortsFromTheOffer(t *testing.T) {
	p, err := Place(Wanted{HeadGroup, true, 1, 2048, 0, nil}, offer(good, 8, 16384, 31000, 31099), cluster, "c1-head", nil)
	if err != nil || p == nil {
		t.Fatal(p, err)
	}
	if p.Node.Head.GCS != 31000 || p.Node.Head.Dashboard != 31003 || len(p.Plan.Ports) != 15 {
		t.Errorf("%+v", p.Node)
	}
	if p.Node.Address != "node-a" || p.Node.ObjectStoreMB != 614 {
		t.Errorf("%+v", p.Node)
	}
}

func TestWorkerGetsTheHeadAddress(t *testing.T) {
	p, err := Place(Wanted{"small", false, 2, 4096, 0, nil}, offer(good, 8, 16384, 31000, 31099), cluster, "c1-small-1", &head)
	if err != nil || p == nil {
		t.Fatal(p, err)
	}
	if p.Node.HeadAddress != "node-a:31000" || len(p.Plan.Ports) != 10 {
		t.Errorf("%+v", p.Node)
	}
}

func TestOfferThatCannotHoldTheNodeIsNotUsed(t *testing.T) {
	for _, o := range []medios.Offer{
		offer(map[string]string{"containerizer": "medios", "host": "other"}, 8, 16384, 31000, 31099), // same containerizer, another node
		offer(good, 0.5, 16384, 31000, 31099), offer(good, 8, 1024, 31000, 31099), offer(good, 8, 16384, 31000, 31005),
	} {
		if p, err := Place(Wanted{HeadGroup, true, 1, 2048, 0, nil}, o, cluster, "c1-head", nil); p != nil || err != nil {
			t.Errorf("placed on %+v (%v)", o, err)
		}
	}
}

// testdata/pods.json holds the expected pod records of a running head, a
// waiting worker and a worker being killed.
func TestPodRecordsMatchTheRecordedFormAndAreAcceptedByTheStore(t *testing.T) {
	h := head
	h.InstanceID = "c1-headgroup-aaaaaa"
	dying := Launched{InstanceID: "c1-small-bbbbbb", Group: "small", Running: true, Address: "node-a", KillRequestedAt: "2026-11-17T19:00:00Z"}
	waiting := Launched{InstanceID: "c1-small-cccccc", Group: "small", Address: "node-a"}
	pods := []store.Pod{PodRecord("ns", "c1", h), PodRecord("ns", "c1", waiting), PodRecord("ns", "c1", dying)}
	got, _ := json.Marshal(pods)
	want, err := os.ReadFile("testdata/pods.json")
	if err != nil {
		t.Fatal(err)
	}
	var g, w any
	_ = json.Unmarshal(got, &g)
	_ = json.Unmarshal(want, &w)
	if !reflect.DeepEqual(g, w) {
		t.Errorf("pod records differ:\n got %s", got)
	}
	s := store.NewMemory()
	for _, p := range pods {
		if err := s.PutPod(p); err != nil {
			t.Error(err)
		}
	}
	if listed, _ := s.ListPods("ns", "c1"); len(listed) != 3 {
		t.Errorf("listed %d pods", len(listed))
	}
}

func TestContainerEnvReachesTheTask(t *testing.T) {
	raw := []byte(`{"spec":{"headGroupSpec":{"template":{"spec":{"containers":[{
		"env":[{"name":"GRPC_ENABLE_FORK_SUPPORT","value":"0"}],
		"resources":{"limits":{"cpu":"1","memory":"2Gi"}}}]}}}}}`)
	record, err := ParseRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	d, err := Decide(record, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []tasks.EnvVar{{Name: "GRPC_ENABLE_FORK_SUPPORT", Value: "0"}}
	if len(d.Launch) != 1 || !reflect.DeepEqual(d.Launch[0].Env, want) {
		t.Fatalf("launch = %+v", d.Launch)
	}
	cluster := Cluster{Name: "c1", Containerizer: tasks.DockerShell, WorkerPorts: 8, AuthToken: "t"}
	p, err := Place(d.Launch[0], offer(map[string]string{"containerizer": "docker_shell"}, 8, 16384, 31000, 31099), cluster, "c1-head", nil)
	if err != nil || p == nil {
		t.Fatalf("placement = %v, err = %v", p, err)
	}
	if v, ok := p.Plan.EnvValue("GRPC_ENABLE_FORK_SUPPORT"); !ok || v != "0" {
		t.Fatalf("variable not in the task plan: %q %v", v, ok)
	}

	d.Launch[0].Env = []tasks.EnvVar{{Name: "RAY_AUTH_TOKEN", Value: "other"}}
	if _, err := Place(d.Launch[0], offer(map[string]string{"containerizer": "docker_shell"}, 8, 16384, 31000, 31099), cluster, "c1-head", nil); err == nil {
		t.Fatal("a record that sets RAY_AUTH_TOKEN was accepted")
	}

	withValueFrom := []byte(`{"spec":{"headGroupSpec":{"template":{"spec":{"containers":[{
		"env":[{"name":"X","valueFrom":{"secretKeyRef":{"name":"s","key":"k"}}}],
		"resources":{"limits":{"cpu":"1","memory":"2Gi"}}}]}}}}}`)
	record, err = ParseRecord(withValueFrom)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decide(record, nil); err == nil {
		t.Fatal("an env entry with valueFrom was accepted")
	}
}
