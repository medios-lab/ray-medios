package launch_test

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/medios-lab/medios-sdk-go/medios"

	"github.com/medios-lab/ray-medios/internal/launch"
	"github.com/medios-lab/ray-medios/internal/schedule"
	"github.com/medios-lab/ray-medios/internal/tasks"
)

func offer(attributes map[string]string, ranges ...[2]uint64) medios.Offer {
	o := medios.Offer{ID: medios.ID{Value: "o1"}, NodeID: medios.ID{Value: "n1"}, Hostname: "node-a"}
	for name, v := range map[string]float64{"cpus": 8, "mem": 16384, "gpus": 1} {
		o.Resources = append(o.Resources, medios.Resource{Name: name, Type: "SCALAR", Scalar: &medios.Scalar{Value: v}})
	}
	r := &medios.Ranges{}
	for _, span := range ranges {
		r.Range = append(r.Range, medios.Range{Begin: span[0], End: span[1]})
	}
	o.Resources = append(o.Resources, medios.Resource{Name: "ports", Type: "RANGES", Ranges: r})
	for k, v := range attributes {
		o.Attributes = append(o.Attributes, medios.Attribute{Name: k, Type: "TEXT", Text: &medios.AttributeText{Value: v}})
	}
	return o
}

func TestOfferFromANodeLackingAPlacementAttributeDoesNotMatch(t *testing.T) {
	placement := map[string]string{"containerizer": "medios", "gpu": "gb10"}
	if !launch.OfferMatches(offer(map[string]string{"containerizer": "medios", "gpu": "gb10", "images": "docker"}), placement) {
		t.Error("an offer with every attribute does not match")
	}
	for _, a := range []map[string]string{
		{"containerizer": "medios", "host": "other"}, {"containerizer": "docker_shell", "gpu": "gb10"}, {},
	} {
		if launch.OfferMatches(offer(a), placement) {
			t.Errorf("an offer with %v matches", a)
		}
	}
}

func TestOfferResourcesAreRead(t *testing.T) {
	o := offer(map[string]string{"a": "b"}, [2]uint64{31010, 31010}, [2]uint64{31000, 31002})
	if launch.OfferScalar(o, "cpus") != 8 || launch.OfferScalar(o, "disk") != 0 {
		t.Error("scalars")
	}
	if got := launch.OfferPorts(o); !reflect.DeepEqual(got, []int{31000, 31001, 31002, 31010}) {
		t.Error(got)
	}
}

// The files in testdata hold the expected TaskInfo JSON of a head, a head with
// the autoscaler and a worker for these inputs.
func TestTaskInfoMatchesTheRecordedWireForm(t *testing.T) {
	cluster := schedule.Cluster{Name: "c1", Containerizer: tasks.Medios, NodeAttributes: map[string]string{"gpu": "gb10"},
		AuthToken: "tok", WorkerPorts: 4}
	head := schedule.Launched{InstanceID: "c1-headgroup-aaaaaa", Group: schedule.HeadGroup, IsHead: true, Running: true,
		Address: "node-a", GCSPort: 31000}
	endpoint := &tasks.AutoscalerEndpoint{Host: "fw.example", Port: 8443, Namespace: "ns",
		CACertPEM: "CA PEM\n", ClientCertPEM: "CERT PEM\n", ClientKeyPEM: "KEY PEM\n"}
	cases := []struct {
		file       string
		wanted     schedule.Wanted
		instanceID string
		head       *schedule.Launched
		autoscaler *tasks.AutoscalerEndpoint
	}{
		{"task_head.json", schedule.Wanted{Group: schedule.HeadGroup, IsHead: true, CPUs: 1, MemoryMB: 2048}, head.InstanceID, nil, nil},
		{"task_head_autoscaler.json", schedule.Wanted{Group: schedule.HeadGroup, IsHead: true, CPUs: 1, MemoryMB: 2048}, head.InstanceID, nil, endpoint},
		{"task_worker.json", schedule.Wanted{Group: "small", CPUs: 2, MemoryMB: 4096, GPUs: 1}, "c1-small-bbbbbb", &head, nil},
	}
	o := offer(map[string]string{"containerizer": "medios", "gpu": "gb10"}, [2]uint64{31000, 31099})
	for _, c := range cases {
		cl := cluster
		cl.Autoscaler = c.autoscaler
		p, err := schedule.Place(c.wanted, o, cl, c.instanceID, c.head)
		if err != nil || p == nil {
			t.Fatalf("%s: place: %v %v", c.file, p, err)
		}
		task, err := launch.TaskInfo(p.Plan, o, c.instanceID, "run:c1", "img@sha256:0", p.Size, nil, medios.StoreRW)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := json.Marshal(task)
		want, err := os.ReadFile("testdata/" + c.file)
		if err != nil {
			t.Fatal(err)
		}
		var g, w any
		_ = json.Unmarshal(got, &g)
		_ = json.Unmarshal(want, &w)
		if !reflect.DeepEqual(g, w) {
			t.Errorf("%s differs:\n got %s", c.file, got)
		}
	}
}

func plan() tasks.TaskPlan {
	return tasks.TaskPlan{Argv: []string{"ray", "start", "a b"}, Env: []tasks.EnvVar{{Name: "X", Value: "1"}},
		Ports: []int{31002, 31000}, Placement: map[string]string{"containerizer": "docker_shell"}}
}

func TestDockerShellPlanCarriesTheImageAndLinuxInfo(t *testing.T) {
	p := plan()
	p.LinuxInfo = &tasks.LinuxInfo{IPCMode: "PRIVATE", ShmSizeMB: 1024}
	p.ShellBefore = []string{"one", "two"}
	task, err := launch.TaskInfo(p, offer(nil, [2]uint64{31000, 31002}), "t1", "run:t1", "img@sha256:0", launch.Size{CPUs: 1, MemoryMB: 512}, nil, medios.StoreRW)
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := json.Marshal(task.Container)
	if string(wire) != `{"type":"DOCKER","docker":{"image":"img@sha256:0"},"linux_info":{"ipc_mode":"PRIVATE","shm_size":1024}}` {
		t.Error(string(wire))
	}
	if task.Command.Value != "set -e\none\ntwo\nexec ray start 'a b'" {
		t.Error(task.Command.Value)
	}
	if len(task.Resources) != 3 {
		t.Errorf("gpus reserved for a task that asked for none: %v", task.Resources)
	}
}

func TestRefusals(t *testing.T) {
	p := plan()
	p.Placement = map[string]string{}
	o := offer(nil, [2]uint64{31000, 31002})
	if _, err := launch.TaskInfo(p, o, "t1", "run:t1", "img", launch.Size{CPUs: 1, MemoryMB: 512}, nil, medios.StoreRW); err == nil || !strings.Contains(err.Error(), "containerizer") {
		t.Errorf("plan without a containerizer: %v", err)
	}
	if _, err := launch.TaskInfo(plan(), o, "t1", "run:t1", "img", launch.Size{CPUs: 9, MemoryMB: 512}, nil, medios.StoreRW); err == nil {
		t.Error("more cpus than the offer holds were accepted")
	}
	if _, err := launch.TaskInfo(plan(), o, "t1", "nobody", "img", launch.Size{CPUs: 1, MemoryMB: 512}, nil, medios.StoreRW); err == nil {
		t.Error("a bad principal was accepted")
	}
}

func TestHostNetworkPlanSetsDockerNetworkHost(t *testing.T) {
	p := plan()
	p.HostNetwork = true
	task, err := launch.TaskInfo(p, offer(nil, [2]uint64{31000, 31002}), "t1", "run:t1", "img@sha256:0", launch.Size{CPUs: 1, MemoryMB: 512}, nil, medios.StoreRW)
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := json.Marshal(task.Container)
	if string(wire) != `{"type":"DOCKER","docker":{"image":"img@sha256:0","network":"HOST"}}` {
		t.Error(string(wire))
	}
}

func TestStoresAreMountedReadWriteFromTheOffer(t *testing.T) {
	p := plan()
	p.Placement = map[string]string{"containerizer": "docker_shell"}
	o := offer(nil, [2]uint64{31000, 31002})
	stores := map[string]string{"results": "/mnt/cluster_storage"}
	if launch.OfferHasStores(o, stores) {
		t.Fatal("an offer without the store has it")
	}
	if _, err := launch.TaskInfo(p, o, "t1", "run:t1", "img@sha256:0", launch.Size{CPUs: 1, MemoryMB: 512}, stores, medios.StoreRW); err == nil {
		t.Fatal("a task was built from an offer without the store")
	}
	o.Resources = append(o.Resources, medios.Resource{Name: "store/results", Type: "SCALAR", Scalar: &medios.Scalar{Value: 1}})
	if !launch.OfferHasStores(o, stores) {
		t.Fatal("an offer with the store lacks it")
	}
	task, err := launch.TaskInfo(p, o, "t1", "run:t1", "img@sha256:0", launch.Size{CPUs: 1, MemoryMB: 512}, stores, medios.StoreRW)
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := json.Marshal(task.Container.Volumes)
	if got := string(wire); got != `[{"container_path":"/mnt/cluster_storage","mode":"RW","source":{"type":"STORE","store":{"name":"results"}}}]` {
		t.Error(got)
	}
	found := false
	for _, r := range task.Resources {
		found = found || r.Name == "store/results"
	}
	if !found {
		t.Error("the task does not hold the store resource")
	}
}

func TestRemainingKeepsAStoreSoASecondTaskCanMountIt(t *testing.T) {
	p := plan()
	p.Placement = map[string]string{"containerizer": "docker_shell"}
	p.Ports = nil
	o := offer(nil, [2]uint64{31000, 31002})
	o.Resources = append(o.Resources, medios.Resource{Name: "store/results", Type: "SCALAR", Scalar: &medios.Scalar{Value: 1}})
	stores := map[string]string{"results": "/mnt/cluster_storage"}
	size := launch.Size{CPUs: 1, MemoryMB: 512}
	first, err := launch.TaskInfo(p, o, "t1", "run:t1", "img@sha256:0", size, stores, medios.StoreRW)
	if err != nil {
		t.Fatal(err)
	}
	left := launch.Remaining(o, first.Resources)
	if got := launch.OfferScalar(left, "store/results"); got != 1 {
		t.Fatalf("the store resource left in the offer is %g", got)
	}
	if got, want := launch.OfferScalar(left, "cpus"), launch.OfferScalar(o, "cpus")-1; got != want {
		t.Fatalf("cpus left %g, want %g", got, want)
	}
	second, err := launch.TaskInfo(p, left, "t2", "run:t2", "img@sha256:0", size, stores, medios.StoreRW)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range second.Resources {
		if r.Name == "store/results" && r.Scalar.Value == 1 {
			return
		}
	}
	t.Fatal("the second task does not list the whole store resource")
}
