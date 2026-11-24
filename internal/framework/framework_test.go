package framework

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/medios-lab/medios-sdk-go/medios"

	"github.com/medios-lab/ray-medios/internal/store"
	"github.com/medios-lab/ray-medios/internal/tasks"
)

type call struct {
	name string
	id   string
	task medios.TaskInfo
	arg  string
}

type fakeClient struct {
	calls      []call
	failAccept bool
	accepts    int // ACCEPT calls; each task of one is its own "accept" entry in calls
}

func (c *fakeClient) Accept(_ context.Context, ids []medios.ID, ts []medios.TaskInfo) error {
	if c.failAccept {
		return errors.New("accept refused")
	}
	c.accepts++
	for _, t := range ts {
		c.calls = append(c.calls, call{name: "accept", id: ids[0].Value, task: t})
	}
	return nil
}
func (c *fakeClient) Decline(_ context.Context, ids []medios.ID, refuse float64) error {
	c.calls = append(c.calls, call{name: "decline", id: ids[0].Value, arg: fmt.Sprint(refuse)})
	return nil
}
func (c *fakeClient) Kill(_ context.Context, taskID, nodeID string) error {
	c.calls = append(c.calls, call{name: "kill", id: taskID, arg: nodeID})
	return nil
}
func (c *fakeClient) Acknowledge(_ context.Context, taskID, _, uuid string) error {
	c.calls = append(c.calls, call{name: "ack", id: taskID, arg: uuid})
	return nil
}
func (c *fakeClient) Suppress(context.Context) error {
	c.calls = append(c.calls, call{name: "suppress"})
	return nil
}
func (c *fakeClient) Revive(context.Context) error {
	c.calls = append(c.calls, call{name: "revive"})
	return nil
}
func (c *fakeClient) named(name string) []call {
	var out []call
	for _, k := range c.calls {
		if k.name == name {
			out = append(out, k)
		}
	}
	return out
}

var good = map[string]string{"containerizer": "medios", "gpu": "gb10"}

func offer(id string, attributes map[string]string, firstPort uint64) medios.Offer {
	o := medios.Offer{ID: medios.ID{Value: id}, NodeID: medios.ID{Value: "n1"}, Hostname: "node-a", Resources: []medios.Resource{
		{Name: "cpus", Type: "SCALAR", Scalar: &medios.Scalar{Value: 8}},
		{Name: "mem", Type: "SCALAR", Scalar: &medios.Scalar{Value: 16384}},
		{Name: "ports", Type: "RANGES", Ranges: &medios.Ranges{Range: []medios.Range{{Begin: firstPort, End: firstPort + 99}}}},
	}}
	for k, v := range attributes {
		o.Attributes = append(o.Attributes, medios.Attribute{Name: k, Type: "TEXT", Text: &medios.AttributeText{Value: v}})
	}
	return o
}

func offers(items ...medios.Offer) medios.Event {
	return medios.Event{Type: medios.EventOffers, Offers: &medios.OffersBody{Offers: items}}
}

func update(taskID string, state medios.TaskState) medios.Event {
	return medios.Event{Type: medios.EventUpdate, Update: &medios.UpdateBody{Status: medios.TaskStatus{
		TaskID: medios.ID{Value: taskID}, State: state, NodeID: medios.ID{Value: "n1"}, UUID: "u1"}}}
}

const record = `{"metadata":{"name":"c1","namespace":"ns"},"spec":{
	"headGroupSpec":{"template":{"spec":{"containers":[{"resources":{"limits":{"cpu":"1","memory":"2Gi"}}}]}}},
	"workerGroupSpecs":[{"groupName":"small","replicas":1,"minReplicas":0,"maxReplicas":3,
		"template":{"spec":{"containers":[{"resources":{"limits":{"cpu":"2","memory":"4Gi"}}}]}}}]}}`

type fixture struct {
	t      *testing.T
	fw     *Framework
	client *fakeClient
	store  *store.Memory
	config Config
}

func newFixture(t *testing.T, change func(*Config, *store.Memory)) *fixture {
	t.Helper()
	s := store.NewMemory()
	if _, err := s.PutCluster([]byte(record)); err != nil {
		t.Fatal(err)
	}
	config := Config{Namespace: "ns", Name: "c1", Principal: "team:t", Image: "img@sha256:0",
		Containerizer: tasks.Medios, NodeAttributes: map[string]string{"gpu": "gb10"}, WorkerPorts: 4, AuthToken: "tok"}
	if change != nil {
		change(&config, s)
	}
	client := &fakeClient{}
	fw, err := New(client, s, config)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	fw.NewSuffix = func() string { n++; return fmt.Sprint(n) }
	fw.Now = func() time.Time { return time.Date(2026, 11, 17, 19, 0, 0, 0, time.UTC) }
	return &fixture{t, fw, client, s, config}
}

func (f *fixture) handle(ev medios.Event) {
	f.t.Helper()
	if err := f.fw.Handle(context.Background(), ev); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) pods() map[string]store.Pod {
	out := map[string]store.Pod{}
	listed, _ := f.store.ListPods("ns", "c1")
	for _, p := range listed {
		out[p.Metadata.Name] = p
	}
	return out
}

func running(p store.Pod) bool {
	_, ok := p.Status.ContainerStatuses[0].State["running"]
	return ok
}

func (f *fixture) startHead() string {
	f.t.Helper()
	f.handle(offers(offer("o1", good, 31000)))
	f.handle(update("c1-headgroup-1", medios.TaskRunning))
	return "c1-headgroup-1"
}

func (f *fixture) startWorker() string {
	f.t.Helper()
	f.handle(offers(offer("o2", good, 31100)))
	for id, n := range f.fw.Launched {
		if !n.IsHead {
			return id
		}
	}
	f.t.Fatal("no worker was launched")
	return ""
}

func TestBadPrincipalIsRefusedAtConstruction(t *testing.T) {
	f := newFixture(t, nil)
	f.config.Principal = "nobody"
	if _, err := New(f.client, f.store, f.config); err == nil {
		t.Error("a bad principal was accepted")
	}
}

func TestFirstOffersLaunchTheHeadOnlyAndDeclineTheRest(t *testing.T) {
	f := newFixture(t, nil)
	f.handle(offers(offer("o1", good, 31000), offer("o2", good, 31000),
		offer("o3", map[string]string{"containerizer": "medios", "host": "other"}, 31000)))
	accepts, declines := f.client.named("accept"), f.client.named("decline")
	if len(accepts) != 1 || accepts[0].id != "o1" || !strings.Contains(accepts[0].task.Command.Value, "--head") ||
		accepts[0].task.Principal != "team:t" {
		t.Fatalf("%+v", accepts)
	}
	if len(declines) != 2 || declines[0].id != "o2" || declines[1].id != "o3" || declines[0].arg != "120" {
		t.Errorf("%+v", declines)
	}
	pods := f.pods()
	if len(pods) != 1 || pods["c1-headgroup-1"].Metadata.Labels["ray.io/node-type"] != "head" || running(pods["c1-headgroup-1"]) {
		t.Errorf("%+v", pods)
	}
	// The worker waits for the head, so no offers are wanted.
	if len(f.client.named("suppress")) != 1 {
		t.Error("offers were not suppressed")
	}
	if f.fw.DashboardURL != "http://node-a:31003" {
		t.Error(f.fw.DashboardURL)
	}
}

func TestWorkerIsLaunchedAfterTheHeadRunsAndGetsItsAddress(t *testing.T) {
	f := newFixture(t, nil)
	headID := f.startHead()
	if len(f.client.named("ack")) != 1 || len(f.client.named("revive")) != 1 || !running(f.pods()[headID]) {
		t.Fatalf("%+v", f.client.calls)
	}
	f.startWorker()
	command := f.client.named("accept")[1].task.Command.Value
	if !strings.Contains(command, "--address=node-a:31000") || strings.Contains(command, "--head") {
		t.Error(command)
	}
	// The worker waits for the GCS before `ray start`.
	if !strings.Contains(command, "until ray health-check --address node-a:31000 > /dev/null 2>&1; do sleep 5; done\nexec ray start") {
		t.Error(command)
	}
	if len(f.pods()) != 2 {
		t.Error(f.pods())
	}
	f.handle(offers(offer("o3", good, 31200)))
	declines := f.client.named("decline")
	if len(f.client.named("accept")) != 2 || declines[len(declines)-1].id != "o3" {
		t.Error("an offer was used after replicas were met")
	}
}

func TestFailedAcceptDeclinesTheOfferAndRecordsNothing(t *testing.T) {
	f := newFixture(t, nil)
	f.client.failAccept = true
	f.handle(offers(offer("o1", good, 31000)))
	if d := f.client.named("decline"); len(d) != 1 || d[0].id != "o1" || len(f.fw.Launched) != 0 || len(f.pods()) != 0 {
		t.Errorf("%+v", f.client.calls)
	}
}

func TestWorkerNamedForRemovalIsKilledOnceAndMarked(t *testing.T) {
	f := newFixture(t, nil)
	f.startHead()
	workerID := f.startWorker()
	f.handle(update(workerID, medios.TaskRunning))
	_, err := f.store.PatchCluster("ns", "c1", []byte(`[{"op":"replace","path":"/spec/workerGroupSpecs/0/replicas","value":0},
		{"op":"replace","path":"/spec/workerGroupSpecs/0/scaleStrategy","value":{"workersToDelete":["`+workerID+`"]}}]`))
	if err != nil {
		t.Fatal(err)
	}
	f.handle(medios.Event{Type: medios.EventHeartbeat})
	f.handle(medios.Event{Type: medios.EventHeartbeat})
	kills := f.client.named("kill")
	if len(kills) != 1 || kills[0].id != workerID || kills[0].arg != "n1" {
		t.Fatalf("%+v", kills)
	}
	if f.pods()[workerID].Metadata.DeletionTimestamp != "2026-11-17T19:00:00Z" {
		t.Error(f.pods()[workerID].Metadata)
	}
	f.handle(update(workerID, medios.TaskKilled))
	if _, ok := f.pods()[workerID]; ok || f.fw.Stopped != "" || len(f.fw.Launched) != 1 {
		t.Errorf("stopped=%q pods=%v", f.fw.Stopped, f.pods())
	}
}

func TestHeadTaskEndingStopsTheFrameworkWithoutARelaunch(t *testing.T) {
	f := newFixture(t, nil)
	headID := f.startHead()
	f.handle(update(headID, medios.TaskFailed))
	if !strings.HasPrefix(f.fw.Stopped, "head task c1-headgroup-1 ended") || len(f.pods()) != 0 {
		t.Fatal(f.fw.Stopped)
	}
	f.handle(offers(offer("o9", good, 31000)))
	declines := f.client.named("decline")
	if len(f.client.named("accept")) != 1 || declines[len(declines)-1].id != "o9" {
		t.Error("an offer was used after the framework stopped")
	}
}

func TestWorkerTaskEndingUnaskedStopsTheFramework(t *testing.T) {
	f := newFixture(t, nil)
	f.startHead()
	workerID := f.startWorker()
	f.handle(update(workerID, medios.TaskFailed))
	if !strings.HasPrefix(f.fw.Stopped, "worker task "+workerID+" ended") {
		t.Error(f.fw.Stopped)
	}
}

func TestControllerErrorStopsTheFramework(t *testing.T) {
	f := newFixture(t, nil)
	f.handle(medios.Event{Type: medios.EventError, Error: &medios.ErrorBody{Message: "framework removed"}})
	if f.fw.Stopped != "controller error: framework removed" {
		t.Error(f.fw.Stopped)
	}
}

func enableAutoscaling(t *testing.T, s *store.Memory) {
	t.Helper()
	if _, err := s.PatchCluster("ns", "c1", []byte(`[{"op":"add","path":"/spec/enableInTreeAutoscaling","value":true}]`)); err != nil {
		t.Fatal(err)
	}
}

func TestAutoscalingRecordWithoutAnEndpointIsRefused(t *testing.T) {
	f := newFixture(t, nil)
	enableAutoscaling(t, f.store)
	if _, err := New(f.client, f.store, f.config); err == nil || !strings.Contains(err.Error(), "autoscaler endpoint") {
		t.Error(err)
	}
}

func TestAutoscalingRecordPutsTheAutoscalerInTheHeadsTask(t *testing.T) {
	f := newFixture(t, func(c *Config, s *store.Memory) {
		enableAutoscaling(t, s)
		c.AutoscalerEndpoint = &tasks.AutoscalerEndpoint{Host: "fw-host", Port: 8443, Namespace: "ns",
			CACertPEM: "CA", ClientCertPEM: "CERT", ClientKeyPEM: "KEY"}
	})
	f.handle(offers(offer("o1", good, 31000)))
	task := f.client.named("accept")[0].task
	env := map[string]string{}
	for _, v := range task.Command.Environment.Variables {
		env[v.Name] = v.Value
	}
	if env["KUBERNETES_SERVICE_HOST"] != "fw-host" || env["KUBERNETES_SERVICE_PORT_HTTPS"] != "8443" ||
		env["AUTOSCALER_METRIC_PORT"] != "31004" || env["RAYMEDIOS_CLIENT_KEY_BASE64"] != "S0VZ" {
		t.Errorf("%v", env)
	}
	if !strings.Contains(task.Command.Value, "exec ray kuberay-autoscaler --cluster-name c1 --cluster-namespace ns --gcs-address node-a:31000 ) &") {
		t.Error(task.Command.Value)
	}
	for _, r := range task.Resources {
		if r.Name == "ports" && len(r.Ranges.Range) != 15 {
			t.Errorf("%d ports", len(r.Ranges.Range))
		}
	}
	f.handle(update("c1-headgroup-1", medios.TaskRunning))
	f.startWorker()
	if strings.Contains(f.client.named("accept")[1].task.Command.Value, "kuberay-autoscaler") {
		t.Error("a worker's task runs the autoscaler")
	}
}

func TestOneOfferLaunchesEveryWorkerItHoldsInOneAccept(t *testing.T) {
	f := newFixture(t, func(_ *Config, s *store.Memory) {
		if _, err := s.PutCluster([]byte(strings.Replace(record, `"replicas":1`, `"replicas":3`, 1))); err != nil {
			t.Fatal(err)
		}
	})
	f.startHead()
	before := f.client.accepts
	// 8 CPUs and 100 ports hold three 2-CPU workers of 10 ports each.
	f.handle(offers(offer("o2", good, 31100)))
	accepts := f.client.named("accept")[1:]
	if f.client.accepts-before != 1 || len(accepts) != 3 {
		t.Fatalf("%d ACCEPT calls carrying %d tasks", f.client.accepts-before, len(accepts))
	}
	seen := map[uint64]string{}
	for _, a := range accepts {
		for _, r := range a.task.Resources {
			if r.Ranges == nil {
				continue
			}
			for _, p := range r.Ranges.Ports() {
				if other, taken := seen[p]; taken {
					t.Fatalf("port %d is in tasks %s and %s", p, other, a.task.TaskID.Value)
				}
				seen[p] = a.task.TaskID.Value
			}
		}
	}
	if len(seen) != 30 || len(f.pods()) != 4 {
		t.Errorf("%d ports, %d pods", len(seen), len(f.pods()))
	}
}

func TestAnOfferTooSmallForEveryWorkerLaunchesTheOnesThatFit(t *testing.T) {
	f := newFixture(t, func(_ *Config, s *store.Memory) {
		if _, err := s.PutCluster([]byte(strings.Replace(record, `"replicas":1`, `"replicas":3`, 1))); err != nil {
			t.Fatal(err)
		}
	})
	f.startHead()
	small := offer("o2", good, 31100)
	small.Resources[0].Scalar = &medios.Scalar{Value: 5} // two 2-CPU workers
	f.handle(offers(small))
	if n := len(f.client.named("accept")) - 1; n != 2 {
		t.Fatalf("%d workers launched", n)
	}
	if small.Resources[0].Scalar.Value != 5 {
		t.Error("the offer passed in was changed")
	}
}
