package tasks

import (
	"slices"
	"strconv"
	"strings"
	"testing"
)

var attrs = map[string]string{"gpu": "gb10"}

func workerPorts() NodePorts {
	return NodePorts{7011, 7012, 7013, 7014, 7015, 7016, []int{7110, 7111}}
}

func worker() RayNode {
	return RayNode{
		Cluster: "c1", Group: "small-group", InstanceID: "c1-small-group-ab12", Address: "10.0.0.5",
		NumCPUs: 2, MemoryMB: 4096, ObjectStoreMB: 1024, Ports: workerPorts(),
		AuthToken: "tok", HeadAddress: "10.0.0.6:6379",
	}
}

func head() RayNode {
	n := worker()
	n.Group, n.InstanceID, n.HeadAddress = "headgroup", "c1-head", ""
	n.Ports = NodePorts{7001, 7002, 7003, 7004, 7005, 7006, []int{7100, 7101}}
	n.Head = &HeadPorts{GCS: 6379, ClientServer: 7007, DashboardMetrics: 7008, Dashboard: 7009, AutoscalerMetrics: 7010}
	return n
}

func mustPlan(t *testing.T, n RayNode, c Containerizer) TaskPlan {
	t.Helper()
	p, err := Plan(n, c, attrs)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func has(p TaskPlan, args ...string) bool {
	for _, a := range args {
		if !slices.Contains(p.Argv, a) {
			return false
		}
	}
	return true
}

func TestWorkerPlanSetsEveryPortAndReservesExactlyThose(t *testing.T) {
	n := worker()
	p := mustPlan(t, n, Medios)
	line := strings.Join(p.Argv, " ")
	for _, port := range n.AllPorts() {
		if !strings.Contains(line, strconv.Itoa(port)) {
			t.Errorf("port %d is not on the command line", port)
		}
	}
	got, want := slices.Sorted(slices.Values(p.Ports)), slices.Sorted(slices.Values(n.AllPorts()))
	if !slices.Equal(got, want) {
		t.Errorf("ports %v, want %v", got, want)
	}
	if !has(p, "--address=10.0.0.6:6379", "--block") || has(p, "--head") {
		t.Errorf("argv %v", p.Argv)
	}
}

func TestResourcesAreAlwaysExplicitAndInBytes(t *testing.T) {
	p := mustPlan(t, worker(), Medios)
	if !has(p, "--num-cpus=2", "--num-gpus=0", "--memory=4294967296", "--object-store-memory=1073741824") {
		t.Errorf("argv %v", p.Argv)
	}
}

func TestIdentityReachesTheEnvironment(t *testing.T) {
	p := mustPlan(t, worker(), Medios)
	for name, want := range map[string]string{
		"RAY_CLOUD_INSTANCE_ID": "c1-small-group-ab12", "RAY_NODE_TYPE_NAME": "small-group", "RAY_AUTH_MODE": "token", "RAY_AUTH_TOKEN": "tok",
	} {
		if got, _ := p.EnvValue(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if _, ok := p.EnvValue("RAY_enable_autoscaler_v2"); ok {
		t.Error("a worker sets RAY_enable_autoscaler_v2")
	}
}

func TestHeadPlanCarriesHeadOnlySettings(t *testing.T) {
	p := mustPlan(t, head(), Medios)
	if !has(p, "--head", "--port=6379", "--ray-client-server-port=7007",
		"--include-dashboard=true", "--dashboard-host=0.0.0.0", "--dashboard-port=7009") {
		t.Errorf("argv %v", p.Argv)
	}
	if has(p, "--no-monitor") || len(p.ShellBefore) != 0 {
		t.Error("a head without an autoscaler endpoint sets --no-monitor or runs shell commands")
	}
	if _, ok := p.EnvValue("RAY_enable_autoscaler_v2"); ok {
		t.Error("RAY_enable_autoscaler_v2 is set without an autoscaler endpoint")
	}
	if v, _ := p.EnvValue("AUTOSCALER_METRIC_PORT"); v != "7010" {
		t.Errorf("AUTOSCALER_METRIC_PORT = %q", v)
	}
	if v, _ := p.EnvValue("DASHBOARD_METRIC_PORT"); v != "7008" {
		t.Errorf("DASHBOARD_METRIC_PORT = %q", v)
	}
	for _, port := range []int{6379, 7007, 7008, 7009, 7010} {
		if !slices.Contains(p.Ports, port) {
			t.Errorf("port %d is not reserved", port)
		}
	}
	for _, a := range p.Argv {
		if strings.HasPrefix(a, "--address=") {
			t.Errorf("a head has %s", a)
		}
	}
}

func TestContainerizerFields(t *testing.T) {
	m := mustPlan(t, worker(), Medios)
	if m.LinuxInfo != nil || m.Placement["containerizer"] != "medios" || m.Placement["gpu"] != "gb10" {
		t.Errorf("medios plan: %+v %v", m.LinuxInfo, m.Placement)
	}
	d := mustPlan(t, worker(), DockerShell)
	if d.LinuxInfo == nil || *d.LinuxInfo != (LinuxInfo{"PRIVATE", 1024}) || d.Placement["containerizer"] != "docker_shell" {
		t.Errorf("docker_shell plan: %+v %v", d.LinuxInfo, d.Placement)
	}
	if !slices.Equal(m.Argv, d.Argv) {
		t.Error("the command line differs between containerizers")
	}
}

func TestInvalidNodeIsRefusedWithTheReason(t *testing.T) {
	cases := []struct {
		message string
		change  func(*RayNode)
	}{
		{"workers is empty", func(n *RayNode) { n.Ports.Workers = nil }},
		{"used twice", func(n *RayNode) { n.Ports.ObjectManager = 7011 }},
		{"outside", func(n *RayNode) { n.Ports.NodeManager = 0 }},
		{"a head takes HeadPorts", func(n *RayNode) { n.Head = &HeadPorts{1, 2, 3, 4, 5} }},
		{"auth_token is empty", func(n *RayNode) { n.AuthToken = "" }},
		{"address is empty", func(n *RayNode) { n.Address = "" }},
		{"must be positive", func(n *RayNode) { n.ObjectStoreMB = 0 }},
		{"only a head", func(n *RayNode) { n.Autoscaler = &AutoscalerEndpoint{"fw", 8443, "ns", "CA", "CERT", "KEY"} }},
	}
	for _, c := range cases {
		n := worker()
		c.change(&n)
		if _, err := Plan(n, Medios, attrs); err == nil || !strings.Contains(err.Error(), c.message) {
			t.Errorf("want an error containing %q, got %v", c.message, err)
		}
	}
	h := head()
	h.Head = &HeadPorts{}
	if _, err := Plan(h, Medios, attrs); err == nil || !strings.Contains(err.Error(), "outside") {
		t.Errorf("head with unset head ports: %v", err)
	}
	if _, err := Plan(worker(), Medios, map[string]string{"containerizer": "docker_shell"}); err == nil {
		t.Error("containerizer was accepted as a node attribute")
	}
}

func TestHeadWithAnAutoscalerEndpointRunsTheStockAutoscaler(t *testing.T) {
	h := head()
	h.Autoscaler = &AutoscalerEndpoint{"fw", 8443, "ns", "CA", "CERT", "KEY"}
	p := mustPlan(t, h, Medios)
	if v, _ := p.EnvValue("RAY_enable_autoscaler_v2"); !has(p, "--no-monitor") || v != "1" {
		t.Error("--no-monitor or RAY_enable_autoscaler_v2 is missing")
	}
	last := p.ShellBefore[len(p.ShellBefore)-1]
	if !strings.HasSuffix(last, "--cluster-namespace ns --gcs-address 10.0.0.5:6379 ) &") {
		t.Errorf("last shell command: %s", last)
	}
}

func TestShellQuoteMatchesPOSIXQuoting(t *testing.T) {
	if got := ShellJoin([]string{"sh", "-c", "echo 'a b' && exit 0"}); got != `sh -c 'echo '"'"'a b'"'"' && exit 0'` {
		t.Error(got)
	}
	if ShellQuote("") != "''" {
		t.Error("empty word")
	}
}

func TestEveryDockerShellTaskRunsInTheNodesNetworkNamespace(t *testing.T) {
	for _, c := range []struct {
		node          RayNode
		containerizer Containerizer
		want          bool
	}{{head(), DockerShell, true}, {worker(), DockerShell, true}, {head(), Medios, false}, {worker(), Medios, false}} {
		p, err := Plan(c.node, c.containerizer, map[string]string{"gpu": "gb10"})
		if err != nil {
			t.Fatal(err)
		}
		if p.HostNetwork != c.want {
			t.Errorf("%s head=%v: HostNetwork = %v", c.containerizer, c.node.Head != nil, p.HostNetwork)
		}
	}
}
