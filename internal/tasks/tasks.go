// Package tasks decides what the framework puts in the Medios task of one Ray
// node.
//
// Plan is a pure function from a node description and a containerizer to the
// command line, environment, port reservation and containerizer-specific
// fields. It imports no Medios SDK; package launch turns a TaskPlan into a
// TaskInfo. The reasoning and evidence are in docs/task-mapping.md.
package tasks

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// MiB is the number of bytes in one mebibyte.
const MiB = 1024 * 1024

// Containerizer is the Medios containerizer a cluster's tasks are placed on.
//
// Type: parameter, set per cluster, with no default: the caller names one.
// Both medios and docker_shell run tasks in the node's network namespace
// (see "Containerizer" in docs/task-mapping.md).
type Containerizer string

const (
	Medios      Containerizer = "medios"
	DockerShell Containerizer = "docker_shell"
)

// NodePorts are the ports of one Ray node. Each is reserved from the offer
// and set on the command line, because Medios does not tell a process what it
// reserved.
type NodePorts struct {
	NodeManager          int
	ObjectManager        int
	RuntimeEnvAgent      int
	MetricsExport        int
	DashboardAgentListen int
	DashboardAgentGRPC   int
	// One per Ray worker process, and one per driver that attaches to this
	// node. Ray refuses to start a worker process when the list is used up.
	Workers []int
}

// HeadPorts are the ports a head has besides NodePorts.
type HeadPorts struct {
	GCS int
	// Ray starts a client server on the head whenever its client extra is
	// installed, on 10001 unless told otherwise.
	ClientServer int
	// The dashboard process serves metrics on DASHBOARD_METRIC_PORT, 44227
	// unless told otherwise.
	DashboardMetrics int
	// The dashboard's HTTP port, which also serves job submission.
	Dashboard int
	// Metrics port of the autoscaler (AUTOSCALER_METRIC_PORT, 44217 unless
	// told otherwise). The stock autoscaler serves it when the head's task
	// runs one; otherwise the head's own monitor process does.
	AutoscalerMetrics int
}

// GCSWaitIntervalS is the number of seconds between two checks of whether the
// head's GCS answers. Type: default. Reason: the value in KubeRay's
// wait-gcs-ready init container (ray-operator/controllers/ray/common/pod.go).
const GCSWaitIntervalS = 5

// AutoscalerEndpoint says where the stock autoscaler in the head's task finds
// the framework's HTTPS endpoint, and holds the mutual-TLS material it
// presents. The three PEM texts are supplied by whoever runs the framework
// and are never generated here. They reach the task as plain environment
// variables, as the Ray auth token does, so they are readable in the Medios
// controller's state.
type AutoscalerEndpoint struct {
	Host          string
	Port          int
	Namespace     string
	CACertPEM     string
	ClientCertPEM string
	ClientKeyPEM  string
}

// RayNode describes one Ray node.
type RayNode struct {
	Cluster string
	Group   string
	// Equals metadata.name of the node's pod record; the autoscaler matches
	// the two through RAY_CLOUD_INSTANCE_ID.
	InstanceID string
	// The offer's hostname, or an IP address of the Medios node. Other Ray
	// nodes connect to it. Both forms have run (docs/task-mapping.md,
	// "Addresses").
	Address       string
	NumCPUs       int
	NumGPUs       int
	MemoryMB      int
	ObjectStoreMB int
	Ports         NodePorts
	// Set on a head and nil on a worker.
	Head *HeadPorts
	// The cluster's Ray auth token. It reaches the task as the plain
	// environment variable RAY_AUTH_TOKEN, so it is readable in the Medios
	// controller's state. Neither containerizer delivers a SECRET variable.
	AuthToken string
	// "<head address>:<GCS port>" for a worker; empty makes this node the head.
	HeadAddress string
	// Set on a head whose task also runs the stock autoscaler, as the
	// autoscaler container of a KubeRay head pod does.
	Autoscaler *AutoscalerEndpoint
	// The `env` of the group's first container in the RayCluster record.
	// A KubeRay pod's Ray container gets these variables; so does the task.
	ContainerEnv []EnvVar
}

// AllPorts lists every port of the node.
func (n RayNode) AllPorts() []int {
	p := n.Ports
	ports := []int{p.NodeManager, p.ObjectManager, p.RuntimeEnvAgent, p.MetricsExport,
		p.DashboardAgentListen, p.DashboardAgentGRPC}
	ports = append(ports, p.Workers...)
	if h := n.Head; h != nil {
		ports = append(ports, h.GCS, h.ClientServer, h.DashboardMetrics, h.Dashboard, h.AutoscalerMetrics)
	}
	return ports
}

// EnvVar is one environment variable of a task.
type EnvVar struct {
	Name  string
	Value string
}

// LinuxInfo holds the LinuxInfo fields a task needs.
type LinuxInfo struct {
	IPCMode   string
	ShmSizeMB int
}

// TaskPlan is the content of one Ray node's Medios task.
type TaskPlan struct {
	// The `ray start` command line, run last and in the foreground.
	Argv []string
	// Shell commands the task runs before Argv, one per item.
	ShellBefore []string
	Env         []EnvVar
	Ports       []int
	// Node attributes the offer must carry.
	Placement map[string]string
	// Nil when the containerizer needs no LinuxInfo.
	LinuxInfo *LinuxInfo
	// HostNetwork asks docker_shell to run the task in the node's network
	// namespace (`docker.network: HOST`).
	HostNetwork bool
}

// EnvValue returns the value of the named variable.
func (p TaskPlan) EnvValue(name string) (string, bool) {
	for _, e := range p.Env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

func validate(n RayNode) error {
	isHead := n.HeadAddress == ""
	if isHead != (n.Head != nil) {
		return fmt.Errorf("a head takes HeadPorts and a worker takes none")
	}
	if len(n.Ports.Workers) == 0 {
		return fmt.Errorf("ports.workers is empty; Ray could start no worker process")
	}
	if !isHead && n.Autoscaler != nil {
		return fmt.Errorf("only a head runs the autoscaler")
	}
	ports := n.AllPorts()
	var bad []int
	seen := map[int]bool{}
	twice := false
	for _, p := range ports {
		if p <= 0 || p >= 65536 {
			bad = append(bad, p)
		}
		if seen[p] {
			twice = true
		}
		seen[p] = true
	}
	if len(bad) > 0 {
		return fmt.Errorf("ports outside 1-65535, or unset: %v", bad)
	}
	if twice {
		sorted := append([]int(nil), ports...)
		sort.Ints(sorted)
		return fmt.Errorf("a port is used twice: %v", sorted)
	}
	for _, f := range []struct{ name, value string }{
		{"cluster", n.Cluster}, {"group", n.Group}, {"instance_id", n.InstanceID},
		{"address", n.Address}, {"auth_token", n.AuthToken},
	} {
		if f.value == "" {
			return fmt.Errorf("%s is empty", f.name)
		}
	}
	if n.NumCPUs < 0 || n.NumGPUs < 0 {
		return fmt.Errorf("num_cpus and num_gpus must not be negative")
	}
	if n.MemoryMB <= 0 || n.ObjectStoreMB <= 0 {
		return fmt.Errorf("memory_mb and object_store_mb must be positive")
	}
	return nil
}

// Plan plans the task of one Ray node.
//
// nodeAttributes are the Medios node attributes, besides containerizer, that
// an offer must carry for this cluster's tasks. The caller always names them:
// several nodes of a fleet share one containerizer, and a cluster is meant
// for some of them.
func Plan(n RayNode, containerizer Containerizer, nodeAttributes map[string]string) (TaskPlan, error) {
	if err := validate(n); err != nil {
		return TaskPlan{}, err
	}
	if _, ok := nodeAttributes["containerizer"]; ok {
		return TaskPlan{}, fmt.Errorf("node attributes must not name containerizer; pass it as the containerizer")
	}
	p := n.Ports
	workers := make([]string, len(p.Workers))
	for i, w := range p.Workers {
		workers[i] = strconv.Itoa(w)
	}
	argv := []string{
		"ray",
		"start",
		// Keeps `ray start` in the foreground so the task lives as long as
		// the Ray node does, as KubeRay's pods do.
		"--block",
		"--node-ip-address=" + n.Address,
		fmt.Sprintf("--node-manager-port=%d", p.NodeManager),
		fmt.Sprintf("--object-manager-port=%d", p.ObjectManager),
		fmt.Sprintf("--runtime-env-agent-port=%d", p.RuntimeEnvAgent),
		fmt.Sprintf("--metrics-export-port=%d", p.MetricsExport),
		fmt.Sprintf("--dashboard-agent-listen-port=%d", p.DashboardAgentListen),
		fmt.Sprintf("--dashboard-agent-grpc-port=%d", p.DashboardAgentGRPC),
		"--worker-port-list=" + strings.Join(workers, ","),
		// Always explicit: a Medios task cannot read its own limits and Ray
		// would otherwise size itself to the host.
		fmt.Sprintf("--num-cpus=%d", n.NumCPUs),
		fmt.Sprintf("--num-gpus=%d", n.NumGPUs),
		fmt.Sprintf("--memory=%d", n.MemoryMB*MiB),
		fmt.Sprintf("--object-store-memory=%d", n.ObjectStoreMB*MiB),
	}
	env := []EnvVar{
		{"RAY_CLOUD_INSTANCE_ID", n.InstanceID},
		{"RAY_NODE_TYPE_NAME", n.Group},
		// Ray 2.60.0 clients, `ray health-check` among them, send the token
		// only in this mode; without it the head refuses them with
		// InvalidAuthToken (docs/task-mapping.md, "Auth token").
		{"RAY_AUTH_MODE", "token"},
		{"RAY_AUTH_TOKEN", n.AuthToken},
	}
	var shellBefore []string
	if h := n.Head; h != nil {
		argv = append(argv,
			"--head",
			fmt.Sprintf("--port=%d", h.GCS),
			fmt.Sprintf("--ray-client-server-port=%d", h.ClientServer),
			// The head runs the dashboard, as a KubeRay head does. KubeRay
			// binds it to the wildcard address (--dashboard-host=0.0.0.0).
			"--include-dashboard=true",
			"--dashboard-host=0.0.0.0",
			fmt.Sprintf("--dashboard-port=%d", h.Dashboard),
		)
		env = append(env,
			EnvVar{"DASHBOARD_METRIC_PORT", strconv.Itoa(h.DashboardMetrics)},
			EnvVar{"AUTOSCALER_METRIC_PORT", strconv.Itoa(h.AutoscalerMetrics)},
		)
		if n.Autoscaler != nil {
			// The stock autoscaler command replaces the head's own monitor.
			// KubeRay sets no-monitor under the same condition (pod.go).
			argv = append(argv, "--no-monitor")
			env = append(env, EnvVar{"RAY_enable_autoscaler_v2", "1"})
			commands, more, err := autoscalerCommands(n, fmt.Sprintf("%s:%d", n.Address, h.GCS))
			if err != nil {
				return TaskPlan{}, err
			}
			shellBefore, env = commands, append(env, more...)
		}
	} else {
		argv = append(argv, "--address="+n.HeadAddress)
		// A worker's task reaches TASK_RUNNING before the head's GCS
		// listens. KubeRay gives worker pods an init container that waits on
		// `ray health-check`; this is the same wait.
		shellBefore = []string{waitForGCS(n.HeadAddress)}
	}

	var linuxInfo *LinuxInfo
	hostNetwork := false
	switch containerizer {
	case Medios:
		// The task runs in the node's network namespace and binds its ports
		// there. No LinuxInfo: an image task's /dev/shm is a tmpfs sized by
		// the node, which the task cannot change without the IPC isolator.
	case DockerShell:
		// Medios refuses shm_size unless ipc_mode is PRIVATE, and without
		// both the container gets Docker's 64 MB /dev/shm. The object store
		// lives in /dev/shm, so the two sizes are equal. Type: heuristic;
		// whether Ray needs headroom above the object store size is
		// unmeasured.
		linuxInfo = &LinuxInfo{IPCMode: "PRIVATE", ShmSizeMB: n.ObjectStoreMB}
		// Every Ray node's task runs in the node's network namespace. On
		// the Docker bridge a head's listeners fail to bind the node's
		// address and a worker's runtime env agent exits shortly after start
		// (docs/task-mapping.md, "Containerizer"). A KubeRay pod has its own
		// network namespace; host networking is an intentional docker_shell
		// departure in v0.1.0.
		hostNetwork = true
	default:
		return TaskPlan{}, fmt.Errorf("unknown containerizer: %q", containerizer)
	}

	// A variable the framework sets carries an address, a port or the auth
	// token of this Ray node. KubeRay keeps the record's value for most of
	// its own variables; the framework refuses the record instead, because a
	// replaced value here would point the Ray node at the wrong place.
	set := map[string]bool{}
	for _, e := range env {
		set[e.Name] = true
	}
	for _, e := range n.ContainerEnv {
		if set[e.Name] {
			return TaskPlan{}, fmt.Errorf("group %s: the record's container env sets %s, which the framework sets", n.Group, e.Name)
		}
		set[e.Name] = true
		env = append(env, e)
	}

	placement := map[string]string{"containerizer": string(containerizer)}
	for k, v := range nodeAttributes {
		placement[k] = v
	}
	return TaskPlan{
		Argv: argv, ShellBefore: shellBefore, Env: env, Ports: n.AllPorts(),
		Placement: placement, LinuxInfo: linuxInfo, HostNetwork: hostNetwork,
	}, nil
}

func waitForGCS(gcsAddress string) string {
	return fmt.Sprintf("until ray health-check --address %s > /dev/null 2>&1; do sleep %d; done",
		ShellQuote(gcsAddress), GCSWaitIntervalS)
}

// autoscalerCommands returns the shell commands that write the TLS files and
// start the stock autoscaler in the background, and the environment they need.
//
// The command is KubeRay's (`ray kuberay-autoscaler --cluster-name
// --cluster-namespace`) plus `--gcs-address`, because the head's GCS is not
// on Ray's default port. The PEM texts travel base64-encoded, so the image
// needs `base64` and `mktemp`.
func autoscalerCommands(n RayNode, gcsAddress string) ([]string, []EnvVar, error) {
	a := n.Autoscaler
	for _, f := range []struct{ name, value string }{
		{"host", a.Host}, {"namespace", a.Namespace}, {"ca_cert_pem", a.CACertPEM},
		{"client_cert_pem", a.ClientCertPEM}, {"client_key_pem", a.ClientKeyPEM},
	} {
		if f.value == "" {
			return nil, nil, fmt.Errorf("autoscaler endpoint %s is empty", f.name)
		}
	}
	if a.Port <= 0 || a.Port >= 65536 {
		return nil, nil, fmt.Errorf("autoscaler endpoint port outside 1-65535: %d", a.Port)
	}
	env := []EnvVar{
		{"KUBERNETES_SERVICE_HOST", a.Host},
		{"KUBERNETES_SERVICE_PORT_HTTPS", strconv.Itoa(a.Port)},
	}
	commands := []string{"umask 077", "tls_dir=$(mktemp -d)"}
	for _, f := range []struct{ file, pem string }{
		{"ca.crt", a.CACertPEM}, {"client.crt", a.ClientCertPEM}, {"client.key", a.ClientKeyPEM},
	} {
		variable := "RAYMEDIOS_" + strings.ToUpper(strings.ReplaceAll(f.file, ".", "_")) + "_BASE64"
		env = append(env, EnvVar{variable, base64.StdEncoding.EncodeToString([]byte(f.pem))})
		commands = append(commands, fmt.Sprintf(`printf %%s "$%s" | base64 -d > "$tls_dir/%s"`, variable, f.file))
	}
	commands = append(commands,
		`export RAY_KUBERAY_CA_CERT_PATH="$tls_dir/ca.crt" RAY_KUBERAY_CLIENT_CERT_PATH="$tls_dir/client.crt" `+
			`RAY_KUBERAY_CLIENT_KEY_PATH="$tls_dir/client.key"`,
		fmt.Sprintf("( %s; exec ray kuberay-autoscaler --cluster-name %s --cluster-namespace %s --gcs-address %s ) &",
			waitForGCS(gcsAddress), ShellQuote(n.Cluster), ShellQuote(a.Namespace), ShellQuote(gcsAddress)),
	)
	return commands, env, nil
}

var shellSafe = regexp.MustCompile(`^[A-Za-z0-9_@%+=:,./-]+$`)

// ShellQuote quotes one word for a POSIX shell.
func ShellQuote(s string) string {
	if shellSafe.MatchString(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}

// ShellJoin quotes each word and joins them with spaces.
func ShellJoin(words []string) string {
	quoted := make([]string, len(words))
	for i, w := range words {
		quoted[i] = ShellQuote(w)
	}
	return strings.Join(quoted, " ")
}
