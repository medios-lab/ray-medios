// Package schedule holds the decisions of the framework's scheduler as pure
// functions.
//
// Decide compares a RayCluster record with the Ray nodes the framework has
// already launched and returns which Ray nodes to launch and which to kill.
// Place fits one wanted Ray node onto one Medios offer. PodRecord builds the
// Pod-format record the stock autoscaler reads. Nothing here calls Medios;
// package framework applies these to the Medios scheduler's events.
package schedule

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/medios-lab/medios-sdk-go/medios"

	"github.com/medios-lab/ray-medios/internal/launch"
	"github.com/medios-lab/ray-medios/internal/store"
	"github.com/medios-lab/ray-medios/internal/tasks"
)

// HeadGroup is the Ray node type name KubeRay gives the head group.
const HeadGroup = "headgroup"

// ObjectStoreFraction is the share of a Ray node's memory given to its object
// store. Type: default. Reason: Ray's own default share when
// --object-store-memory is absent (DEFAULT_OBJECT_STORE_MEMORY_PROPORTION,
// ray_constants.py:110 at the pin). tasks.Plan always passes the size
// explicitly because a Medios task cannot read its limits.
const ObjectStoreFraction = 0.3

// ObjectStoreMinimumMB is the smallest object store Ray accepts, in MiB.
// Type: invariant of Ray (OBJECT_STORE_MINIMUM_MEMORY_BYTES,
// ray_constants.py:116 at the pin).
const ObjectStoreMinimumMB = 75

// Ports a head and a worker need besides the ports for Ray worker processes.
// Type: invariant of tasks.NodePorts and tasks.HeadPorts.
const (
	headFixedPorts   = 11
	workerFixedPorts = 6
)

// rayStartParams keys that would size a Ray node. The stock autoscaler reads
// them before container limits (autoscaling_config.py:366-460 at the pin).
var sizingStartParams = []string{"num-cpus", "num-gpus", "memory"}

var units = map[string]float64{
	"": 1, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12,
	"Ki": 1 << 10, "Mi": 1 << 20, "Gi": 1 << 30, "Ti": 1 << 40,
}

// quantity is a Kubernetes quantity, which JSON carries as a string or a
// number.
type quantity string

func (q *quantity) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		*q = quantity(s)
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("quantity is neither a string nor a number: %s", b)
	}
	*q = quantity(n.String())
	return nil
}

// ParseCPU reads a Kubernetes cpu quantity as whole CPUs, rounded up as
// KubeRay does.
func ParseCPU(text string) (int, error) {
	number, divisor := text, 1.0
	if strings.HasSuffix(text, "m") {
		number, divisor = strings.TrimSuffix(text, "m"), 1000
	}
	value, err := strconv.ParseFloat(number, 64)
	if err != nil {
		return 0, fmt.Errorf("cpu quantity not understood: %q", text)
	}
	value /= divisor
	if value <= 0 {
		return 0, fmt.Errorf("cpu quantity is not positive: %q", text)
	}
	return int(math.Ceil(value)), nil
}

var memoryPattern = regexp.MustCompile(`^([0-9.]+)([A-Za-z]*)$`)

// ParseMemoryMB reads a Kubernetes memory quantity in MiB. A quantity that is
// not a whole number of MiB is refused: the stock autoscaler would count its
// bytes rounded up while `ray start --memory` got a different number.
func ParseMemoryMB(text string) (int, error) {
	match := memoryPattern.FindStringSubmatch(text)
	if match == nil {
		return 0, fmt.Errorf("memory quantity not understood: %q", text)
	}
	unit, ok := units[match[2]]
	number, err := strconv.ParseFloat(match[1], 64)
	if !ok || err != nil {
		return 0, fmt.Errorf("memory quantity not understood: %q", text)
	}
	size := number * unit
	if size <= 0 || math.Mod(size, tasks.MiB) != 0 {
		return 0, fmt.Errorf("memory quantity is not a positive whole number of MiB: %q", text)
	}
	return int(size / tasks.MiB), nil
}

// Wanted is one Ray node the record asks for that no task exists for yet.
type Wanted struct {
	Group    string
	IsHead   bool
	CPUs     int
	MemoryMB int
	GPUs     int
	// The `env` of the group's first container.
	Env []tasks.EnvVar
}

// Launched is one Ray node the framework has a Medios task for.
type Launched struct {
	// Also the task ID and the pod record's metadata.name.
	InstanceID string
	Group      string
	IsHead     bool
	// The task's last status update was TASK_RUNNING.
	Running bool
	Address string
	GCSPort int // head only
	// RFC 3339 time at which the framework asked Medios to kill the task, or
	// empty. It becomes the pod record's metadata.deletionTimestamp, which
	// makes the stock autoscaler skip the record until the task is gone.
	KillRequestedAt string
}

// Decision is what Decide returns.
type Decision struct {
	Launch []Wanted
	Kill   []string // instance IDs
}

type groupSpec struct {
	GroupName      string                     `json:"groupName"`
	Replicas       *int                       `json:"replicas"`
	Resources      map[string]json.RawMessage `json:"resources"`
	RayStartParams map[string]json.RawMessage `json:"rayStartParams"`
	ScaleStrategy  struct {
		WorkersToDelete []string `json:"workersToDelete"`
	} `json:"scaleStrategy"`
	Template struct {
		Spec struct {
			Containers []struct {
				Env []struct {
					Name      string          `json:"name"`
					Value     string          `json:"value"`
					ValueFrom json.RawMessage `json:"valueFrom"`
				} `json:"env"`
				Resources struct {
					Limits   map[string]quantity `json:"limits"`
					Requests map[string]quantity `json:"requests"`
				} `json:"resources"`
			} `json:"containers"`
		} `json:"spec"`
	} `json:"template"`
}

// Record is the part of a RayCluster record the framework reads.
type Record struct {
	Metadata struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Spec struct {
		EnableInTreeAutoscaling bool        `json:"enableInTreeAutoscaling"`
		HeadGroupSpec           *groupSpec  `json:"headGroupSpec"`
		WorkerGroupSpecs        []groupSpec `json:"workerGroupSpecs"`
	} `json:"spec"`
}

// ParseRecord reads a RayCluster record.
func ParseRecord(raw []byte) (Record, error) {
	var r Record
	if err := json.Unmarshal(raw, &r); err != nil {
		return Record{}, fmt.Errorf("RayCluster record: %w", err)
	}
	if r.Spec.HeadGroupSpec == nil {
		return Record{}, fmt.Errorf("RayCluster record has no spec.headGroupSpec")
	}
	return r, nil
}

// groupEnv returns the `env` of a group's first container. An entry with
// `valueFrom` is refused: the framework has no Secret, ConfigMap or pod field
// to read the value from.
func groupEnv(g groupSpec, name string) ([]tasks.EnvVar, error) {
	if len(g.Template.Spec.Containers) == 0 {
		return nil, nil
	}
	var env []tasks.EnvVar
	for _, e := range g.Template.Spec.Containers[0].Env {
		if e.Name == "" {
			return nil, fmt.Errorf("group %s has a container env entry without a name", name)
		}
		if len(e.ValueFrom) > 0 && string(e.ValueFrom) != "null" {
			return nil, fmt.Errorf("group %s: container env %s uses valueFrom, which is not supported", name, e.Name)
		}
		env = append(env, tasks.EnvVar{Name: e.Name, Value: e.Value})
	}
	return env, nil
}

// groupResources returns the cpus, memory in MiB and gpus of a group, from
// its first container's limits and then requests, which is the stock
// autoscaler's order. The autoscaler prefers the group's `resources` field
// and sizing keys in `rayStartParams` over both; a record that sets those is
// refused, because the framework would size the Ray node differently from
// the autoscaler.
func groupResources(g groupSpec, name string) (cpus, memoryMB, gpus int, err error) {
	if len(g.Resources) > 0 {
		return 0, 0, 0, fmt.Errorf("group %s sets resources, which is not supported", name)
	}
	var used []string
	for _, k := range sizingStartParams {
		if _, ok := g.RayStartParams[k]; ok {
			used = append(used, k)
		}
	}
	if len(used) > 0 {
		return 0, 0, 0, fmt.Errorf("group %s sets rayStartParams %v, which is not supported", name, used)
	}
	if len(g.Template.Spec.Containers) == 0 {
		return 0, 0, 0, fmt.Errorf("group %s has no container", name)
	}
	res := g.Template.Spec.Containers[0].Resources
	get := func(field string) (string, error) {
		if q, ok := res.Limits[field]; ok {
			return string(q), nil
		}
		if q, ok := res.Requests[field]; ok {
			return string(q), nil
		}
		return "", fmt.Errorf("group %s has no %s in its first container's resources", name, field)
	}
	cpuText, err := get("cpu")
	if err != nil {
		return 0, 0, 0, err
	}
	memText, err := get("memory")
	if err != nil {
		return 0, 0, 0, err
	}
	// The first key ending in "gpu" with a positive count, limits before
	// requests. Keys of one map are taken in name order.
	for _, m := range []map[string]quantity{res.Limits, res.Requests} {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if !strings.HasSuffix(k, "gpu") {
				continue
			}
			n, perr := strconv.ParseFloat(string(m[k]), 64)
			if perr != nil {
				return 0, 0, 0, fmt.Errorf("group %s: %s quantity not understood: %q", name, k, m[k])
			}
			if gpus == 0 && math.Ceil(n) > 0 {
				gpus = int(math.Ceil(n))
			}
		}
	}
	if cpus, err = ParseCPU(cpuText); err != nil {
		return 0, 0, 0, fmt.Errorf("group %s: %w", name, err)
	}
	if memoryMB, err = ParseMemoryMB(memText); err != nil {
		return 0, 0, 0, fmt.Errorf("group %s: %w", name, err)
	}
	return cpus, memoryMB, gpus, nil
}

// Decide returns which Ray nodes to launch and which to kill.
//
// Workers are launched only once the head's task is running, because a
// worker's command line carries the head's address. A worker named in its
// group's scaleStrategy.workersToDelete is killed and is not counted toward
// replicas, which is how the stock autoscaler removes a worker
// (docs/endpoint-contract.md).
func Decide(record Record, launched []Launched) (Decision, error) {
	var d Decision
	var head *Launched
	for i := range launched {
		if launched[i].IsHead {
			head = &launched[i]
			break
		}
	}
	if head == nil {
		cpus, mem, gpus, err := groupResources(*record.Spec.HeadGroupSpec, HeadGroup)
		if err != nil {
			return Decision{}, err
		}
		env, err := groupEnv(*record.Spec.HeadGroupSpec, HeadGroup)
		if err != nil {
			return Decision{}, err
		}
		d.Launch = append(d.Launch, Wanted{HeadGroup, true, cpus, mem, gpus, env})
	}
	for _, g := range record.Spec.WorkerGroupSpecs {
		name := g.GroupName
		if name == "" || g.Replicas == nil {
			return Decision{}, fmt.Errorf("a worker group has no groupName or no replicas")
		}
		doomed := map[string]bool{}
		for _, id := range g.ScaleStrategy.WorkersToDelete {
			doomed[id] = true
		}
		kept := 0
		for _, n := range launched {
			if n.IsHead || n.Group != name {
				continue
			}
			if !doomed[n.InstanceID] {
				kept++
			} else if n.KillRequestedAt == "" {
				d.Kill = append(d.Kill, n.InstanceID)
			}
		}
		missing := *g.Replicas - kept
		if missing > 0 && head != nil && head.Running {
			cpus, mem, gpus, err := groupResources(g, name)
			if err != nil {
				return Decision{}, err
			}
			env, err := groupEnv(g, name)
			if err != nil {
				return Decision{}, err
			}
			for i := 0; i < missing; i++ {
				d.Launch = append(d.Launch, Wanted{name, false, cpus, mem, gpus, env})
			}
		}
	}
	return d, nil
}

// Placement is one wanted Ray node fitted onto an offer.
type Placement struct {
	Node tasks.RayNode
	Plan tasks.TaskPlan
	Size launch.Size
}

// Cluster holds the per-cluster inputs of Place.
type Cluster struct {
	Name           string
	Containerizer  tasks.Containerizer
	NodeAttributes map[string]string
	AuthToken      string
	// The number of ports reserved for Ray worker processes and attached
	// drivers on each node. Type: parameter, no default
	// (docs/task-mapping.md, "Ports").
	WorkerPorts int
	// Passed to the head's plan; nil when the head runs no autoscaler.
	Autoscaler *tasks.AutoscalerEndpoint
}

// Place fits wanted onto offer. It returns nil when the offer cannot hold it.
// head is the launched head, which a worker needs.
func Place(wanted Wanted, offer medios.Offer, c Cluster, instanceID string, head *Launched) (*Placement, error) {
	if c.WorkerPorts < 1 {
		return nil, fmt.Errorf("worker ports must be at least 1")
	}
	if !wanted.IsHead && head == nil {
		return nil, fmt.Errorf("a worker cannot be placed before the head")
	}
	needed := workerFixedPorts + c.WorkerPorts
	if wanted.IsHead {
		needed = headFixedPorts + c.WorkerPorts
	}
	ports := launch.OfferPorts(offer)
	placement := map[string]string{"containerizer": string(c.Containerizer)}
	for k, v := range c.NodeAttributes {
		placement[k] = v
	}
	if !launch.OfferMatches(offer, placement) ||
		len(ports) < needed ||
		launch.OfferScalar(offer, "cpus") < float64(wanted.CPUs) ||
		launch.OfferScalar(offer, "mem") < float64(wanted.MemoryMB) ||
		launch.OfferScalar(offer, "gpus") < float64(wanted.GPUs) {
		return nil, nil
	}
	objectStoreMB := int(float64(wanted.MemoryMB) * ObjectStoreFraction)
	if objectStoreMB < ObjectStoreMinimumMB {
		return nil, fmt.Errorf("group %s has %d MiB; its object store would be %d MiB and Ray requires %d",
			wanted.Group, wanted.MemoryMB, objectStoreMB, ObjectStoreMinimumMB)
	}
	p := ports[:needed]
	node := tasks.RayNode{
		Cluster: c.Name, Group: wanted.Group, InstanceID: instanceID, Address: offer.Hostname,
		NumCPUs: wanted.CPUs, NumGPUs: wanted.GPUs, MemoryMB: wanted.MemoryMB, ObjectStoreMB: objectStoreMB,
		AuthToken: c.AuthToken, ContainerEnv: wanted.Env,
	}
	fixed := p[:workerFixedPorts]
	if wanted.IsHead {
		node.Head = &tasks.HeadPorts{GCS: p[0], ClientServer: p[1], DashboardMetrics: p[2], Dashboard: p[3], AutoscalerMetrics: p[4]}
		node.Autoscaler = c.Autoscaler
		fixed = p[headFixedPorts-workerFixedPorts : headFixedPorts]
		node.Ports.Workers = p[headFixedPorts:]
	} else {
		node.HeadAddress = fmt.Sprintf("%s:%d", head.Address, head.GCSPort)
		node.Ports.Workers = p[workerFixedPorts:]
	}
	node.Ports.NodeManager, node.Ports.ObjectManager, node.Ports.RuntimeEnvAgent = fixed[0], fixed[1], fixed[2]
	node.Ports.MetricsExport, node.Ports.DashboardAgentListen, node.Ports.DashboardAgentGRPC = fixed[3], fixed[4], fixed[5]
	plan, err := tasks.Plan(node, c.Containerizer, c.NodeAttributes)
	if err != nil {
		return nil, err
	}
	return &Placement{node, plan, launch.Size{CPUs: float64(wanted.CPUs), MemoryMB: float64(wanted.MemoryMB), GPUs: float64(wanted.GPUs)}}, nil
}

// PodRecord builds the Pod-format record of one launched Ray node, as the
// stock autoscaler reads it (docs/endpoint-contract.md, pod fields).
func PodRecord(namespace, cluster string, n Launched) store.Pod {
	nodeType, state := "worker", "waiting"
	if n.IsHead {
		nodeType = "head"
	}
	if n.Running {
		state = "running"
	}
	return store.Pod{
		APIVersion: "v1", Kind: "Pod",
		Metadata: store.PodMetadata{
			Name: n.InstanceID, Namespace: namespace,
			Labels: map[string]string{
				store.ClusterLabel: cluster, "ray.io/node-type": nodeType, "ray.io/group": n.Group,
			},
			DeletionTimestamp: n.KillRequestedAt,
		},
		Status: store.PodStatus{ContainerStatuses: []store.ContainerStatus{{State: map[string]struct{}{state: {}}}}},
	}
}
