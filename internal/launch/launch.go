// Package launch reads a Medios offer and turns a TaskPlan into a Medios
// TaskInfo.
package launch

import (
	"fmt"
	"sort"
	"strings"

	"github.com/medios-lab/medios-sdk-go/medios"

	"github.com/medios-lab/ray-medios/internal/tasks"
)

// OfferAttributes returns the offer's text attributes, which are the node's
// attributes.
func OfferAttributes(o medios.Offer) map[string]string {
	out := map[string]string{}
	for _, a := range o.Attributes {
		if a.Text != nil {
			out[a.Name] = a.Text.Value
		}
	}
	return out
}

// OfferMatches reports whether the node carries every attribute in placement
// with that value.
func OfferMatches(o medios.Offer, placement map[string]string) bool {
	have := OfferAttributes(o)
	for name, value := range placement {
		if got, ok := have[name]; !ok || got != value {
			return false
		}
	}
	return true
}

// OfferScalar sums the offer's scalar resources of the given name.
func OfferScalar(o medios.Offer, name string) float64 {
	var total float64
	for _, r := range o.Resources {
		if r.Name == name && r.Scalar != nil {
			total += r.Scalar.Value
		}
	}
	return total
}

// OfferPorts lists every port the offer holds, ascending.
func OfferPorts(o medios.Offer) []int {
	var ports []int
	for _, r := range o.Resources {
		if r.Name == "ports" && r.Ranges != nil {
			for _, p := range r.Ranges.Ports() {
				ports = append(ports, int(p))
			}
		}
	}
	sort.Ints(ports)
	return ports
}

// Size is the share of an offer one task takes.
type Size struct {
	CPUs     float64
	MemoryMB float64
	GPUs     float64
}

// OfferHasStores reports whether the node declares every store named in
// stores, which maps a Medios store name to a container path.
func OfferHasStores(o medios.Offer, stores map[string]string) bool {
	for name := range stores {
		if _, ok := medios.FindStore(o, name); !ok {
			return false
		}
	}
	return true
}

// TaskInfo builds the TaskInfo that runs plan on the node that made offer.
// Each store in stores, a map of Medios store name to container path, is
// mounted in the given mode.
//
// Scalar resources are carved from the offer's own resources, so each keeps
// the identity the controller compares by equality (a gpu's class, a
// reservation). The offer must hold the whole of each quantity in one
// resource.
func TaskInfo(plan tasks.TaskPlan, offer medios.Offer, taskID, principal, image string, size Size, stores map[string]string, mode medios.StoreMode) (medios.TaskInfo, error) {
	var container *medios.ContainerInfo
	switch plan.Placement["containerizer"] {
	case string(tasks.Medios):
		// A medios node takes the image in ContainerInfo.medios; sending a
		// DOCKER container there ends in TASK_ERROR.
		container = &medios.ContainerInfo{Type: medios.ContainerMedios, Medios: &medios.MediosInfo{
			Image: &medios.Image{Type: medios.ImageDocker, Docker: medios.ImageDockerInfo{Name: image}}}}
	case string(tasks.DockerShell):
		container = &medios.ContainerInfo{Type: medios.ContainerDocker, Docker: &medios.DockerInfo{Image: image}}
		if plan.HostNetwork {
			container.Docker.Network = medios.NetworkHost
		}
	default:
		return medios.TaskInfo{}, fmt.Errorf("the plan's placement names no known containerizer: %q", plan.Placement["containerizer"])
	}
	if l := plan.LinuxInfo; l != nil {
		shm := uint32(l.ShmSizeMB)
		container.LinuxInfo = &medios.LinuxInfo{IpcMode: l.IPCMode, ShmSize: &shm}
	}
	// The SDK leaves this check to the caller.
	if err := medios.ValidateContainerInfo(container); err != nil {
		return medios.TaskInfo{}, err
	}

	var resources []medios.Resource
	for _, want := range []struct {
		name  string
		value float64
	}{{"cpus", size.CPUs}, {"mem", size.MemoryMB}, {"gpus", size.GPUs}} {
		if want.name == "gpus" && want.value == 0 {
			continue
		}
		carved, err := carve(offer, want.name, want.value)
		if err != nil {
			return medios.TaskInfo{}, err
		}
		resources = append(resources, carved)
	}
	if len(plan.Ports) > 0 {
		ports := make([]uint64, len(plan.Ports))
		sorted := append([]int(nil), plan.Ports...)
		sort.Ints(sorted)
		for i, p := range sorted {
			ports[i] = uint64(p)
		}
		resources = append(resources, medios.NewPortsResource(ports...))
	}

	// CommandInfo carries one shell string: the plan's shell commands, one
	// per line, then the argument list quoted into the last line.
	value := tasks.ShellJoin(plan.Argv)
	if len(plan.ShellBefore) > 0 {
		lines := append([]string{"set -e"}, plan.ShellBefore...)
		value = strings.Join(append(lines, "exec "+value), "\n")
	}
	variables := make([]medios.EnvVar, len(plan.Env))
	for i, e := range plan.Env {
		variables[i] = medios.EnvVar{Name: e.Name, Value: e.Value}
	}
	task, err := medios.NewTaskInfo(principal, taskID, medios.ID{Value: taskID}, offer.NodeID, resources,
		medios.CommandInfo{Shell: true, Value: value, Environment: &medios.Environment{Variables: variables}})
	if err != nil {
		return medios.TaskInfo{}, err
	}
	task.Container = container
	names := make([]string, 0, len(stores))
	for name := range stores {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if task, err = task.WithStore(offer, name, stores[name], mode); err != nil {
			return medios.TaskInfo{}, err
		}
	}
	return task, nil
}

// Remaining returns the offer as it stands after a task took taken from it,
// so a second task can be fitted to the same offer and sent in the same
// ACCEPT. A scalar is subtracted from the resource entry carve would pick,
// the first of that name holding at least the amount. Taken ports leave the
// offer's port ranges. A store's shared resource is left whole. The offer
// passed in is not changed.
func Remaining(o medios.Offer, taken []medios.Resource) medios.Offer {
	out := o
	out.Resources = append([]medios.Resource(nil), o.Resources...)
	gone := map[uint64]bool{}
	for _, t := range taken {
		if t.Ranges != nil {
			for _, p := range t.Ranges.Ports() {
				gone[p] = true
			}
		}
		// A store's resource is shared: every task that mounts the store lists
		// the offer's whole store resource, so it stays in the offer.
		if t.Scalar == nil || strings.HasPrefix(t.Name, medios.StoreResourcePrefix) {
			continue
		}
		for i, r := range out.Resources {
			if r.Name == t.Name && r.Scalar != nil && r.Scalar.Value >= t.Scalar.Value {
				out.Resources[i].Scalar = &medios.Scalar{Value: r.Scalar.Value - t.Scalar.Value}
				break
			}
		}
	}
	for i, r := range out.Resources {
		if r.Name != "ports" || r.Ranges == nil || len(gone) == 0 {
			continue
		}
		left := &medios.Ranges{}
		for _, p := range r.Ranges.Ports() {
			if gone[p] {
				continue
			}
			if n := len(left.Range); n > 0 && left.Range[n-1].End+1 == p {
				left.Range[n-1].End = p
			} else {
				left.Range = append(left.Range, medios.Range{Begin: p, End: p})
			}
		}
		out.Resources[i].Ranges = left
	}
	return out
}

func carve(o medios.Offer, name string, want float64) (medios.Resource, error) {
	for _, r := range o.Resources {
		if r.Name != name {
			continue
		}
		if carved, ok := r.Carve(want); ok {
			return carved, nil
		}
	}
	return medios.Resource{}, fmt.Errorf("offer %s holds no single %s resource of at least %g", o.ID.Value, name, want)
}
