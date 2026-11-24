// Package store holds the desired-state record the autoscaler edits and the
// pod records it reads.
//
// The autoscaler writes only the RayCluster record. The framework's scheduler
// writes the pod records. Both go through Store, so where the state is kept
// can change without touching the endpoint.
package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	jsonpatch "github.com/evanphx/json-patch/v5"
)

// ClusterLabel is the label the stock autoscaler selects pods by (Ray,
// cloud_provider.py:604).
const ClusterLabel = "ray.io/cluster"

// ErrClusterNotFound says the store holds no such RayCluster record.
var ErrClusterNotFound = errors.New("not found")

// ErrPatchRejected says the patch is well-formed JSON but cannot be applied
// to the record.
var ErrPatchRejected = errors.New("patch rejected")

// Pod is the Pod-format record of one Ray node. It holds the fields the stock
// provider reads without a default (Ray, cloud_provider.py:647-688).
type Pod struct {
	APIVersion string      `json:"apiVersion"`
	Kind       string      `json:"kind"`
	Metadata   PodMetadata `json:"metadata"`
	Status     PodStatus   `json:"status"`
}

type PodMetadata struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	Labels            map[string]string `json:"labels"`
	DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
}

type PodStatus struct {
	ContainerStatuses []ContainerStatus `json:"containerStatuses"`
}

// ContainerStatus has one state key: "running" or "waiting".
type ContainerStatus struct {
	State map[string]struct{} `json:"state"`
}

// Store is what the endpoint and the framework need from the state.
type Store interface {
	// GetCluster returns the record as JSON, or ErrClusterNotFound.
	GetCluster(namespace, name string) ([]byte, error)
	// PatchCluster applies a JSON Patch document atomically and returns the
	// new record.
	PatchCluster(namespace, name string, patch []byte) ([]byte, error)
	// ListPods returns the cluster's pod records and the list's version.
	ListPods(namespace, cluster string) ([]Pod, string)
	PutPod(pod Pod) error
	RemovePod(namespace, name string) error
}

type key struct{ namespace, name string }

// Memory is an in-process store. State is lost when the process exits, so a
// framework restart forgets every cluster.
type Memory struct {
	mu       sync.Mutex
	clusters map[key][]byte
	pods     map[key]Pod
	// One counter for the whole store, incremented on every write. It is the
	// source of metadata.resourceVersion, which the autoscaler sends back in
	// a JSON Patch `test` to detect a concurrent change.
	version int
}

func NewMemory() *Memory {
	return &Memory{clusters: map[key][]byte{}, pods: map[key]Pod{}}
}

func (m *Memory) nextVersion() string {
	m.version++
	return strconv.Itoa(m.version)
}

func decode(raw []byte) (map[string]any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber() // keeps every number as written
	var out map[string]any
	if err := d.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// PutCluster stores a RayCluster record and returns it as stored.
func (m *Memory) PutCluster(raw []byte) ([]byte, error) {
	record, err := decode(raw)
	if err != nil {
		return nil, fmt.Errorf("RayCluster record is not a JSON object: %w", err)
	}
	metadata, _ := record["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	namespace, _ := metadata["namespace"].(string)
	// The stock config producer reads both without a default; a record that
	// lacks one raises KeyError inside the autoscaler
	// (docs/endpoint-contract.md).
	if name == "" {
		return nil, fmt.Errorf("RayCluster record has no metadata.name")
	}
	if namespace == "" {
		return nil, fmt.Errorf("RayCluster record has no metadata.namespace")
	}
	spec, ok := record["spec"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("RayCluster record has no spec")
	}
	if _, ok := spec["idleTerminationOptions"]; ok {
		// The stock provider logs and continues when its suspend or delete
		// request is refused (Ray, cloud_provider.py:788-876), so refusing at
		// the endpoint would leave the option silently inert. Refuse the
		// record instead. Supporting the option is an open decision.
		return nil, fmt.Errorf("spec.idleTerminationOptions is not supported; the cluster would never be suspended or deleted")
	}
	// The stock provider removes workers with a JSON Patch `replace` on
	// /spec/workerGroupSpecs/<i>/scaleStrategy (Ray, node_provider.py:170),
	// and RFC 6902 `replace` fails when the member is absent. Ray's own
	// sample RayCluster has no scaleStrategy, so every group gets an empty
	// one here. Type: hypothesis. How a Kubernetes API server treats that
	// patch on a record without the member has not been checked; see
	// docs/endpoint-contract.md, open items.
	if groups, ok := spec["workerGroupSpecs"].([]any); ok {
		for _, g := range groups {
			if group, ok := g.(map[string]any); ok {
				if _, has := group["scaleStrategy"]; !has {
					group["scaleStrategy"] = map[string]any{}
				}
			}
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	metadata["resourceVersion"] = m.nextVersion()
	stored, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	m.clusters[key{namespace, name}] = stored
	return bytes.Clone(stored), nil
}

func (m *Memory) GetCluster(namespace, name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.clusters[key{namespace, name}]
	if !ok {
		return nil, fmt.Errorf("rayclusters/%s in namespace %s: %w", name, namespace, ErrClusterNotFound)
	}
	return bytes.Clone(record), nil
}

func (m *Memory) PatchCluster(namespace, name string, patch []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.clusters[key{namespace, name}]
	if !ok {
		return nil, fmt.Errorf("rayclusters/%s in namespace %s: %w", name, namespace, ErrClusterNotFound)
	}
	operations, err := jsonpatch.DecodePatch(patch)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPatchRejected, err)
	}
	applied, err := operations.Apply(record)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPatchRejected, err)
	}
	patched, err := decode(applied)
	if err != nil {
		return nil, fmt.Errorf("%w: the patched record is not a JSON object", ErrPatchRejected)
	}
	metadata, _ := patched["metadata"].(map[string]any)
	if metadata == nil || metadata["name"] != name || metadata["namespace"] != namespace {
		return nil, fmt.Errorf("%w: a patch may not change metadata.name or namespace", ErrPatchRejected)
	}
	if _, ok := patched["spec"].(map[string]any); !ok {
		return nil, fmt.Errorf("%w: a patch may not remove spec", ErrPatchRejected)
	}
	metadata["resourceVersion"] = m.nextVersion()
	stored, err := json.Marshal(patched)
	if err != nil {
		return nil, err
	}
	m.clusters[key{namespace, name}] = stored
	return bytes.Clone(stored), nil
}

func (m *Memory) PutPod(pod Pod) error {
	if pod.Metadata.Name == "" {
		return fmt.Errorf("pod record has no metadata.name")
	}
	if pod.Metadata.Namespace == "" {
		return fmt.Errorf("pod record has no metadata.namespace")
	}
	// The stock provider indexes these without a default; a record lacking
	// one raises KeyError inside the autoscaler.
	for _, label := range []string{ClusterLabel, "ray.io/node-type", "ray.io/group"} {
		if _, ok := pod.Metadata.Labels[label]; !ok {
			return fmt.Errorf("pod record has no %s label", label)
		}
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		return fmt.Errorf("pod record has no status.containerStatuses")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextVersion()
	m.pods[key{pod.Metadata.Namespace, pod.Metadata.Name}] = pod
	return nil
}

func (m *Memory) RemovePod(namespace, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.pods[key{namespace, name}]; !ok {
		return fmt.Errorf("pods/%s in namespace %s is not stored", name, namespace)
	}
	m.nextVersion()
	delete(m.pods, key{namespace, name})
	return nil
}

func (m *Memory) ListPods(namespace, cluster string) ([]Pod, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pods := []Pod{}
	for k, pod := range m.pods {
		if k.namespace == namespace && pod.Metadata.Labels[ClusterLabel] == cluster {
			pods = append(pods, pod)
		}
	}
	return pods, strconv.Itoa(m.version)
}
