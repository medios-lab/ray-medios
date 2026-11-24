// Package framework is the Medios framework for one Ray cluster.
//
// Framework.Handle takes one event from the Medios scheduler stream. It
// launches a Medios task for each Ray node the RayCluster record asks for,
// kills the task of each worker the record names for removal, and keeps one
// pod record per task in the store for the stock autoscaler to read. The
// caller owns the stream: it subscribes, passes every event to Handle, and
// tears down.
//
// Not handled: a lost subscription, and replacing a Ray node whose task
// ended. When a task ends that the framework did not kill, Stopped is set and
// the caller is expected to tear down.
package framework

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/medios-lab/medios-sdk-go/medios"

	"github.com/medios-lab/ray-medios/internal/launch"
	"github.com/medios-lab/ray-medios/internal/schedule"
	"github.com/medios-lab/ray-medios/internal/store"
	"github.com/medios-lab/ray-medios/internal/tasks"
)

// DeclineRefuseSeconds is how long the controller withholds a declined offer
// from this framework. Type: default. Reason: the value in the Medios SDK's
// reference framework, which takes it from Marathon; not measured on this
// fleet.
const DeclineRefuseSeconds = 120.0

// MinReviveInterval is the shortest time between two REVIVE calls. Type:
// default, same source.
const MinReviveInterval = 5 * time.Second

// Config is what the framework needs besides the RayCluster record. Every
// field except AutoscalerEndpoint is required; none is derived from another.
type Config struct {
	Namespace string
	Name      string
	// Who the cluster's tasks run on behalf of: operator:<name>, team:<name>
	// or run:<id>. Stated by whoever creates the cluster.
	Principal      string
	Image          string
	Containerizer  tasks.Containerizer
	NodeAttributes map[string]string
	// Ports reserved per Ray node for worker processes and attached drivers.
	WorkerPorts int
	AuthToken   string
	// Optional. Medios store name to container path; each is mounted
	// read-write in every Ray node's task, and a task launches only on a
	// node that declares every one.
	Stores map[string]string
	// The head's task mounts each store read-only, taking a reader unit
	// instead of one of the store's writer units.
	HeadStoresReadOnly bool
	// Required when the RayCluster record sets spec.enableInTreeAutoscaling:
	// the head's task then runs the stock autoscaler against this endpoint.
	AutoscalerEndpoint *tasks.AutoscalerEndpoint
}

// Client is the part of the Medios scheduler client the framework calls.
type Client interface {
	Accept(ctx context.Context, offerIDs []medios.ID, tasks []medios.TaskInfo) error
	Decline(ctx context.Context, offerIDs []medios.ID, refuseSeconds float64) error
	Suppress(ctx context.Context) error
	Revive(ctx context.Context) error
	Kill(ctx context.Context, taskID, nodeID string) error
	Acknowledge(ctx context.Context, taskID, nodeID, uuid string) error
}

// Framework holds the state of one Ray cluster's tasks. It is not safe for
// concurrent use; one goroutine calls Handle.
type Framework struct {
	client Client
	store  store.Store
	config Config

	// NewSuffix returns the random part of an instance ID.
	NewSuffix func() string
	// Now returns the current time.
	Now func() time.Time

	// Launched holds the Ray nodes that have a task, by instance ID.
	Launched map[string]schedule.Launched
	// NodeIDs holds the Medios node ID of each task, by instance ID.
	NodeIDs map[string]string
	// Stopped is the reason the framework stopped acting, or empty.
	Stopped string
	// DashboardURL is set when the head is launched.
	DashboardURL string

	order       []string // instance IDs in launch order
	autoscaling bool
	suppressed  bool
	lastRevive  time.Time
}

// New checks the config against the stored record.
func New(client Client, s store.Store, config Config) (*Framework, error) {
	if err := medios.ValidatePrincipal(config.Principal); err != nil {
		return nil, err
	}
	f := &Framework{
		client: client, store: s, config: config,
		NewSuffix: func() string {
			b := make([]byte, 3)
			_, _ = rand.Read(b)
			return hex.EncodeToString(b)
		},
		Now:      time.Now,
		Launched: map[string]schedule.Launched{},
		NodeIDs:  map[string]string{},
	}
	record, err := f.record()
	if err != nil {
		return nil, err
	}
	// KubeRay adds the autoscaler container to the head pod when the record
	// sets this field.
	f.autoscaling = record.Spec.EnableInTreeAutoscaling
	if f.autoscaling && config.AutoscalerEndpoint == nil {
		return nil, fmt.Errorf("the record sets spec.enableInTreeAutoscaling and the config has no autoscaler endpoint")
	}
	return f, nil
}

func (f *Framework) record() (schedule.Record, error) {
	raw, err := f.store.GetCluster(f.config.Namespace, f.config.Name)
	if err != nil {
		return schedule.Record{}, err
	}
	return schedule.ParseRecord(raw)
}

func (f *Framework) launchedList() []schedule.Launched {
	out := make([]schedule.Launched, 0, len(f.order))
	for _, id := range f.order {
		if n, ok := f.Launched[id]; ok {
			out = append(out, n)
		}
	}
	return out
}

func (f *Framework) decision() (schedule.Decision, error) {
	record, err := f.record()
	if err != nil {
		return schedule.Decision{}, err
	}
	return schedule.Decide(record, f.launchedList())
}

// Handle acts on one scheduler event. An error means the framework cannot
// continue; the caller tears down.
func (f *Framework) Handle(ctx context.Context, ev medios.Event) error {
	switch {
	case ev.Type == medios.EventOffers && ev.Offers != nil:
		if err := f.onOffers(ctx, ev.Offers.Offers); err != nil {
			return err
		}
	case ev.Type == medios.EventUpdate && ev.Update != nil:
		if err := f.onUpdate(ctx, ev.Update.Status); err != nil {
			return err
		}
	case ev.Type == medios.EventError && ev.Error != nil:
		f.Stopped = "controller error: " + ev.Error.Message
		return nil
	}
	// Every event, heartbeats included, is a chance to act on a record the
	// autoscaler changed through the endpoint since the last event.
	if err := f.killRemoved(ctx); err != nil {
		return err
	}
	return f.setOfferFlow(ctx)
}

func (f *Framework) onOffers(ctx context.Context, offers []medios.Offer) error {
	cfg := f.config
	log.Printf("%d offers received", len(offers))
	var pending []schedule.Wanted
	if f.Stopped == "" {
		d, err := f.decision()
		if err != nil {
			return err
		}
		pending = d.Launch
	}
	var head *schedule.Launched
	for _, n := range f.launchedList() {
		if n.IsHead {
			head = &n
			break
		}
	}
	cluster := schedule.Cluster{
		Name: cfg.Name, Containerizer: cfg.Containerizer, NodeAttributes: cfg.NodeAttributes,
		AuthToken: cfg.AuthToken, WorkerPorts: cfg.WorkerPorts,
	}
	if f.autoscaling {
		cluster.Autoscaler = cfg.AutoscalerEndpoint
	}
	for _, offer := range offers {
		// Every pending Ray node the offer can hold goes into one ACCEPT. Each
		// is fitted to what the earlier ones left of the offer, so no two
		// tasks take the same ports or the same CPUs and memory.
		remaining := offer
		var infos []medios.TaskInfo
		var placed []schedule.Launched
		var dashboards []int // per placed node; 0 for a worker
		var rest []schedule.Wanted
		for _, wanted := range pending {
			if !launch.OfferHasStores(offer, cfg.Stores) {
				rest = append(rest, wanted)
				continue
			}
			instanceID := fmt.Sprintf("%s-%s-%s", cfg.Name, wanted.Group, f.NewSuffix())
			placement, err := schedule.Place(wanted, remaining, cluster, instanceID, head)
			if err != nil {
				f.decline(ctx, offer)
				return err
			}
			if placement == nil {
				rest = append(rest, wanted)
				continue
			}
			mode := medios.StoreRW
			if wanted.IsHead && cfg.HeadStoresReadOnly {
				mode = medios.StoreRO
			}
			node := placement.Node
			task, err := launch.TaskInfo(placement.Plan, remaining, node.InstanceID, cfg.Principal, cfg.Image, placement.Size, cfg.Stores, mode)
			if err != nil {
				f.decline(ctx, offer)
				return err
			}
			remaining = launch.Remaining(remaining, task.Resources)
			infos = append(infos, task)
			n := schedule.Launched{InstanceID: node.InstanceID, Group: wanted.Group, IsHead: wanted.IsHead, Address: node.Address}
			dashboard := 0
			if node.Head != nil {
				n.GCSPort, dashboard = node.Head.GCS, node.Head.Dashboard
			}
			placed, dashboards = append(placed, n), append(dashboards, dashboard)
		}
		if len(infos) == 0 {
			f.decline(ctx, offer)
			continue
		}
		// The controller returns what the tasks leave of the offer.
		if err := f.client.Accept(ctx, []medios.ID{offer.ID}, infos); err != nil {
			log.Printf("accept of offer %s failed: %v", offer.ID.Value, err)
			f.decline(ctx, offer)
			continue
		}
		pending = rest
		for i, n := range placed {
			if dashboards[i] != 0 {
				f.DashboardURL = fmt.Sprintf("http://%s:%d", n.Address, dashboards[i])
				log.Printf("dashboard %s", f.DashboardURL)
			}
			f.Launched[n.InstanceID] = n
			f.NodeIDs[n.InstanceID] = offer.NodeID.Value
			f.order = append(f.order, n.InstanceID)
			log.Printf("launched %s on %s", n.InstanceID, n.Address)
			if err := f.writePod(n.InstanceID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *Framework) decline(ctx context.Context, offer medios.Offer) {
	if err := f.client.Decline(ctx, []medios.ID{offer.ID}, DeclineRefuseSeconds); err != nil {
		log.Printf("decline of offer %s failed: %v", offer.ID.Value, err)
	}
}

func (f *Framework) onUpdate(ctx context.Context, st medios.TaskStatus) error {
	if st.UUID != "" {
		// An update with a UUID is acknowledged or the node sends it again.
		if err := f.client.Acknowledge(ctx, st.TaskID.Value, st.NodeID.Value, st.UUID); err != nil {
			return fmt.Errorf("acknowledge %s: %w", st.TaskID.Value, err)
		}
	}
	id := st.TaskID.Value
	node, ok := f.Launched[id]
	if !ok {
		return nil
	}
	switch {
	case st.State.IsTerminal():
		delete(f.Launched, id)
		delete(f.NodeIDs, id)
		if err := f.store.RemovePod(f.config.Namespace, id); err != nil {
			return err
		}
		log.Printf("task %s ended: %s %s", id, st.State, st.Message)
		if node.KillRequestedAt == "" {
			kind := "worker"
			if node.IsHead {
				kind = "head"
			}
			f.Stopped = strings.TrimSpace(fmt.Sprintf("%s task %s ended: %s %s", kind, id, st.State, st.Message))
		}
	case st.State == medios.TaskRunning && !node.Running:
		node.Running = true
		f.Launched[id] = node
		log.Printf("task %s is running", id)
		return f.writePod(id)
	}
	return nil
}

func (f *Framework) killRemoved(ctx context.Context) error {
	if f.Stopped != "" {
		return nil
	}
	d, err := f.decision()
	if err != nil {
		return err
	}
	for _, id := range d.Kill {
		if err := f.client.Kill(ctx, id, f.NodeIDs[id]); err != nil {
			return fmt.Errorf("kill %s: %w", id, err)
		}
		node := f.Launched[id]
		node.KillRequestedAt = f.Now().UTC().Format("2006-01-02T15:04:05Z")
		f.Launched[id] = node
		if err := f.writePod(id); err != nil {
			return err
		}
		log.Printf("kill requested for %s", id)
	}
	return nil
}

func (f *Framework) setOfferFlow(ctx context.Context) error {
	wantsOffers := false
	if f.Stopped == "" {
		d, err := f.decision()
		if err != nil {
			return err
		}
		wantsOffers = len(d.Launch) > 0
	}
	switch {
	case !wantsOffers && !f.suppressed:
		if err := f.client.Suppress(ctx); err != nil {
			return fmt.Errorf("suppress: %w", err)
		}
		f.suppressed = true
		log.Print("offers suppressed: nothing to launch")
	case wantsOffers && f.suppressed:
		now := f.Now()
		if f.lastRevive.IsZero() || now.Sub(f.lastRevive) >= MinReviveInterval {
			if err := f.client.Revive(ctx); err != nil {
				return fmt.Errorf("revive: %w", err)
			}
			f.suppressed, f.lastRevive = false, now
			log.Print("offers revived")
		}
	}
	return nil
}

func (f *Framework) writePod(id string) error {
	return f.store.PutPod(schedule.PodRecord(f.config.Namespace, f.config.Name, f.Launched[id]))
}
