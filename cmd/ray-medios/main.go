// Command ray-medios is the Medios framework for one Ray cluster.
//
// It registers with the Medios controller under the framework name `ray`,
// launches one Medios task per Ray node the RayCluster record asks for, and
// tears the framework down when it stops, which ends the cluster's tasks.
// With the endpoint flags it also serves the HTTPS endpoint that the stock
// autoscaler in the head's task reads and patches the record through.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/medios-lab/medios-sdk-go/medios"
	"github.com/medios-lab/medios-sdk-go/scheduler"

	"github.com/medios-lab/ray-medios/internal/endpoint"
	"github.com/medios-lab/ray-medios/internal/framework"
	"github.com/medios-lab/ray-medios/internal/schedule"
	"github.com/medios-lab/ray-medios/internal/store"
	"github.com/medios-lab/ray-medios/internal/tasks"
)

// Build metadata, overridden at link time via -ldflags.
var (
	version = "dev"
	commit  = ""
	date    = ""
)

func versionString() string {
	v, c, d := version, commit, date
	if info, ok := debug.ReadBuildInfo(); ok {
		if v == "dev" && info.Main.Version != "" && info.Main.Version != "(devel)" {
			v = info.Main.Version
		}
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				if c == "" && len(s.Value) >= 7 {
					c = s.Value[:7]
				}
			case "vcs.time":
				if d == "" {
					d = s.Value
				}
			}
		}
	}
	parts := []string{"ray-medios", v}
	if c != "" {
		parts = append(parts, "commit="+c)
	}
	if d != "" {
		parts = append(parts, "built="+d)
	}
	return strings.Join(parts, " ")
}

// frameworkName is the name this framework registers with the Medios
// controller.
const frameworkName = "ray"

// teardownTimeout bounds the TEARDOWN call made on the way out. Type:
// default, provisional; no measurement behind it.
const teardownTimeout = 10 * time.Second

type attributes map[string]string

func (a attributes) String() string { return fmt.Sprint(map[string]string(a)) }
func (a attributes) Set(v string) error {
	name, value, ok := strings.Cut(v, "=")
	if !ok || name == "" {
		return fmt.Errorf("want name=value, got %q", v)
	}
	a[name] = value
	return nil
}

type options struct {
	showVersion                                                                  bool
	controller, user, recordFile, principal, image, containerizer, authTokenFile string
	nodeAttributes, stores                                                       attributes
	workerPorts                                                                  int
	headStoresReadOnly                                                           bool
	// Endpoint: given together or not at all.
	listen, endpointHost                        string
	endpointPort                                int
	serverCert, serverKey, clientCA             string
	autoscalerCA, autoscalerCert, autoscalerKey string
}

func parse(args []string) (options, error) {
	o := options{nodeAttributes: attributes{}, stores: attributes{}}
	fs := flag.NewFlagSet("ray-medios", flag.ContinueOnError)
	fs.BoolVar(&o.showVersion, "version", false, "print build version and exit")
	fs.StringVar(&o.controller, "controller", "", "Medios controller URL (required)")
	fs.StringVar(&o.user, "user", "", "OS user the tasks run as on the Medios nodes (required)")
	fs.StringVar(&o.recordFile, "record", "", "file holding the RayCluster record as JSON (required)")
	fs.StringVar(&o.principal, "principal", "", "who the cluster's tasks run on behalf of: operator:<name>, team:<name> or run:<id> (required)")
	fs.StringVar(&o.image, "image", "", "image of every Ray node's task; it must have ray on PATH (required)")
	fs.StringVar(&o.containerizer, "containerizer", "", "containerizer of the nodes to place tasks on: medios or docker_shell (required)")
	fs.BoolVar(&o.headStoresReadOnly, "head-stores-read-only", false, "mount each -store read-only in the head's task, so the head takes no writer unit of the store")
	fs.Var(o.stores, "store", "name=path: mount the Medios store read-write at this container path in every Ray node's task; repeat for several (optional)")
	fs.Var(o.nodeAttributes, "node-attribute", "name=value a node must carry besides its containerizer; repeat for several (at least one required)")
	fs.IntVar(&o.workerPorts, "worker-ports", 0, "ports reserved per Ray node for Ray worker processes and attached drivers (required)")
	fs.StringVar(&o.authTokenFile, "auth-token-file", "", "file holding the cluster's Ray auth token (required)")
	fs.StringVar(&o.listen, "endpoint-listen", "", "host:port the HTTPS endpoint binds")
	fs.StringVar(&o.endpointHost, "endpoint-host", "", "host the autoscaler connects to; the server certificate must name it")
	fs.IntVar(&o.endpointPort, "endpoint-port", 0, "port the autoscaler connects to")
	fs.StringVar(&o.serverCert, "tls-server-cert", "", "PEM file: the endpoint's certificate")
	fs.StringVar(&o.serverKey, "tls-server-key", "", "PEM file: the endpoint's key")
	fs.StringVar(&o.clientCA, "tls-client-ca", "", "PEM file: CA the endpoint verifies client certificates against")
	fs.StringVar(&o.autoscalerCA, "autoscaler-ca", "", "PEM file: CA the autoscaler verifies the endpoint against")
	fs.StringVar(&o.autoscalerCert, "autoscaler-cert", "", "PEM file: certificate the autoscaler presents")
	fs.StringVar(&o.autoscalerKey, "autoscaler-key", "", "PEM file: key of the autoscaler's certificate")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if o.showVersion {
		return o, nil
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	for _, f := range []struct{ name, value string }{
		{"controller", o.controller}, {"user", o.user}, {"record", o.recordFile}, {"principal", o.principal},
		{"image", o.image}, {"containerizer", o.containerizer}, {"auth-token-file", o.authTokenFile},
	} {
		if f.value == "" {
			return o, fmt.Errorf("-%s is required", f.name)
		}
	}
	if c := tasks.Containerizer(o.containerizer); c != tasks.Medios && c != tasks.DockerShell {
		return o, fmt.Errorf("-containerizer is %s or %s, got %q", tasks.Medios, tasks.DockerShell, o.containerizer)
	}
	if o.workerPorts < 1 {
		return o, errors.New("-worker-ports is required and must be at least 1")
	}
	if len(o.nodeAttributes) == 0 {
		return o, errors.New("at least one -node-attribute is required")
	}
	set := 0
	endpointFlags := []struct{ name, value string }{
		{"endpoint-listen", o.listen}, {"endpoint-host", o.endpointHost}, {"endpoint-port", strconv.Itoa(o.endpointPort)},
		{"tls-server-cert", o.serverCert}, {"tls-server-key", o.serverKey}, {"tls-client-ca", o.clientCA},
		{"autoscaler-ca", o.autoscalerCA}, {"autoscaler-cert", o.autoscalerCert}, {"autoscaler-key", o.autoscalerKey},
	}
	var missing []string
	for _, f := range endpointFlags {
		if f.value != "" && f.value != "0" {
			set++
		} else {
			missing = append(missing, "-"+f.name)
		}
	}
	if set != 0 && set != len(endpointFlags) {
		return o, fmt.Errorf("the endpoint flags are given together or not at all; missing: %s", strings.Join(missing, " "))
	}
	return o, nil
}

func readText(flagName, path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("-%s: %w", flagName, err)
	}
	if len(strings.TrimSpace(string(b))) == 0 {
		return "", fmt.Errorf("-%s: %s is empty", flagName, path)
	}
	return string(b), nil
}

func run(o options) (reason string, err error) {
	rawRecord, err := os.ReadFile(o.recordFile)
	if err != nil {
		return "", fmt.Errorf("-record: %w", err)
	}
	s := store.NewMemory()
	stored, err := s.PutCluster(rawRecord)
	if err != nil {
		return "", err
	}
	record, err := schedule.ParseRecord(stored)
	if err != nil {
		return "", err
	}
	token, err := readText("auth-token-file", o.authTokenFile)
	if err != nil {
		return "", err
	}
	config := framework.Config{
		Namespace: record.Metadata.Namespace, Name: record.Metadata.Name,
		Principal: o.principal, Image: o.image, Containerizer: tasks.Containerizer(o.containerizer),
		NodeAttributes: o.nodeAttributes, WorkerPorts: o.workerPorts, AuthToken: strings.TrimSpace(token), Stores: o.stores, HeadStoresReadOnly: o.headStoresReadOnly,
	}

	var server *http.Server
	var listener net.Listener
	if o.listen != "" {
		ep := &tasks.AutoscalerEndpoint{Host: o.endpointHost, Port: o.endpointPort, Namespace: config.Namespace}
		for _, f := range []struct {
			flagName, path string
			into           *string
		}{
			{"autoscaler-ca", o.autoscalerCA, &ep.CACertPEM},
			{"autoscaler-cert", o.autoscalerCert, &ep.ClientCertPEM},
			{"autoscaler-key", o.autoscalerKey, &ep.ClientKeyPEM},
		} {
			if *f.into, err = readText(f.flagName, f.path); err != nil {
				return "", err
			}
		}
		config.AutoscalerEndpoint = ep
		server, listener, err = endpoint.Listen(s, o.listen, endpoint.TLSFiles{
			ServerCert: o.serverCert, ServerKey: o.serverKey, ClientCA: o.clientCA})
		if err != nil {
			return "", fmt.Errorf("endpoint: %w", err)
		}
		defer server.Close()
	}

	client, err := scheduler.NewClient(o.controller, frameworkName, scheduler.WithUser(o.user))
	if err != nil {
		return "", err
	}
	fw, err := framework.New(client, s, config)
	if err != nil {
		return "", err
	}
	if server != nil {
		go func() {
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("endpoint stopped: %v", err)
			}
		}()
		log.Printf("endpoint listening on %s; the autoscaler is told %s:%d", listener.Addr(), o.endpointHost, o.endpointPort)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	// The subscription's context is cancelled only after TEARDOWN has been
	// sent: the controller removes a framework whose stream closed, and
	// refuses a TEARDOWN from a framework that is not subscribed. Deferred
	// calls run last-in first-out, so the teardown below runs before cancel.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, errs, err := client.Subscribe(ctx)
	if err != nil {
		return "", fmt.Errorf("subscribe: %w", err)
	}
	defer func() {
		tctx, tcancel := context.WithTimeout(context.Background(), teardownTimeout)
		defer tcancel()
		if terr := client.Teardown(tctx); terr != nil {
			log.Printf("teardown failed: %v", terr)
		} else {
			log.Print("torn down")
		}
	}()

	for {
		select {
		case sig := <-signals:
			return fmt.Sprintf("signal %s received", sig), nil
		case serr, ok := <-errs:
			if ok && serr != nil {
				return "", fmt.Errorf("event stream: %w", serr)
			}
			return "", errors.New("the controller closed the event stream")
		case ev, ok := <-events:
			if !ok {
				return "", errors.New("the controller closed the event stream")
			}
			if ev.Type == medios.EventSubscribed {
				log.Printf("subscribed as framework %s", client.FrameworkID())
			}
			if err := fw.Handle(ctx, ev); err != nil {
				return "", err
			}
			if fw.Stopped != "" {
				return "", errors.New(fw.Stopped)
			}
		}
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	o, err := parse(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "ray-medios:", err)
		os.Exit(2)
	}
	if o.showVersion {
		fmt.Println(versionString())
		return
	}
	reason, err := run(o)
	if err != nil {
		log.Printf("stopped: %v", err)
		os.Exit(1)
	}
	log.Printf("stopped: %s", reason)
}
