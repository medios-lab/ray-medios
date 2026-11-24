package main

import (
	"strings"
	"testing"
)

var required = []string{"-controller", "http://c:5150", "-user", "u", "-record", "r.json", "-principal", "team:t",
	"-image", "img", "-containerizer", "medios", "-node-attribute", "gpu=gb10", "-worker-ports", "4", "-auth-token-file", "tok"}

func TestRequiredFlags(t *testing.T) {
	if _, err := parse(required); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(required); i += 2 {
		args := append(append([]string{}, required[:i]...), required[i+2:]...)
		if _, err := parse(args); err == nil {
			t.Errorf("accepted without %s", required[i])
		}
	}
}

func TestEndpointFlagsAreGivenTogether(t *testing.T) {
	_, err := parse(append(append([]string{}, required...), "-endpoint-listen", "0.0.0.0:8443", "-endpoint-host", "h"))
	if err == nil || !strings.Contains(err.Error(), "-endpoint-port") || !strings.Contains(err.Error(), "-autoscaler-key") {
		t.Error(err)
	}
}

func TestUnknownContainerizerIsRefused(t *testing.T) {
	args := append([]string{}, required...)
	args[11] = "podman"
	if _, err := parse(args); err == nil || !strings.Contains(err.Error(), "-containerizer") {
		t.Error(err)
	}
}

func TestVersionFlagSkipsRequiredFlags(t *testing.T) {
	o, err := parse([]string{"-version"})
	if err != nil {
		t.Fatal(err)
	}
	if !o.showVersion {
		t.Fatal("expected showVersion to be true")
	}
	if !strings.HasPrefix(versionString(), "ray-medios ") {
		t.Fatalf("unexpected versionString: %q", versionString())
	}
}
