package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"time"
)

const (
	// healthzPath is the liveness route registerRoutes serves and the
	// healthcheck subcommand probes.
	healthzPath = "/healthz"

	// healthcheckTimeout stays under the image's HEALTHCHECK --timeout of
	// 3s, so a hung collector fails with the probe's own error rather than
	// Docker killing the process first.
	healthcheckTimeout = 2 * time.Second
)

// healthcheckClient never uses a proxy. The probe only ever targets the
// collector in its own container, and an HTTP_PROXY set for forwarding
// would otherwise capture a probe addressed by hostname or pod IP.
var healthcheckClient = &http.Client{Transport: &http.Transport{Proxy: nil}}

// runSubcommand handles a non-empty argument list and returns the process
// exit code. "healthcheck" probes the collector's own /healthz and returns
// 0 or 1; anything else writes a usage line to stderr and returns 2, so a
// typo never starts a second collector.
func runSubcommand(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer) int {
	if len(args) != 1 || args[0] != "healthcheck" {
		fmt.Fprintln(stderr, "usage: yukon-collector [healthcheck]")
		return 2
	}
	if err := healthcheckCommand(ctx, getenv); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

// healthcheckCommand probes /healthz on the address YUKON_COLLECTOR_ADDR
// names, the same address the collector listens on.
func healthcheckCommand(ctx context.Context, getenv func(string) string) error {
	ctx, cancel := context.WithTimeout(ctx, healthcheckTimeout)
	defer cancel()
	target, err := healthzURL(ctx, resolveAddr(getenv("YUKON_COLLECTOR_ADDR")))
	if err != nil {
		return err
	}
	return runHealthcheck(ctx, target)
}

// resolveAddr returns raw (the value of YUKON_COLLECTOR_ADDR), or
// defaultAddr when raw is empty.
func resolveAddr(raw string) string {
	if raw == "" {
		return defaultAddr
	}
	return raw
}

// healthzURL returns the URL the healthcheck subcommand probes for the
// collector listening on addr. An empty or unspecified host, zoned or not,
// is probed on the loopback address, since the probe runs inside the same
// container as the collector. A named port is resolved the way the
// listener resolves it.
func healthzURL(ctx context.Context, addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("healthz url: %w", err)
	}
	portNum, err := net.DefaultResolver.LookupPort(ctx, "tcp", port)
	if err != nil {
		return "", fmt.Errorf("healthz url: %w", err)
	}
	if ip, err := netip.ParseAddr(host); host == "" || (err == nil && ip.WithZone("").Unmap().IsUnspecified()) {
		host = "127.0.0.1"
	}
	u := url.URL{Scheme: "http", Host: net.JoinHostPort(host, strconv.Itoa(portNum)), Path: healthzPath}
	return u.String(), nil
}

// runHealthcheck does one GET of the health route at target and returns
// nil for a 200, or an error naming the status or the transport failure.
func runHealthcheck(ctx context.Context, target string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("healthz: %w", err)
	}
	resp, err := healthcheckClient.Do(req)
	if err != nil {
		return fmt.Errorf("healthz: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthz: %s", resp.Status)
	}
	return nil
}
