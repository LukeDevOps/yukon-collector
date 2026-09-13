// Package metrics keeps the collector's counters and serves them in the
// Prometheus text exposition format. It has no dependency beyond the
// standard library: the collector needs a handful of counters, not a
// client library.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Counter is a monotonically increasing count split by one or more
// labels. Each distinct set of label values is its own series.
type Counter struct {
	name   string
	help   string
	labels []string

	mu     sync.Mutex
	series map[string]*atomic.Int64
}

var (
	registryMu sync.Mutex
	registry   []*Counter
)

// NewCounter registers a counter under name, with labels naming its
// label dimensions in the order Inc expects them. Names must be unique
// within the process; a repeat panics, since it is a programming error.
func NewCounter(name, help string, labels ...string) *Counter {
	c := &Counter{name: name, help: help, labels: labels, series: make(map[string]*atomic.Int64)}
	registryMu.Lock()
	defer registryMu.Unlock()
	for _, existing := range registry {
		if existing.name == name {
			panic("metrics: counter " + name + " registered twice")
		}
	}
	registry = append(registry, c)
	return c
}

// Inc adds one to the series identified by values, which must match the
// counter's labels in number and order.
func (c *Counter) Inc(values ...string) {
	c.Add(1, values...)
}

// Add adds n to the series identified by values.
func (c *Counter) Add(n int64, values ...string) {
	if len(values) != len(c.labels) {
		panic(fmt.Sprintf("metrics: counter %s wants %d label values, got %d", c.name, len(c.labels), len(values)))
	}
	c.get(values).Add(n)
}

// Value returns the current count of the series identified by values.
func (c *Counter) Value(values ...string) int64 {
	if len(values) != len(c.labels) {
		panic(fmt.Sprintf("metrics: counter %s wants %d label values, got %d", c.name, len(c.labels), len(values)))
	}
	return c.get(values).Load()
}

func (c *Counter) get(values []string) *atomic.Int64 {
	key := strings.Join(values, "\x00")
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.series[key]
	if !ok {
		v = new(atomic.Int64)
		c.series[key] = v
	}
	return v
}

// Handler serves every registered counter as text/plain in the
// Prometheus exposition format.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(Render()))
	})
}

// Render returns every registered counter in the Prometheus exposition
// format, counters sorted by name and series by label values, so the
// output is stable between calls.
func Render() string {
	registryMu.Lock()
	counters := append([]*Counter(nil), registry...)
	registryMu.Unlock()
	sort.Slice(counters, func(i, j int) bool { return counters[i].name < counters[j].name })

	var b strings.Builder
	for _, c := range counters {
		c.render(&b)
	}
	return b.String()
}

func (c *Counter) render(b *strings.Builder) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s counter\n", c.name, escapeHelp(c.help), c.name)

	c.mu.Lock()
	keys := make([]string, 0, len(c.series))
	for k := range c.series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		values := strings.Split(k, "\x00")
		b.WriteString(c.name)
		if len(c.labels) > 0 {
			b.WriteByte('{')
			for i, label := range c.labels {
				if i > 0 {
					b.WriteByte(',')
				}
				fmt.Fprintf(b, `%s="%s"`, label, escapeLabelValue(values[i]))
			}
			b.WriteByte('}')
		}
		fmt.Fprintf(b, " %d\n", c.series[k].Load())
	}
	c.mu.Unlock()
}

func escapeHelp(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(s)
}

func escapeLabelValue(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}
