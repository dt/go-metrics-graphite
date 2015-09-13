package graphite

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/dt/go-metrics"
)

// GraphiteConfig provides a container with configuration parameters for
// the Graphite exporter
type GraphiteConfig struct {
	Addr          *net.TCPAddr     // Network address to connect to
	Registry      metrics.Registry // Registry to be exported
	FlushInterval time.Duration    // Flush interval
	DurationUnit  time.Duration    // Time conversion unit for durations
	Prefix        string           // Prefix to be prepended to metric names
	Percentiles   []float64        // Percentiles to export from timers and histograms
}

// Graphite is a blocking exporter function which reports metrics in r
// to a graphite server located at addr, flushing them every d duration
// and prepending metric names with prefix.
func Graphite(r metrics.Registry, d time.Duration, prefix string, addr *net.TCPAddr) {
	GraphiteWithConfig(GraphiteConfig{
		Addr:          addr,
		Registry:      r,
		FlushInterval: d,
		DurationUnit:  time.Nanosecond,
		Prefix:        prefix,
		Percentiles:   []float64{0.5, 0.75, 0.95, 0.99, 0.999},
	})
}

// GraphiteWithConfig is a blocking exporter function just like Graphite,
// but it takes a GraphiteConfig instead.
func GraphiteWithConfig(c GraphiteConfig) {
	for _ = range time.Tick(c.FlushInterval) {
		if err := graphite(&c); nil != err {
			log.Println(err)
		}
	}
}

// GraphiteOnce performs a single submission to Graphite, returning a
// non-nil error on failed connections. This can be used in a loop
// similar to GraphiteWithConfig for custom error handling.
func GraphiteOnce(c GraphiteConfig) error {
	return graphite(&c)
}

func graphite(c *GraphiteConfig) error {
	now := time.Now().Unix()
	du := float64(c.DurationUnit)
	conn, err := net.DialTCP("tcp", nil, c.Addr)
	if nil != err {
		return err
	}
	defer conn.Close()
	w := bufio.NewWriter(conn)
	c.Registry.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case metrics.Counter:
			fmt.Fprintf(w, ExportFormats.Counter, c.Prefix, name, metric.Count(), now)
		case metrics.Gauge:
			fmt.Fprintf(w, ExportFormats.Gauge, c.Prefix, name, metric.Value(), now)
		case metrics.GaugeFloat64:
			fmt.Fprintf(w, ExportFormats.GaugeFloat64, c.Prefix, name, metric.Value(), now)
		case metrics.Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles(c.Percentiles)
			fmt.Fprintf(w, ExportFormats.HistogramCount, c.Prefix, name, h.Count(), now)
			fmt.Fprintf(w, ExportFormats.Min, c.Prefix, name, h.Min(), now)
			fmt.Fprintf(w, ExportFormats.Max, c.Prefix, name, h.Max(), now)
			fmt.Fprintf(w, ExportFormats.Mean, c.Prefix, name, h.Mean(), now)
			fmt.Fprintf(w, ExportFormats.Stddev, c.Prefix, name, h.StdDev(), now)
			for psIdx, psKey := range c.Percentiles {
				key := strings.Replace(strconv.FormatFloat(psKey*100.0, 'f', -1, 64), ".", "", 1)
				fmt.Fprintf(w, ExportFormats.Percentile, c.Prefix, name, key, ps[psIdx], now)
			}
		case metrics.Meter:
			m := metric.Snapshot()
			fmt.Fprintf(w, ExportFormats.HistogramCount, c.Prefix, name, m.Count(), now)
			fmt.Fprintf(w, ExportFormats.Rate1, c.Prefix, name, m.Rate1(), now)
			fmt.Fprintf(w, ExportFormats.Rate5, c.Prefix, name, m.Rate5(), now)
			fmt.Fprintf(w, ExportFormats.Rate15, c.Prefix, name, m.Rate15(), now)
			fmt.Fprintf(w, ExportFormats.Mean, c.Prefix, name, m.RateMean(), now)
		case metrics.Timer:
			t := metric.Snapshot()
			ps := t.Percentiles(c.Percentiles)
			fmt.Fprintf(w, ExportFormats.HistogramCount, c.Prefix, name, t.Count(), now)
			fmt.Fprintf(w, ExportFormats.Min, c.Prefix, name, t.Min()/int64(du), now)
			fmt.Fprintf(w, ExportFormats.Max, c.Prefix, name, t.Max()/int64(du), now)
			fmt.Fprintf(w, ExportFormats.Mean, c.Prefix, name, t.Mean()/du, now)
			fmt.Fprintf(w, ExportFormats.Stddev, c.Prefix, name, t.StdDev()/du, now)
			for psIdx, psKey := range c.Percentiles {
				key := strings.Replace(strconv.FormatFloat(psKey*100.0, 'f', -1, 64), ".", "", 1)
				fmt.Fprintf(w, ExportFormats.Percentile, c.Prefix, name, key, ps[psIdx]/du, now)
			}
			fmt.Fprintf(w, ExportFormats.Rate1, c.Prefix, name, t.Rate1(), now)
			fmt.Fprintf(w, ExportFormats.Rate5, c.Prefix, name, t.Rate5(), now)
			fmt.Fprintf(w, ExportFormats.Rate15, c.Prefix, name, t.Rate15(), now)
			fmt.Fprintf(w, ExportFormats.Mean, c.Prefix, name, t.RateMean(), now)
		default:
			log.Printf("Cannot export unknown metric type %T for '%s'\n", i, name)
		}
		w.Flush()
	})
	return nil
}
