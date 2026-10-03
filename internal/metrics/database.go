package metrics

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
)

type DatabaseCollector struct {
	db                 *sql.DB
	connections        *prometheus.Desc
	maxOpenConnections *prometheus.Desc
	waitTotal          *prometheus.Desc
	waitDuration       *prometheus.Desc
	closedTotal        *prometheus.Desc
}

func NewDatabaseCollector(db *sql.DB) *DatabaseCollector {
	return &DatabaseCollector{
		db: db,
		connections: prometheus.NewDesc(
			"orion_db_connections", "Current SQL connections by state.", []string{"state"}, nil,
		),
		maxOpenConnections: prometheus.NewDesc(
			"orion_db_max_open_connections", "Configured maximum number of open SQL connections.", nil, nil,
		),
		waitTotal: prometheus.NewDesc(
			"orion_db_wait_total", "Total number of waits for a SQL connection.", nil, nil,
		),
		waitDuration: prometheus.NewDesc(
			"orion_db_wait_duration_seconds_total", "Total time blocked waiting for a SQL connection.", nil, nil,
		),
		closedTotal: prometheus.NewDesc(
			"orion_db_connections_closed_total", "Total SQL connections closed by reason.", []string{"reason"}, nil,
		),
	}
}

func (c *DatabaseCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.connections
	ch <- c.maxOpenConnections
	ch <- c.waitTotal
	ch <- c.waitDuration
	ch <- c.closedTotal
}

func (c *DatabaseCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.db.Stats()
	ch <- prometheus.MustNewConstMetric(c.connections, prometheus.GaugeValue, float64(stats.OpenConnections), "open")
	ch <- prometheus.MustNewConstMetric(c.connections, prometheus.GaugeValue, float64(stats.InUse), "in_use")
	ch <- prometheus.MustNewConstMetric(c.connections, prometheus.GaugeValue, float64(stats.Idle), "idle")
	ch <- prometheus.MustNewConstMetric(c.maxOpenConnections, prometheus.GaugeValue, float64(stats.MaxOpenConnections))
	ch <- prometheus.MustNewConstMetric(c.waitTotal, prometheus.CounterValue, float64(stats.WaitCount))
	ch <- prometheus.MustNewConstMetric(c.waitDuration, prometheus.CounterValue, stats.WaitDuration.Seconds())
	ch <- prometheus.MustNewConstMetric(c.closedTotal, prometheus.CounterValue, float64(stats.MaxIdleClosed), "max_idle")
	ch <- prometheus.MustNewConstMetric(c.closedTotal, prometheus.CounterValue, float64(stats.MaxIdleTimeClosed), "max_idle_time")
	ch <- prometheus.MustNewConstMetric(c.closedTotal, prometheus.CounterValue, float64(stats.MaxLifetimeClosed), "max_lifetime")
}
