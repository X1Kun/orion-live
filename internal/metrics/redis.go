package metrics

import (
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
)

// RedisPoolCollector reads local client statistics; scrapes never contact Redis.
type RedisPoolCollector struct {
	client      *redis.Client
	connections *prometheus.Desc
	limit       *prometheus.Desc
	hits        *prometheus.Desc
	misses      *prometheus.Desc
	timeouts    *prometheus.Desc
	stale       *prometheus.Desc
}

func NewRedisPoolCollector(client *redis.Client) *RedisPoolCollector {
	return &RedisPoolCollector{
		client:      client,
		connections: prometheus.NewDesc("orion_redis_pool_connections", "Local Redis pool connections by state.", []string{"state"}, nil),
		limit:       prometheus.NewDesc("orion_redis_pool_size", "Configured Redis connection pool size.", nil, nil),
		hits:        prometheus.NewDesc("orion_redis_pool_hits_total", "Acquisitions that found an idle Redis connection.", nil, nil),
		misses:      prometheus.NewDesc("orion_redis_pool_misses_total", "Acquisitions with no idle Redis connection; not a wait counter.", nil, nil),
		timeouts:    prometheus.NewDesc("orion_redis_pool_timeouts_total", "Redis connection pool wait timeouts. Does not include every context deadline or command timeout.", nil, nil),
		stale:       prometheus.NewDesc("orion_redis_pool_stale_connections_total", "Stale Redis connections removed by the pool.", nil, nil),
	}
}

func (c *RedisPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{c.connections, c.limit, c.hits, c.misses, c.timeouts, c.stale} {
		ch <- desc
	}
}

func (c *RedisPoolCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.client.PoolStats()
	ch <- prometheus.MustNewConstMetric(c.connections, prometheus.GaugeValue, float64(stats.TotalConns), "total")
	ch <- prometheus.MustNewConstMetric(c.connections, prometheus.GaugeValue, float64(stats.IdleConns), "idle")
	ch <- prometheus.MustNewConstMetric(c.limit, prometheus.GaugeValue, float64(c.client.Options().PoolSize))
	ch <- prometheus.MustNewConstMetric(c.hits, prometheus.CounterValue, float64(stats.Hits))
	ch <- prometheus.MustNewConstMetric(c.misses, prometheus.CounterValue, float64(stats.Misses))
	ch <- prometheus.MustNewConstMetric(c.timeouts, prometheus.CounterValue, float64(stats.Timeouts))
	ch <- prometheus.MustNewConstMetric(c.stale, prometheus.CounterValue, float64(stats.StaleConns))
}
