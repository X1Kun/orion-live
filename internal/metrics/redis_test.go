package metrics

import (
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"testing"
)

func TestRedisPoolCollectorReadsLocalStatsWithoutDialing(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", PoolSize: 7, IdleCheckFrequency: -1})
	defer client.Close()
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(NewRedisPoolCollector(client))
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 6 {
		t.Fatalf("metric families=%d, want 6", len(families))
	}
	for _, family := range families {
		if family.GetName() == "orion_redis_pool_size" && family.Metric[0].GetGauge().GetValue() != 7 {
			t.Fatal("incorrect pool size")
		}
		if family.GetName() == "orion_redis_pool_connections" {
			for _, metric := range family.Metric {
				if metric.GetGauge().GetValue() != 0 {
					t.Fatal("unexpected connections opened during collection")
				}
			}
		}
	}
}
