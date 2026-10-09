package metrics

import (
	"database/sql"
	"github.com/go-redis/redis/v8"

	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
)

func RegisterInfrastructureCollectors(sqlDB *sql.DB, db *gorm.DB) {
	prometheus.MustRegister(NewDatabaseCollector(sqlDB), NewOutboxCollector(db))
}

func RegisterRedisCollector(client *redis.Client) {
	prometheus.MustRegister(NewRedisPoolCollector(client))
}
