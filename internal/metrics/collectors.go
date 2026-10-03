package metrics

import (
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
)

func RegisterInfrastructureCollectors(sqlDB *sql.DB, db *gorm.DB) {
	prometheus.MustRegister(NewDatabaseCollector(sqlDB), NewOutboxCollector(db))
}
