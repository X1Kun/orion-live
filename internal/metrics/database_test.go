package metrics

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
)

func TestDatabaseCollectorExportsPoolStats(t *testing.T) {
	db, err := sql.Open("mysql", "orion:unused@tcp(127.0.0.1:1)/orion")
	if err != nil {
		t.Fatalf("open SQL handle: %v", err)
	}
	defer db.Close()
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(NewDatabaseCollector(db))
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	want := map[string]bool{
		"orion_db_connections": false, "orion_db_max_open_connections": false,
		"orion_db_wait_total": false, "orion_db_wait_duration_seconds_total": false,
		"orion_db_connections_closed_total": false,
	}
	for _, family := range families {
		if _, ok := want[family.GetName()]; ok {
			want[family.GetName()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("metric family %q was not gathered", name)
		}
	}
}
