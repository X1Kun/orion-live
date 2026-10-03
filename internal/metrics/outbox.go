package metrics

import (
	"context"
	"time"

	"github.com/X1Kun/orion-live/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
)

type OutboxCollector struct {
	db                *gorm.DB
	events            *prometheus.Desc
	oldestUnpublished *prometheus.Desc
	collectionSuccess *prometheus.Desc
}

func NewOutboxCollector(db *gorm.DB) *OutboxCollector {
	return &OutboxCollector{
		db: db,
		events: prometheus.NewDesc(
			"orion_outbox_events", "Current Outbox event count by status.", []string{"status"}, nil,
		),
		oldestUnpublished: prometheus.NewDesc(
			"orion_outbox_oldest_unpublished_age_seconds", "Age of the oldest pending or claimed Outbox event.", nil, nil,
		),
		collectionSuccess: prometheus.NewDesc(
			"orion_outbox_collection_success", "Whether the latest Outbox metrics collection succeeded.", nil, nil,
		),
	}
}

func (c *OutboxCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.events
	ch <- c.oldestUnpublished
	ch <- c.collectionSuccess
}

func (c *OutboxCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	type statusCount struct {
		Status model.OutboxStatus
		Count  int64
	}
	var rows []statusCount
	if err := c.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Select("status, COUNT(*) AS count").
		Group("status").
		Scan(&rows).Error; err != nil {
		ch <- prometheus.MustNewConstMetric(c.collectionSuccess, prometheus.GaugeValue, 0)
		return
	}

	counts := map[model.OutboxStatus]float64{
		model.OutboxStatusPending: 0, model.OutboxStatusClaimed: 0,
		model.OutboxStatusPublished: 0, model.OutboxStatusFailed: 0,
	}
	for _, row := range rows {
		counts[row.Status] = float64(row.Count)
	}
	for _, status := range []model.OutboxStatus{
		model.OutboxStatusPending, model.OutboxStatusClaimed,
		model.OutboxStatusPublished, model.OutboxStatusFailed,
	} {
		ch <- prometheus.MustNewConstMetric(c.events, prometheus.GaugeValue, counts[status], string(status))
	}

	var oldest struct{ CreatedAt *time.Time }
	if err := c.db.WithContext(ctx).
		Model(&model.OutboxEvent{}).
		Select("MIN(created_at) AS created_at").
		Where("status IN ?", []model.OutboxStatus{model.OutboxStatusPending, model.OutboxStatusClaimed}).
		Scan(&oldest).Error; err != nil {
		ch <- prometheus.MustNewConstMetric(c.collectionSuccess, prometheus.GaugeValue, 0)
		return
	}
	age := 0.0
	if oldest.CreatedAt != nil {
		age = time.Since(oldest.CreatedAt.UTC()).Seconds()
		if age < 0 {
			age = 0
		}
	}
	ch <- prometheus.MustNewConstMetric(c.oldestUnpublished, prometheus.GaugeValue, age)
	ch <- prometheus.MustNewConstMetric(c.collectionSuccess, prometheus.GaugeValue, 1)
}
