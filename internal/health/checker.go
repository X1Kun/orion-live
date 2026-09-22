package health

import (
	"context"
	"database/sql"
	"sync/atomic"

	redisclient "github.com/go-redis/redis/v8"
)

type ComponentChecker interface {
	Ready() error
}

type Checker struct {
	db         *sql.DB
	redis      *redisclient.Client
	components []ComponentChecker
	ready      atomic.Bool
}

func NewChecker(db *sql.DB, redis *redisclient.Client, components ...ComponentChecker) *Checker {
	checker := &Checker{db: db, redis: redis, components: components}
	checker.ready.Store(true)
	return checker
}

func (c *Checker) SetDraining() {
	c.ready.Store(false)
}

func (c *Checker) Ready(ctx context.Context) error {
	if !c.ready.Load() {
		return context.Canceled
	}
	if err := c.db.PingContext(ctx); err != nil {
		return err
	}
	if err := c.redis.Ping(ctx).Err(); err != nil {
		return err
	}
	for _, component := range c.components {
		if err := component.Ready(); err != nil {
			return err
		}
	}
	return nil
}
