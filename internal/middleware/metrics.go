package middleware

import (
	"strconv"
	"time"

	"github.com/X1Kun/orion-live/internal/metrics"
	"github.com/gin-gonic/gin"
)

const WebSocketConnectionKey = "websocket_connection"

// Metrics records request counts and latency using bounded route labels.
func Metrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		if c.GetBool(WebSocketConnectionKey) {
			return
		}
		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())
		// FullPath keeps metric labels bounded by using the registered route template.
		path := c.FullPath()
		if path == "" {
			path = "unmatched"
		}
		method := c.Request.Method
		metrics.HTTPRequestDuration.WithLabelValues(path, method).Observe(duration)
		metrics.HTTPRequestsTotal.WithLabelValues(path, method, status).Inc()
	}
}
