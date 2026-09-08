package router

import (
	"net/http"

	"github.com/X1Kun/orion-live/internal/handler"
	"github.com/X1Kun/orion-live/internal/middleware"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func New(users *handler.UserHandler, liveSessions *handler.LiveSessionHandler, health *handler.HealthHandler, jwtSecret string) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery(), middleware.Metrics())

	router.GET("/healthz", health.Live)
	router.GET("/readyz", health.Ready)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	v1 := router.Group("/api/v1")
	v1.POST("/users/register", users.Register)
	v1.POST("/users/login", users.Login)
	v1.GET("/live-sessions/:id", liveSessions.Get)

	protected := v1.Group("")
	protected.Use(middleware.Auth(jwtSecret))
	protected.GET("/profile", users.GetProfile)
	protected.POST("/live-sessions", liveSessions.Create)
	protected.POST("/live-sessions/:id/start", liveSessions.Start)
	protected.POST("/live-sessions/:id/end", liveSessions.End)

	router.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, gin.H{
			"error": gin.H{"code": "NOT_FOUND", "message": "resource not found"},
		})
	})
	return router
}
