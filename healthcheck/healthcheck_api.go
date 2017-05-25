package healthcheck

import (
	"net/http"

	"github.com/gorilla/context"
	"github.com/twitchscience/rs_ingester/lib"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
)

// NewHealthRouter initializes the healthcheck router
func NewHealthRouter() http.Handler {

	health := web.New()

	health.Use(middleware.EnvInit)
	health.Use(middleware.RequestID)
	health.Use(middleware.RealIP)
	health.Use(lib.SimpleLogger)
	health.Use(context.ClearHandler)

	health.Get("/health", HealthCheck)

	return health
}

// HealthCheck responds with the health of the ingester
func HealthCheck(c web.C, w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
