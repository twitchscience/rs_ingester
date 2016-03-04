package control

import (
	"net/http"

	"github.com/gorilla/context"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
)

// NewControlRouter instantiates an http.Handler with the control routes
func NewControlRouter(cHandler *Handler) http.Handler {
	control := web.New()

	control.Use(middleware.EnvInit)
	control.Use(middleware.RequestID)
	control.Use(middleware.RealIP)
	control.Use(middleware.Logger)
	control.Use(context.ClearHandler)

	control.Post("/control/ingest", cHandler.ForceIngest)
	control.Get("/loads/tables", cHandler.GetPendingTables)

	return control
}
