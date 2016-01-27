package control

import (
	"net/http"

	"github.com/gorilla/context"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
)

func NewControlRouter(cHandler *ControlHandler) http.Handler {
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
