package nest

import (
	"net/http"
	"strings"

	"github.com/AlexxIT/go2rtc/internal/api"
	"github.com/AlexxIT/go2rtc/internal/app"
	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/nest"
	"github.com/rs/zerolog"
)

func Init() {
	// Route pkg/nest's structured diagnostic events into the app
	// logger. Default is no-op so pkg/nest stays import-clean.
	nestLog := app.GetLogger("nest")
	nest.Log = func(level, msg string, kv ...any) {
		var ev *zerolog.Event
		switch level {
		case "debug":
			ev = nestLog.Debug()
		case "info":
			ev = nestLog.Info()
		case "warn":
			ev = nestLog.Warn()
		case "error":
			ev = nestLog.Error()
		default:
			ev = nestLog.Debug()
		}
		for i := 0; i+1 < len(kv); i += 2 {
			key, ok := kv[i].(string)
			if !ok {
				continue
			}
			ev = ev.Interface(key, kv[i+1])
		}
		ev.Msg(msg)
	}

	streams.HandleFunc("nest", func(source string) (core.Producer, error) {
		return nest.Dial(source)
	})

	api.HandleFunc("api/nest", apiNest)
}

func apiNest(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	clientID := query.Get("client_id")
	clientSecret := query.Get("client_secret")
	refreshToken := query.Get("refresh_token")
	projectID := query.Get("project_id")

	nestAPI, err := nest.NewAPI(clientID, clientSecret, refreshToken)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	devices, err := nestAPI.GetDevices(projectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var items []*api.Source

	for _, device := range devices {
		query.Set("device_id", device.DeviceID)
		query.Set("protocols", strings.Join(device.Protocols, ","))

		items = append(items, &api.Source{
			Name: device.Name, URL: "nest:?" + query.Encode(),
		})
	}

	api.ResponseSources(w, items)
}
