package nest

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Log is the pkg/nest logging hook. Default is no-op so this package
// can be imported without bringing in a logging dependency. The
// wrapping go2rtc layer (internal/nest) replaces it during Init with a
// function that routes structured records into the application
// logger.
//
// Signature: level is one of "debug", "info", "warn", "error"; msg is
// a short message; kv is alternating key/value pairs in the
// zerolog-style (string key, any value, repeated).
//
// Used here to surface the stream-extend lifecycle (timer arming,
// extend success/failure, terminal abandonment). Without it the
// extend path is completely silent on success and almost-silent on
// failure — which made the "new session never extended" diagnosis
// from the 09:36 outage impossible to confirm from logs.
var Log = func(level, msg string, kv ...any) {}

type API struct {
	Token     string
	ExpiresAt time.Time

	StreamProjectID string
	StreamDeviceID  string
	StreamExpiresAt time.Time

	// WebRTC
	StreamSessionID string

	// RTSP
	StreamToken          string
	StreamExtensionToken string

	extendTimer *time.Timer
	extendStop  chan struct{}

	// refreshMu serializes OAuth token refreshes against this API so
	// concurrent 401 retries (e.g. several near-simultaneous extend
	// failures, or extend + a producer reconnect) do not each fire a
	// separate refresh-token request to Google.
	refreshMu sync.Mutex
}

type Auth struct {
	AccessToken string
}

type DeviceInfo struct {
	Name      string
	DeviceID  string
	Protocols []string
}

var cache = map[string]*API{}
var cacheMu sync.Mutex

func NewAPI(clientID, clientSecret, refreshToken string) (*API, error) {
	cacheMu.Lock()
	defer cacheMu.Unlock()

	key := clientID + ":" + clientSecret + ":" + refreshToken
	now := time.Now()

	if api := cache[key]; api != nil && now.Before(api.ExpiresAt) {
		Log("debug", "[nest] OAuth cache hit (token still valid)",
			"expires_at", api.ExpiresAt,
			"ttl", time.Until(api.ExpiresAt).String())
		return api, nil
	}

	Log("debug", "[nest] OAuth acquiring new token (cache miss or expired)")

	data := url.Values{
		"grant_type":    []string{"refresh_token"},
		"client_id":     []string{clientID},
		"client_secret": []string{clientSecret},
		"refresh_token": []string{refreshToken},
	}

	client := &http.Client{Timeout: time.Second * 10}
	res, err := client.PostForm("https://www.googleapis.com/oauth2/v4/token", data)
	if err != nil {
		Log("error", "[nest] OAuth token request transport error",
			"err", err.Error())
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		// Most useful failure modes: 400 (invalid_grant — refresh token
		// revoked or wrong client_id/secret), 401 (bad credentials), 429
		// (rate limit), 5xx (Google outage). Surface the status so the
		// distinction is obvious in logs.
		Log("error", "[nest] OAuth token request rejected",
			"status", res.Status)
		return nil, errors.New("nest: wrong status: " + res.Status)
	}

	var resv struct {
		AccessToken string        `json:"access_token"`
		ExpiresIn   time.Duration `json:"expires_in"`
		Scope       string        `json:"scope"`
		TokenType   string        `json:"token_type"`
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		Log("error", "[nest] OAuth response decode failed",
			"err", err.Error())
		return nil, err
	}

	api := &API{
		Token:     resv.AccessToken,
		ExpiresAt: now.Add(resv.ExpiresIn * time.Second),
	}

	cache[key] = api

	Log("info", "[nest] OAuth token acquired",
		"expires_at", api.ExpiresAt,
		"ttl", (resv.ExpiresIn * time.Second).String())

	return api, nil
}

func (a *API) GetDevices(projectID string) ([]DeviceInfo, error) {
	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" + projectID + "/devices"
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	// 10 s timeout (was time.Second * 5000 ≈ 83 minutes — almost certainly
	// an upstream typo for 5s; left as 10s so flaky Google API calls fail
	// fast instead of blocking a goroutine for over an hour).
	client := &http.Client{Timeout: 10 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, errors.New("nest: wrong status: " + res.Status)
	}

	var resv struct {
		Devices []Device
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		return nil, err
	}

	devices := make([]DeviceInfo, 0, len(resv.Devices))

	for _, device := range resv.Devices {
		// only RTSP and WEB_RTC available (both supported)
		if len(device.Traits.SdmDevicesTraitsCameraLiveStream.SupportedProtocols) == 0 {
			continue
		}

		i := strings.LastIndexByte(device.Name, '/')
		if i <= 0 {
			continue
		}

		name := device.Traits.SdmDevicesTraitsInfo.CustomName
		// Devices configured through the Nest app use the container/room name as opposed to the customName trait
		if name == "" && len(device.ParentRelations) > 0 {
			name = device.ParentRelations[0].DisplayName
		}

		devices = append(devices, DeviceInfo{
			Name:      name,
			DeviceID:  device.Name[i+1:],
			Protocols: device.Traits.SdmDevicesTraitsCameraLiveStream.SupportedProtocols,
		})
	}

	return devices, nil
}

func (a *API) ExchangeSDP(projectID, deviceID, offer string) (string, error) {
	var reqv struct {
		Command string `json:"command"`
		Params  struct {
			Offer string `json:"offerSdp"`
		} `json:"params"`
	}
	reqv.Command = "sdm.devices.commands.CameraLiveStream.GenerateWebRtcStream"
	reqv.Params.Offer = offer

	b, err := json.Marshal(reqv)
	if err != nil {
		return "", err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		projectID + "/devices/" + deviceID + ":executeCommand"

	maxRetries := 3
	retryDelay := time.Second * 30

	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
		if err != nil {
			return "", err
		}

		req.Header.Set("Authorization", "Bearer "+a.Token)

		// 10 s timeout — see GetDevices for context on why we dropped from
		// the upstream 5000s value.
		client := &http.Client{Timeout: 10 * time.Second}
		res, err := client.Do(req)
		if err != nil {
			return "", err
		}

		// Handle 409 (Conflict), 429 (Too Many Requests), and 401 (Unauthorized)
		if res.StatusCode == 409 || res.StatusCode == 429 || res.StatusCode == 401 {
			res.Body.Close()
			// Surface the retry path so OAuth flakiness is visible
			// (previously this branch was completely silent — making
			// "ExchangeSDP eventually failed" indistinguishable from
			// "ExchangeSDP failed once and then transient-retry-loop
			// finally gave up").
			Log("warn", "[nest] ExchangeSDP retryable status, refreshing token + retrying",
				"status", res.Status,
				"attempt", attempt+1,
				"max_attempts", maxRetries,
				"retry_delay", retryDelay.String())
			if attempt < maxRetries-1 {
				// Get new token from Google
				if err := a.refreshToken(); err != nil {
					return "", err
				}
				time.Sleep(retryDelay)
				retryDelay *= 2 // exponential backoff
				continue
			}
		}

		defer res.Body.Close()

		if res.StatusCode != 200 {
			return "", errors.New("nest: wrong status: " + res.Status)
		}

		var resv struct {
			Results struct {
				Answer         string    `json:"answerSdp"`
				ExpiresAt      time.Time `json:"expiresAt"`
				MediaSessionID string    `json:"mediaSessionId"`
			} `json:"results"`
		}

		if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
			return "", err
		}

		a.StreamProjectID = projectID
		a.StreamDeviceID = deviceID
		a.StreamSessionID = resv.Results.MediaSessionID
		a.StreamExpiresAt = resv.Results.ExpiresAt

		return resv.Results.Answer, nil
	}

	return "", errors.New("nest: max retries exceeded")
}

// refreshToken obtains a fresh OAuth access token from Google and
// mutates this API instance's Token + ExpiresAt in place. Bypasses
// NewAPI's cache check intentionally — callers reach this path
// because the existing token was rejected (401), which means the
// cache's recorded ExpiresAt is wrong (server-side expiry was earlier
// than we thought) and a cache hit would just return the same stale
// token.
//
// Serialized via refreshMu so concurrent 401 retries fold into one
// refresh request; second caller through the lock benefits from the
// first's freshly written Token without re-fetching.
func (a *API) refreshToken() error {
	a.refreshMu.Lock()
	defer a.refreshMu.Unlock()

	// Look up our credentials from the cache (the cache stores APIs
	// keyed by credential triple; the key was set in NewAPI).
	var refreshKey string
	cacheMu.Lock()
	for key, api := range cache {
		if api == a {
			refreshKey = key
			break
		}
	}
	cacheMu.Unlock()

	if refreshKey == "" {
		return errors.New("nest: unable to find cached credentials")
	}

	parts := strings.Split(refreshKey, ":")
	if len(parts) != 3 {
		return errors.New("nest: invalid cache key format")
	}
	clientID, clientSecret, refreshToken := parts[0], parts[1], parts[2]

	previousExpiresAt := a.ExpiresAt
	Log("debug", "[nest] OAuth token refresh starting",
		"previous_expires_at", previousExpiresAt,
		"since_predicted_expiry", time.Since(previousExpiresAt).String())

	data := url.Values{
		"grant_type":    []string{"refresh_token"},
		"client_id":     []string{clientID},
		"client_secret": []string{clientSecret},
		"refresh_token": []string{refreshToken},
	}

	client := &http.Client{Timeout: time.Second * 10}
	res, err := client.PostForm("https://www.googleapis.com/oauth2/v4/token", data)
	if err != nil {
		Log("error", "[nest] OAuth refresh transport error",
			"err", err.Error())
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		Log("error", "[nest] OAuth refresh rejected",
			"status", res.Status)
		return errors.New("nest: token refresh failed: " + res.Status)
	}

	var resv struct {
		AccessToken string        `json:"access_token"`
		ExpiresIn   time.Duration `json:"expires_in"`
	}
	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		Log("error", "[nest] OAuth refresh response decode failed",
			"err", err.Error())
		return err
	}

	a.Token = resv.AccessToken
	a.ExpiresAt = time.Now().Add(resv.ExpiresIn * time.Second)

	Log("info", "[nest] OAuth token refreshed",
		"new_expires_at", a.ExpiresAt,
		"new_ttl", (resv.ExpiresIn * time.Second).String())

	return nil
}

// extendBackoffInitial is the initial back-off delay for transient
// 409/429 responses in ExtendStream. Doubles after each failed retry up
// to maxRetries. Exposed as a package variable so tests can shorten it.
var extendBackoffInitial = 30 * time.Second

// extendURI builds the Google SDM API URI for a stream-extend command.
// Hookable for tests so they can point requests at an httptest server
// without needing a live Google endpoint or network access.
var extendURI = func(projectID, deviceID string) string {
	return "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		projectID + "/devices/" + deviceID + ":executeCommand"
}

func (a *API) ExtendStream() error {
	var reqv struct {
		Command string `json:"command"`
		Params  struct {
			MediaSessionID       string `json:"mediaSessionId,omitempty"`
			StreamExtensionToken string `json:"streamExtensionToken,omitempty"`
		} `json:"params"`
	}

	if a.StreamToken != "" {
		// RTSP
		reqv.Command = "sdm.devices.commands.CameraLiveStream.ExtendRtspStream"
		reqv.Params.StreamExtensionToken = a.StreamExtensionToken
	} else {
		// WebRTC
		reqv.Command = "sdm.devices.commands.CameraLiveStream.ExtendWebRtcStream"
		reqv.Params.MediaSessionID = a.StreamSessionID
	}

	b, err := json.Marshal(reqv)
	if err != nil {
		return err
	}

	uri := extendURI(a.StreamProjectID, a.StreamDeviceID)

	// Helper because the request must be replayed verbatim after a retry
	// (a single *http.Request can't be Do'd twice — the body reader
	// would be drained).
	doRequest := func() (*http.Response, error) {
		req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+a.Token)
		client := &http.Client{Timeout: 10 * time.Second}
		return client.Do(req)
	}

	// Retry loop handles three classes of response status:
	//   - 401 Unauthorized: OAuth tokens expire after ~60 minutes and a
	//     mid-session extend will land on a freshly-expired token. Refresh
	//     and retry immediately (counts as one attempt, no backoff).
	//   - 409 Conflict / 429 Too Many Requests: transient server-side
	//     condition. Back off exponentially and retry.
	//   - 200 OK: parse the new session expiry/token and return.
	//   - anything else: surface as error immediately.
	const maxRetries = 3
	backoff := extendBackoffInitial

	for attempt := 0; attempt < maxRetries; attempt++ {
		res, err := doRequest()
		if err != nil {
			return err
		}

		if res.StatusCode == 401 && attempt < maxRetries-1 {
			res.Body.Close()
			// until_predicted_expiry sign distinguishes two cases:
			//   positive: token rejected *before* our predicted expiry —
			//     Google rotated early, or our cached ExpiresAt was wrong
			//   negative: token was rejected *after* our predicted expiry —
			//     we should have refreshed proactively (current code is
			//     lazy 401-driven, so this is normal)
			Log("info", "[nest] extend got 401, refreshing OAuth token",
				"session", a.StreamSessionID,
				"predicted_expires_at", a.ExpiresAt,
				"until_predicted_expiry", time.Until(a.ExpiresAt).String(),
				"attempt", attempt+1,
				"max_attempts", maxRetries)
			if err := a.refreshToken(); err != nil {
				return errors.New("nest: token refresh failed during extend: " + err.Error())
			}
			continue
		}

		if (res.StatusCode == 409 || res.StatusCode == 429) && attempt < maxRetries-1 {
			res.Body.Close()
			Log("warn", "[nest] extend got transient status, backing off and retrying",
				"session", a.StreamSessionID,
				"status", res.Status,
				"attempt", attempt+1,
				"max_attempts", maxRetries,
				"backoff", backoff.String())
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		if res.StatusCode != 200 {
			res.Body.Close()
			return errors.New("nest: wrong status: " + res.Status)
		}

		var resv struct {
			Results struct {
				ExpiresAt            time.Time `json:"expiresAt"`
				MediaSessionID       string    `json:"mediaSessionId"`
				StreamExtensionToken string    `json:"streamExtensionToken"`
				StreamToken          string    `json:"streamToken"`
			} `json:"results"`
		}

		err = json.NewDecoder(res.Body).Decode(&resv)
		res.Body.Close()
		if err != nil {
			return err
		}

		a.StreamSessionID = resv.Results.MediaSessionID
		a.StreamExpiresAt = resv.Results.ExpiresAt
		a.StreamExtensionToken = resv.Results.StreamExtensionToken
		a.StreamToken = resv.Results.StreamToken

		return nil
	}

	return errors.New("nest: extend max retries exceeded")
}

func (a *API) GenerateRtspStream(projectID, deviceID string) (string, error) {
	var reqv struct {
		Command string   `json:"command"`
		Params  struct{} `json:"params"`
	}
	reqv.Command = "sdm.devices.commands.CameraLiveStream.GenerateRtspStream"

	b, err := json.Marshal(reqv)
	if err != nil {
		return "", err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		projectID + "/devices/" + deviceID + ":executeCommand"
	req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	// 10 s timeout — see GetDevices for context.
	client := &http.Client{Timeout: 10 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	// defer Close() to cover the non-200 / decode-error early returns; the
	// original code only closed the body via the implicit decoder path on
	// the success branch, leaking connections under sustained errors.
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return "", errors.New("nest: wrong status: " + res.Status)
	}

	var resv struct {
		Results struct {
			StreamURLs           map[string]string `json:"streamUrls"`
			StreamExtensionToken string            `json:"streamExtensionToken"`
			StreamToken          string            `json:"streamToken"`
			ExpiresAt            time.Time         `json:"expiresAt"`
		} `json:"results"`
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		return "", err
	}

	if _, ok := resv.Results.StreamURLs["rtspUrl"]; !ok {
		return "", errors.New("nest: failed to generate rtsp url")
	}

	a.StreamProjectID = projectID
	a.StreamDeviceID = deviceID
	a.StreamToken = resv.Results.StreamToken
	a.StreamExtensionToken = resv.Results.StreamExtensionToken
	a.StreamExpiresAt = resv.Results.ExpiresAt

	return resv.Results.StreamURLs["rtspUrl"], nil
}

func (a *API) StopRTSPStream() error {
	if a.StreamProjectID == "" || a.StreamDeviceID == "" {
		return errors.New("nest: tried to stop rtsp stream without a project or device ID")
	}

	var reqv struct {
		Command string `json:"command"`
		Params  struct {
			StreamExtensionToken string `json:"streamExtensionToken"`
		} `json:"params"`
	}
	reqv.Command = "sdm.devices.commands.CameraLiveStream.StopRtspStream"
	reqv.Params.StreamExtensionToken = a.StreamExtensionToken

	b, err := json.Marshal(reqv)
	if err != nil {
		return err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		a.StreamProjectID + "/devices/" + a.StreamDeviceID + ":executeCommand"
	req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	// 10 s timeout — see GetDevices for context.
	client := &http.Client{Timeout: 10 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	// defer Close() so the non-200 early return doesn't leak the connection.
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return errors.New("nest: wrong status: " + res.Status)
	}

	a.StreamProjectID = ""
	a.StreamDeviceID = ""
	a.StreamExtensionToken = ""
	a.StreamToken = ""

	return nil
}

type Device struct {
	Name string `json:"name"`
	Type string `json:"type"`
	//Assignee string `json:"assignee"`
	Traits struct {
		SdmDevicesTraitsInfo struct {
			CustomName string `json:"customName"`
		} `json:"sdm.devices.traits.Info"`
		SdmDevicesTraitsCameraLiveStream struct {
			VideoCodecs        []string `json:"videoCodecs"`
			AudioCodecs        []string `json:"audioCodecs"`
			SupportedProtocols []string `json:"supportedProtocols"`
		} `json:"sdm.devices.traits.CameraLiveStream"`
		//SdmDevicesTraitsCameraImage struct {
		//	MaxImageResolution struct {
		//		Width  int `json:"width"`
		//		Height int `json:"height"`
		//	} `json:"maxImageResolution"`
		//} `json:"sdm.devices.traits.CameraImage"`
		//SdmDevicesTraitsCameraPerson struct {
		//} `json:"sdm.devices.traits.CameraPerson"`
		//SdmDevicesTraitsCameraMotion struct {
		//} `json:"sdm.devices.traits.CameraMotion"`
		//SdmDevicesTraitsDoorbellChime struct {
		//} `json:"sdm.devices.traits.DoorbellChime"`
		//SdmDevicesTraitsCameraClipPreview struct {
		//} `json:"sdm.devices.traits.CameraClipPreview"`
	} `json:"traits"`
	ParentRelations []struct {
		Parent      string `json:"parent"`
		DisplayName string `json:"displayName"`
	} `json:"parentRelations"`
}

func (a *API) StartExtendStreamTimer() {
	if a.extendTimer != nil {
		Log("debug", "[nest] extend timer already armed, skipping",
			"session", a.StreamSessionID)
		return
	}

	// Nest streams expire ~5 minutes after the last extension. Re-arm after
	// each successful ExtendStream so the loop keeps running; without the loop
	// the stream silently dies at the second expiry (~10 min after connect).
	delay := extendDelay(a.StreamExpiresAt)
	a.extendTimer = time.NewTimer(delay)
	a.extendStop = make(chan struct{})
	timer, stop := a.extendTimer, a.extendStop

	Log("debug", "[nest] extend timer armed",
		"session", a.StreamSessionID,
		"expires_at", a.StreamExpiresAt,
		"next_fire_in", delay.String())

	go func() {
		for {
			select {
			case <-stop:
				Log("debug", "[nest] extend goroutine stopped (stop signal)",
					"session", a.StreamSessionID)
				return
			case <-timer.C:
			}

			if err := a.ExtendStream(); err != nil {
				Log("warn", "[nest] extend failed, retrying in 10s",
					"session", a.StreamSessionID,
					"err", err.Error())
				// Retry once after a short delay to ride out transient errors.
				select {
				case <-stop:
					Log("debug", "[nest] extend goroutine stopped during retry wait",
						"session", a.StreamSessionID)
					return
				case <-time.After(10 * time.Second):
				}
				if err := a.ExtendStream(); err != nil {
					Log("error", "[nest] extend giving up — stream will die at expires_at",
						"session", a.StreamSessionID,
						"expires_at", a.StreamExpiresAt,
						"err", err.Error())
					return
				}
			}

			next := extendDelay(a.StreamExpiresAt)
			Log("debug", "[nest] extend ok",
				"session", a.StreamSessionID,
				"expires_at", a.StreamExpiresAt,
				"next_fire_in", next.String())
			timer.Reset(next)
		}
	}()
}

func (a *API) StopExtendStreamTimer() {
	if a.extendTimer != nil {
		Log("debug", "[nest] extend timer cancelled",
			"session", a.StreamSessionID)
		close(a.extendStop)
		a.extendTimer.Stop()
		a.extendTimer = nil
		a.extendStop = nil
	}
}

// extendDelay returns the wait time before the next stream extension call.
// Clamped to a 1-second minimum so a stale or already-past expiry doesn't
// cause a busy-loop of ExtendStream() calls.
func extendDelay(expiresAt time.Time) time.Duration {
	d := time.Until(expiresAt) - time.Minute
	if d < time.Second {
		return time.Second
	}
	return d
}
