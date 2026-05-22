package nest

import (
	"errors"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/rtsp"
	"github.com/AlexxIT/go2rtc/pkg/webrtc"
	pion "github.com/pion/webrtc/v4"
)

// refreshBeforeCap is how long before Nest's max-stream-lifetime cap to
// trigger a proactive reconnect. Google's WebRTC stream lifetime is
// documented as up to 1 hour; empirically the cap hits between 60 and
// 67 minutes. Triggering refresh at +55 minutes from initial Exchange
// gives a 5+ minute buffer for the swap to complete (GetProducer for
// nest takes a few seconds — ExchangeSDP RPC + ICE setup) before the
// old session goes silent.
//
// Exposed as a package variable so tests can shorten it.
var refreshBeforeCap = 55 * time.Minute

type WebRTCClient struct {
	conn *webrtc.Conn
	api  *API

	// proactiveReconnect callback wired up by the wrapping
	// streams.Producer via SetReconnectCallback. Invoked once by the
	// refresh timer ahead of Nest's lifetime cap. Guarded by mu so the
	// timer goroutine and the producer reconnect path (which calls
	// SetReconnectCallback again on the new conn after a swap) can't
	// race.
	mu              sync.Mutex
	reconnectCB     func()
	refreshTimer    *time.Timer
	refreshFired    bool
}

type RTSPClient struct {
	conn *rtsp.Conn
	api  *API

	mu              sync.Mutex
	reconnectCB     func()
	refreshTimer    *time.Timer
	refreshFired    bool
}

func Dial(rawURL string) (core.Producer, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	query := u.Query()
	cliendID := query.Get("client_id")
	cliendSecret := query.Get("client_secret")
	refreshToken := query.Get("refresh_token")
	projectID := query.Get("project_id")
	deviceID := query.Get("device_id")

	if cliendID == "" || cliendSecret == "" || refreshToken == "" || projectID == "" || deviceID == "" {
		return nil, errors.New("nest: wrong query")
	}

	maxRetries := 3
	retryDelay := time.Second * 30

	var nestAPI *API
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		nestAPI, err = NewAPI(cliendID, cliendSecret, refreshToken)
		if err == nil {
			break
		}
		lastErr = err
		if attempt < maxRetries-1 {
			time.Sleep(retryDelay)
			retryDelay *= 2 // exponential backoff
		}
	}

	if nestAPI == nil {
		return nil, lastErr
	}

	protocols := strings.Split(query.Get("protocols"), ",")
	if len(protocols) > 0 && protocols[0] == "RTSP" {
		return rtspConn(nestAPI, rawURL, projectID, deviceID)
	}

	// Default to WEB_RTC for backwards compataiility
	return rtcConn(nestAPI, rawURL, projectID, deviceID)
}

func (c *WebRTCClient) GetMedias() []*core.Media {
	return c.conn.GetMedias()
}

func (c *WebRTCClient) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	return c.conn.GetTrack(media, codec)
}

func (c *WebRTCClient) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	return c.conn.AddTrack(media, codec, track)
}

func (c *WebRTCClient) Start() error {
	c.scheduleRefresh()
	c.api.StartExtendStreamTimer()
	return c.conn.Start()
}

func (c *WebRTCClient) Stop() error {
	c.cancelRefresh()
	c.api.StopExtendStreamTimer()
	return c.conn.Stop()
}

// SetReconnectCallback implements core.LifetimeLimited. Called by the
// streams.Producer wrapping this client during start (and again after
// each reconnect-swap onto a new client instance).
func (c *WebRTCClient) SetReconnectCallback(cb func()) {
	c.mu.Lock()
	c.reconnectCB = cb
	c.mu.Unlock()
}

// scheduleRefresh arms a one-shot timer to invoke the producer's
// proactive-reconnect callback ahead of Nest's lifetime cap. Safe to
// call multiple times — only the first arm takes effect.
func (c *WebRTCClient) scheduleRefresh() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refreshTimer != nil {
		return
	}
	c.refreshTimer = time.AfterFunc(refreshBeforeCap, c.fireRefresh)
}

func (c *WebRTCClient) cancelRefresh() {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Mark as fired so any in-flight fireRefresh that races us (timer
	// already entered the AfterFunc goroutine before we Stop'd it)
	// no-ops. Without this, a near-simultaneous Stop + timer-fire can
	// trigger a proactive reconnect on a client that's being torn
	// down — and the late callback could fire long after the new
	// client took over, mistargeting the producer.
	c.refreshFired = true
	if c.refreshTimer != nil {
		c.refreshTimer.Stop()
		c.refreshTimer = nil
	}
}

// fireRefresh runs in the timer goroutine. Invokes the producer's
// proactive-reconnect callback in a fresh goroutine so the timer
// runner isn't blocked by the reconnect — GetProducer for Nest can
// take several seconds (ExchangeSDP + ICE).
func (c *WebRTCClient) fireRefresh() {
	c.mu.Lock()
	if c.refreshFired {
		c.mu.Unlock()
		return
	}
	c.refreshFired = true
	cb := c.reconnectCB
	c.mu.Unlock()
	if cb == nil {
		Log("warn", "[nest] refresh timer fired but no reconnect callback registered",
			"session", c.api.StreamSessionID)
		return
	}
	Log("info", "[nest] refresh timer fired — triggering proactive reconnect",
		"session", c.api.StreamSessionID,
		"age", refreshBeforeCap.String())
	go cb()
}

func (c *WebRTCClient) MarshalJSON() ([]byte, error) {
	return c.conn.MarshalJSON()
}

func rtcConn(nestAPI *API, rawURL, projectID, deviceID string) (*WebRTCClient, error) {
	maxRetries := 3
	retryDelay := time.Second * 30
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		rtcAPI, err := webrtc.NewAPI()
		if err != nil {
			return nil, err
		}

		conf := pion.Configuration{}
		pc, err := rtcAPI.NewPeerConnection(conf)
		if err != nil {
			return nil, err
		}

		conn := webrtc.NewConn(pc)
		conn.FormatName = "nest/webrtc"
		conn.Mode = core.ModeActiveProducer
		conn.Protocol = "http"
		conn.URL = rawURL

		// https://developers.google.com/nest/device-access/traits/device/camera-live-stream#generatewebrtcstream-request-fields
		medias := []*core.Media{
			{Kind: core.KindAudio, Direction: core.DirectionRecvonly},
			{Kind: core.KindVideo, Direction: core.DirectionRecvonly},
			{Kind: "app"}, // important for Nest
		}

		// 3. Create offer with candidates
		offer, err := conn.CreateCompleteOffer(medias)
		if err != nil {
			return nil, err
		}

		// 4. Exchange SDP via Hass
		answer, err := nestAPI.ExchangeSDP(projectID, deviceID, offer)
		if err != nil {
			lastErr = err
			if attempt < maxRetries-1 {
				time.Sleep(retryDelay)
				retryDelay *= 2
				continue
			}
			return nil, err
		}

		// 5. Set answer with remote medias
		if err = conn.SetAnswer(answer); err != nil {
			return nil, err
		}

		return &WebRTCClient{conn: conn, api: nestAPI}, nil
	}

	return nil, lastErr
}

func rtspConn(nestAPI *API, rawURL, projectID, deviceID string) (*RTSPClient, error) {
	rtspURL, err := nestAPI.GenerateRtspStream(projectID, deviceID)
	if err != nil {
		return nil, err
	}

	rtspClient := rtsp.NewClient(rtspURL)
	if err := rtspClient.Dial(); err != nil {
		return nil, err
	}
	if err := rtspClient.Describe(); err != nil {
		return nil, err
	}

	return &RTSPClient{conn: rtspClient, api: nestAPI}, nil
}

func (c *RTSPClient) GetMedias() []*core.Media {
	result := c.conn.GetMedias()
	return result
}

func (c *RTSPClient) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	return c.conn.GetTrack(media, codec)
}

func (c *RTSPClient) Start() error {
	c.scheduleRefresh()
	c.api.StartExtendStreamTimer()
	return c.conn.Start()
}

func (c *RTSPClient) Stop() error {
	c.cancelRefresh()
	c.api.StopRTSPStream()
	c.api.StopExtendStreamTimer()
	return c.conn.Stop()
}

func (c *RTSPClient) SetReconnectCallback(cb func()) {
	c.mu.Lock()
	c.reconnectCB = cb
	c.mu.Unlock()
}

func (c *RTSPClient) scheduleRefresh() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refreshTimer != nil {
		return
	}
	c.refreshTimer = time.AfterFunc(refreshBeforeCap, c.fireRefresh)
}

func (c *RTSPClient) cancelRefresh() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshFired = true
	if c.refreshTimer != nil {
		c.refreshTimer.Stop()
		c.refreshTimer = nil
	}
}

func (c *RTSPClient) fireRefresh() {
	c.mu.Lock()
	if c.refreshFired {
		c.mu.Unlock()
		return
	}
	c.refreshFired = true
	cb := c.reconnectCB
	c.mu.Unlock()
	if cb == nil {
		Log("warn", "[nest] refresh timer fired but no reconnect callback registered",
			"session", c.api.StreamSessionID)
		return
	}
	Log("info", "[nest] refresh timer fired — triggering proactive reconnect (rtsp)",
		"session", c.api.StreamSessionID,
		"age", refreshBeforeCap.String())
	go cb()
}

func (c *RTSPClient) MarshalJSON() ([]byte, error) {
	return c.conn.MarshalJSON()
}
