# Local Changes

Fork-local fixes against upstream `AlexxIT/go2rtc` from this debugging session.
All changes target compliance with the underlying protocols (RFC 2326 for
RTSP, ONVIF Profile S, Google Nest Device Access API) and observability
improvements. Each item is independently revertable.

Tested against UniFi Protect 7.1.60 + Google Nest (wired+WiFi) as the
primary client/source combination.

---

## ONVIF

### 1. ONVIF profile advertises AAC audio

- **Files:** `pkg/onvif/server.go`
- **Change:** `appendProfile` now emits `<tt:AudioSourceConfiguration>` and
  `<tt:AudioEncoderConfiguration>` (AAC, 48 kHz, 64 kbps) alongside the
  existing video configurations. New top-level builders
  `GetAudioSourcesResponse`, `GetAudioSourceConfigurationsResponse`,
  `GetAudioEncoderConfigurationsResponse` replace the previous empty stubs.
- **Why:** ONVIF clients that gate audio track setup on profile contents
  (UniFi Protect 3rd-party adoption is one) wouldn't issue RTSP SETUP for
  the audio track if no `AudioEncoderConfiguration` was present, even
  though the SDP from DESCRIBE included an audio track.
- **Impact:** UniFi Protect (and similarly strict ONVIF clients) now show
  the "Enable Audio" toggle and successfully negotiate the audio stream.

### 2. Singular AudioSourceConfiguration / AudioEncoderConfiguration

- **Files:** `pkg/onvif/server.go`
- **Change:** Added `GetAudioSourceConfigurationResponse` and
  `GetAudioEncoderConfigurationResponse` (singular variants). Added
  matching operation constants and `StaticResponse` routing.
- **Why:** ONVIF Media Service spec requires both singular and plural
  variants of every Configuration getter. Previously only the plural
  forms were implemented; clients probing the singular forms got 400.

### 3. Proper POSIX TZ format

- **Files:** `pkg/onvif/helpers.go`
- **Change:** Rewrote `GetPosixTZ` to produce IEEE 1003.1 POSIX TZ
  strings (`UTC0`, `EST5`, `JST-9`) instead of the previous non-standard
  `GMT±HH:MM` format. Sign convention now follows POSIX (reversed from
  ISO 8601). Zone name comes from Go's `time.Zone()` and respects the
  container's `$TZ` env var or `/etc/timezone`.
- **Why:** The previous output was non-conformant per ONVIF spec which
  references POSIX. Strict clients ignored it; clients that parsed it
  saw incorrect signs for east-of-UTC zones.

### 4. Imaging service stub

- **Files:** `pkg/onvif/server.go`, `pkg/onvif/envelope.go`,
  `internal/onvif/onvif.go`
- **Change:** Added imaging service to `GetCapabilities` and
  `GetServices` advertisements. Added stub responses for
  `GetImagingSettings`, `GetOptions`, `GetMoveOptions`, `GetStatus`, and
  `GetServiceCapabilities` (the latter routed by URL path since it's
  shared between Media and Imaging services). Added `timg:` namespace
  to the SOAP envelope prefix.
- **Why:** Some clients (Frigate, newer Synology Surveillance) probe the
  imaging service during adoption. Without it they logged warnings and
  occasionally failed adoption. Stub responses (empty configurations)
  cleanly declare "no imaging features supported".

---

## RTSP Server

### 5. GET_PARAMETER / SET_PARAMETER during session setup

- **Files:** `pkg/rtsp/server.go`, `pkg/rtsp/conn.go`
- **Change:** Added `MethodGetParameter` / `MethodSetParameter` constants
  and a case in the `Accept()` method switch that returns `200 OK` for
  both. Added these methods to the `Public:` header in the OPTIONS
  response.
- **Why:** RFC 2326 §10.8/§10.9 define these as keepalive methods.
  Before this fix the server fell through to `default` which returned
  an error and closed the TCP connection, killing any client (UniFi
  Protect, Axis cameras) that uses these as keepalives during setup.

### 6. GET_PARAMETER / SET_PARAMETER during steady state (after PLAY)

- **Files:** `pkg/rtsp/conn.go`
- **Change:** The `handleTCPData` steady-state read loop recognised
  `GET_`/`SET_` prefixes but only emitted responses for `OPTIONS`.
  Extended to respond `200 OK` to `GET_PARAMETER`, `SET_PARAMETER`, and
  `PAUSE` in steady state. Added `TEARDOWN` handler that acknowledges
  and closes the connection. Unknown methods now get
  `455 Method Not Valid In This State`.
- **Why:** This was the most impactful fix of the session. UniFi Protect
  sends `SET_PARAMETER` every ~30s as a keepalive. Previously the request
  was read and logged via the listener but no response was written, so
  UniFi's request timeout expired, UniFi tore down the TCP connection,
  and the consumer cycled into reconnect → mid-GOP green frames →
  "Camera Lost Wired Connection" indefinitely. This fix made UniFi
  sessions hold indefinitely.

### 7. RFC 2326 compliance pass

- **Files:** `pkg/rtsp/server.go`, `pkg/rtsp/conn.go`
- **Changes:**
  - Unknown methods now return `501 Not Implemented` with an `Allow:`
    header (was: connection-drop). Per RFC 2326 §11.4 and §1.4.
  - `PAUSE` returns `200 OK` rather than falling through to the unknown
    method case.
  - Session ID is now generated only on the **first** SETUP within a
    client session; subsequent SETUPs reuse the same ID. Per RFC 2326
    §10.4. Previously each SETUP generated a fresh random ID.

---

## WebRTC Source

### 8. PLI requests for ActiveProducer + immediate PLI on track setup

- **Files:** `pkg/webrtc/conn.go`
- **Change:** The existing periodic-PLI ticker (which fires every 2s for
  PassiveProducer video tracks) now also fires for `ModeActiveProducer`
  (e.g. Nest, where go2rtc dials out to a cloud SFU), at 10s intervals
  to balance keyframe freshness against upstream bandwidth/airtime. Also
  sends an immediate PLI when the track is first set up, so cold-start
  recovery doesn't have to wait for the first ticker fire.
- **Why:** WebRTC SDP doesn't usually include `sprop-parameter-sets`, so
  downstream RTSP consumers can't decode anything until an IDR (carrying
  inline SPS/PPS) arrives. Without PLI requests, IDR cadence is on the
  upstream sender's schedule — typically 30–60s for Google's Nest SFU.
  This caused extended pixelation after a go2rtc restart while UniFi
  reconnected and waited for Google's next scheduled keyframe. With the
  fix, cold-start recovery is typically <5s.

---

## Nest Source

### 9. Stream extension loops

- **Files:** `pkg/nest/api.go`
- **Change:** The `StartExtendStreamTimer` goroutine now loops, re-arming
  the timer after each successful `ExtendStream()`. Previously the
  goroutine waited for one timer fire, called `ExtendStream()` once, and
  exited — so the stream died at the second Google-side expiry (~10 min
  after connect).
- **Why:** Google Nest Device Access streams expire ~5 minutes after the
  last extension and must be re-extended before then. A single extension
  bought 5 more minutes but no further; the loop now keeps the stream
  alive indefinitely.

### 10. Race-free Stop + retry on extension failure

- **Files:** `pkg/nest/api.go`
- **Change:** Added an `extendStop` channel to the `API` struct.
  The extension goroutine uses `select` on both the timer and the stop
  channel so `StopExtendStreamTimer()` cleanly unblocks the goroutine.
  Timer reference is captured locally so a Stop-then-Start race can't
  produce a nil-pointer panic on `Reset()`. On `ExtendStream()` failure,
  the goroutine retries once after 10 seconds (interruptibly) before
  giving up. Added an `extendDelay` helper that clamps the next-wakeup
  duration to a 1-second minimum to prevent busy-looping if Google ever
  returns a stale expiry.
- **Why:** The previous implementation had a latent nil-deref race
  between `Stop()` and the goroutine's `Reset()` call, and no resilience
  against transient extension failures (a single Google 5xx would kill
  the stream until next reconnect).

---

## Observability

### 11. RTSP debug logs: source IP + User-Agent

- **Files:** `internal/rtsp/rtsp.go`
- **Change:** Added `remote=` (peer address) and `user_agent=` (from the
  first request on the connection) structured fields to:
  - `[rtsp] new consumer`
  - `[rtsp] new producer`
  - `[rtsp] handle <error>`
  - `[rtsp] disconnect`
  - `[rtsp]` consumer add-track error
- **Why:** When multiple clients connect to a single stream (UniFi from
  several IPs, an internal ffmpeg loop, an occasional VLC test), the
  default `[rtsp] new consumer stream=driveway` log line gave no way to
  tell which session was which. With the new fields, sessions are
  trivially greppable by `remote=192.168.5.1` or
  `user_agent=GStreamer/1.26.10`.

### 12. Structured logging across HTTP / WebSocket / ONVIF / MP4

- **Files:** `internal/onvif/onvif.go`, `internal/api/api.go`,
  `internal/mp4/mp4.go`, `internal/api/ws/ws.go`
- **Change:** Converted positional log lines to structured fields with
  `remote=`, `user_agent=`, and operation-specific context:
  - `[onvif] server request` (trace) — adds `remote`, `user_agent`, `op`
  - `[onvif] unsupported operation` (warn) — adds `remote`, `user_agent`, `op`
  - `[api] request` middleware (trace) — restructured with `method`,
    `url`, `remote`, `user_agent`
  - `[mp4] request` handler (trace) — restructured with `method`, `url`,
    `remote`, `user_agent`
  - `[api] ws upgrade` (error) — adds `remote`, `user_agent`
  - `[api] ws msg` (trace) — adds `remote`
- **Why:** Consistent observability across all client-facing surfaces.
  Same grep patterns work for any client interaction regardless of
  protocol.

---

## Suggested upstream PR grouping

If contributing back to AlexxIT/go2rtc, these changes group naturally
into five focused PRs:

1. **ONVIF improvements** — items 1, 2, 3, 4
2. **RTSP compliance + keepalives** — items 5, 6, 7
3. **Nest source robustness** — items 9, 10
4. **WebRTC PLI for active producers** — item 8
5. **Observability** — items 11, 12

Each PR is independent of the others (no cross-dependencies between
groups) and addresses a coherent area.

---

## Test coverage

Five new tests added in `pkg/onvif/onvif_test.go` covering the
ONVIF server-side changes:

- `TestGetPosixTZ` — pure-function test of the new POSIX TZ
  formatter. Covers UTC, west/east-of-UTC standard offsets,
  half-hour offsets (NST, IST), and the empty-zone-name fallback.
  Uses `time.FixedZone()` so test results are deterministic
  regardless of the host machine's TZ.
- `TestGetCapabilitiesResponse` — verifies all three services
  (Device, Media, Imaging) appear in the GetCapabilities response.
- `TestGetServicesResponse` — verifies all three WSDL namespaces
  and service URLs appear in the GetServices response.
- `TestGetProfilesResponseIncludesAudio` — regression guard for
  the AudioSourceConfiguration + AudioEncoderConfiguration blocks
  in `appendProfile`. Without these, UniFi Protect won't negotiate
  the audio track during RTSP SETUP.
- `TestImagingResponses` + `TestStaticResponseRoutesImagingOps`
  — verify imaging stubs use the `timg:` namespace and that the
  operation-name dispatcher routes correctly.

No new tests added for the other packages — RTSP / WebRTC / Nest
changes either touch connection-level code that needs heavy mocking
to exercise (PLI sending, GET_PARAMETER keepalives, Nest extension
loop), or pure-state changes (session ID consistency, 501 response)
that exercise via real-world integration testing. All existing
tests in `pkg/onvif/onvif_test.go`, `pkg/rtsp/rtsp_test.go`,
`pkg/rtsp/client_test.go` continue to pass.

## Build verification

All changes compile under Go 1.25 with no new dependencies. Built and
deployed continuously through the debugging session against the
production go2rtc Docker image (`docker/Dockerfile`, multi-stage build).
