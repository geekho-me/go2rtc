# ONVIF

## ONVIF Client

[`new in v1.5.0`](https://github.com/AlexxIT/go2rtc/releases/tag/v1.5.0)

The source is not very useful if you already know RTSP and snapshot links for your camera. But it can be useful if you don't.

**WebUI > Add** webpage supports ONVIF autodiscovery. Your server must be on the same subnet as the camera. If you use Docker, you must use "network host".

```yaml
streams:
  dahua1: onvif://admin:password@192.168.1.123
  reolink1: onvif://admin:password@192.168.1.123:8000
  tapo1: onvif://admin:password@192.168.1.123:2020
```

## ONVIF Server

A regular camera has a single video source (`GetVideoSources`) and two profiles (`GetProfiles`).

Go2rtc has one video source and one profile per stream.

### Security: ONVIF server does not authenticate requests

The ONVIF server built into go2rtc does **not** verify any
credentials on incoming requests. The standard ONVIF
authentication mechanism — `wsse:UsernameToken` carried in the
SOAP `Security` header — is parsed by client tools that talk to
go2rtc, but the server-side handler ignores it and dispatches
every operation regardless of who's calling (or whether they
sent any credentials at all).

This is upstream behaviour, not specific to this fork. Operators
should be aware of what it does and doesn't expose:

- **Stream URLs are still protected by their own auth.** An
  attacker who calls `GetStreamUri` will get back an RTSP URL,
  but they still need the camera-side or go2rtc-side RTSP
  credentials to actually open the stream.
- **Camera metadata is exposed to anyone who can reach the
  endpoint.** Stream enumeration, video encoder configuration,
  system date/time, and other introspection operations
  (`GetProfiles`, `GetVideoEncoderConfiguration`,
  `GetSystemDateAndTime`, etc.) return real data to
  unauthenticated callers. Treat this as unauthenticated
  information disclosure about your camera setup.
- **The credentials prompted for by NVR adoption tools are
  decorative.** UniFi Protect, Home Assistant, etc. will ask for
  a username and password when adding go2rtc as a 3rd-party
  ONVIF camera; both fields can be left blank or set to any value
  and the adoption will succeed.

#### Recommended mitigation

Treat the go2rtc API port (default `1984`, ONVIF endpoints
served at `/onvif/*`) as **trusted-network only**:

- Bind to a private interface rather than `0.0.0.0` unless your
  network is segmented.
- Restrict access via firewall or Docker network so only known
  clients (your NVR, your Home Assistant instance) can reach it.
- Do **not** expose go2rtc to the public internet without a
  reverse proxy that enforces its own authentication.

These are general hardening practices that also benefit the
WebUI, the JSON API, and the RTSP server — the ONVIF endpoint
just makes the need explicit.

#### Upstream fix in progress

An upstream pull request — [AlexxIT/go2rtc#2231][pr2231] — proposes
adding WS-Security `UsernameToken` validation to the ONVIF
server, with dedicated `onvif.username` / `onvif.password` config
keys (separate from the existing HTTP Basic auth used by the
WebUI / JSON API) and an exception for `GetSystemDateAndTime`
so clients can compute clock skew before generating password
digests. The PR is open as of writing; this fork does **not**
carry it. If you need authenticated ONVIF specifically and don't
want to wait for upstream merge, that PR is the reference
implementation to track.

[pr2231]: https://github.com/AlexxIT/go2rtc/pull/2231

## Tested clients

Go2rtc works as ONVIF server:

- Happytime onvif client (windows)
- Home Assistant ONVIF integration (linux)
- Onvier (android)
- ONVIF Device Manager (windows)

PS. Supports only TCP transport for RTSP protocol. UDP and HTTP transports - unsupported yet.

## Tested cameras

Go2rtc works as ONVIF client:

- Dahua IPC-K42
- OpenIPC
- Reolink RLC-520A
- TP-Link Tapo TC60
