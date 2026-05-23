package nest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// captureLogs replaces the package-level Log hook with one that records
// every event into a slice. The slice is returned by-pointer so callers
// can assert against it after the unit-under-test runs; the previous Log
// implementation is restored on test cleanup.
//
// Recorded entries are "<level>|<msg>" — kv pairs are not captured here
// because the existing tests only assert on level/msg.
func captureLogs(t *testing.T) *[]string {
	t.Helper()
	var captured []string
	prev := Log
	Log = func(level, msg string, kv ...any) {
		captured = append(captured, level+"|"+msg)
	}
	t.Cleanup(func() { Log = prev })
	return &captured
}

// shortenExtendBackoff swaps the production 30s initial backoff for 1ms
// so tests don't wait minutes between retries. Reverted on cleanup.
func shortenExtendBackoff(t *testing.T) {
	t.Helper()
	prev := extendBackoffInitial
	extendBackoffInitial = time.Millisecond
	t.Cleanup(func() { extendBackoffInitial = prev })
}

// pointExtendURI redirects ExtendStream's outgoing requests to the given
// httptest server URL instead of Google's real SDM API. Reverted on
// cleanup. Tests that need to assert on the original URL behaviour should
// not call this.
func pointExtendURI(t *testing.T, serverURL string) {
	t.Helper()
	prev := extendURI
	extendURI = func(_, _ string) string { return serverURL }
	t.Cleanup(func() { extendURI = prev })
}

// newExtendAPI builds an API in a minimal state sufficient for
// ExtendStream — a token (any string, the test server ignores it),
// project/device IDs (only used for URI building, which the test
// hook overrides), and a session ID so the WebRTC branch is taken.
func newExtendAPI() *API {
	return &API{
		Token:           "test-token",
		StreamProjectID: "test-project",
		StreamDeviceID:  "test-device",
		StreamSessionID: "session-before-extend",
	}
}

// validExtendResponse is the minimal JSON body ExtendStream needs to
// parse a 200 response without erroring. The expires-at is intentionally
// far in the future so test runs aren't affected by clock skew.
const validExtendResponse = `{
  "results": {
    "expiresAt": "2099-12-31T23:59:59Z",
    "mediaSessionId": "session-after-extend",
    "streamExtensionToken": "ext-token-after",
    "streamToken": "stream-token-after"
  }
}`

// TestExtendStreamRetry covers the retry/back-off behaviour added in
// the 409/429-handling change. The 401-refresh path is covered
// indirectly by the existing refresh-token logic and not re-tested
// here because driving the refreshToken() path requires either a
// second mock OAuth endpoint or a cache pre-seed — out of scope for
// the retry-loop change being landed.
func TestExtendStreamRetry(t *testing.T) {
	shortenExtendBackoff(t)

	t.Run("200 on first attempt — no retry, session updated", func(t *testing.T) {
		var hits int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&hits, 1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(validExtendResponse))
		}))
		defer server.Close()
		pointExtendURI(t, server.URL)

		api := newExtendAPI()
		err := api.ExtendStream()
		require.NoError(t, err)
		require.Equal(t, int32(1), atomic.LoadInt32(&hits), "happy path must hit the endpoint exactly once")
		require.Equal(t, "session-after-extend", api.StreamSessionID,
			"successful extend must overwrite session ID with the new value from the response")
		require.Equal(t, "ext-token-after", api.StreamExtensionToken)
		require.Equal(t, "stream-token-after", api.StreamToken)
	})

	t.Run("429 then 200 — retries once after backoff, succeeds", func(t *testing.T) {
		var hits int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&hits, 1)
			if n == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(validExtendResponse))
		}))
		defer server.Close()
		pointExtendURI(t, server.URL)
		logs := captureLogs(t)

		api := newExtendAPI()
		start := time.Now()
		err := api.ExtendStream()
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Equal(t, int32(2), atomic.LoadInt32(&hits), "must call server twice: one 429 then one 200")
		require.Equal(t, "session-after-extend", api.StreamSessionID, "successful retry must still update session state")
		// At least one backoff interval must have elapsed (extendBackoffInitial = 1ms).
		require.GreaterOrEqual(t, elapsed, time.Millisecond, "must observe at least the configured backoff between attempts")
		// Confirm the retry was logged at warn level (the only event the loop emits for 429/409).
		var foundRetryLog bool
		for _, l := range *logs {
			if strings.HasPrefix(l, "warn|[nest] extend got transient status") {
				foundRetryLog = true
				break
			}
		}
		require.True(t, foundRetryLog, "expected a warn-level log for the 429 retry; got %v", *logs)
	})

	t.Run("409 then 200 — same retry path as 429", func(t *testing.T) {
		var hits int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&hits, 1)
			if n == 1 {
				w.WriteHeader(http.StatusConflict)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(validExtendResponse))
		}))
		defer server.Close()
		pointExtendURI(t, server.URL)

		api := newExtendAPI()
		err := api.ExtendStream()
		require.NoError(t, err)
		require.Equal(t, int32(2), atomic.LoadInt32(&hits))
		require.Equal(t, "session-after-extend", api.StreamSessionID)
	})

	t.Run("three consecutive 429s exhaust max retries and return error", func(t *testing.T) {
		var hits int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&hits, 1)
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer server.Close()
		pointExtendURI(t, server.URL)

		api := newExtendAPI()
		preSessionID := api.StreamSessionID
		err := api.ExtendStream()

		require.Error(t, err)
		require.Contains(t, err.Error(), "wrong status: 429",
			"after retries are exhausted, the loop should surface the last status as an error")
		require.Equal(t, int32(3), atomic.LoadInt32(&hits),
			"maxRetries is 3; should attempt exactly that many times before giving up")
		require.Equal(t, preSessionID, api.StreamSessionID,
			"failed extend must leave session state untouched")
	})

	t.Run("500 server error returns immediately — only 409/429/401 are retried", func(t *testing.T) {
		// 5xx is intentionally not in the retry list. The Google API returns
		// 5xx for genuine server problems where retrying without delay is
		// counterproductive; the producer-level reconnect machinery is the
		// right place to handle those, not this inner loop.
		var hits int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&hits, 1)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()
		pointExtendURI(t, server.URL)

		api := newExtendAPI()
		err := api.ExtendStream()

		require.Error(t, err)
		require.Contains(t, err.Error(), "wrong status: 500")
		require.Equal(t, int32(1), atomic.LoadInt32(&hits),
			"5xx must fail fast without consuming the retry budget")
	})

	t.Run("backoff doubles between retries", func(t *testing.T) {
		// Two consecutive 429s followed by a 200. With initial backoff =
		// 1ms (test override), the loop sleeps 1ms before retry 2 and 2ms
		// before retry 3. Asserting precise timing would be flaky on
		// shared CI; instead assert that elapsed time is at least the sum
		// of expected backoffs (1ms + 2ms = 3ms).
		var hits int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&hits, 1)
			if n <= 2 {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(validExtendResponse))
		}))
		defer server.Close()
		pointExtendURI(t, server.URL)

		api := newExtendAPI()
		start := time.Now()
		err := api.ExtendStream()
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Equal(t, int32(3), atomic.LoadInt32(&hits))
		// Lower bound: 1ms (first backoff) + 2ms (doubled) = 3ms. Practical
		// time.Sleep often rounds up by a few ms on Windows so this is a
		// safe floor without being flaky.
		require.GreaterOrEqual(t, elapsed, 3*time.Millisecond,
			"backoff doubling should produce at least 1ms + 2ms of cumulative sleep")
	})
}
