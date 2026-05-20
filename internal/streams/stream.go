package streams

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

type Stream struct {
	producers []*Producer
	consumers []core.Consumer
	mu        sync.Mutex
	pending   atomic.Int32
}

func NewStream(source any) *Stream {
	switch source := source.(type) {
	case string:
		s := &Stream{producers: []*Producer{NewProducer(source)}}
		s.linkProducers()
		return s
	case []string:
		s := new(Stream)
		for _, str := range source {
			s.producers = append(s.producers, NewProducer(str))
		}
		s.linkProducers()
		return s
	case []any:
		s := new(Stream)
		for _, src := range source {
			str, ok := src.(string)
			if !ok {
				log.Error().Msgf("[stream] NewStream: Expected string, got %v", src)
				continue
			}
			s.producers = append(s.producers, NewProducer(str))
		}
		s.linkProducers()
		return s
	case map[string]any:
		return NewStream(source["url"])
	case nil:
		return new(Stream)
	default:
		panic(core.Caller())
	}
}

// linkProducers sets each Producer's stream back-reference. Required for
// Producer.reconnect to be able to notify the parent Stream
// (kickConsumers) after a successful reconnect.
func (s *Stream) linkProducers() {
	for _, p := range s.producers {
		p.stream = s
	}
}

func (s *Stream) Sources() []string {
	sources := make([]string, 0, len(s.producers))
	for _, prod := range s.producers {
		sources = append(sources, prod.url)
	}
	return sources
}

func (s *Stream) SetSource(source string) {
	for _, prod := range s.producers {
		prod.SetSource(source)
	}
}

func (s *Stream) RemoveConsumer(cons core.Consumer) {
	_ = cons.Stop()

	s.mu.Lock()
	for i, consumer := range s.consumers {
		if consumer == cons {
			s.consumers = append(s.consumers[:i], s.consumers[i+1:]...)
			break
		}
	}
	s.mu.Unlock()

	s.stopProducers()
}

// remoteAddrer is the interface implemented by consumers whose underlying
// transport has a known remote address. Most go2rtc consumer types embed
// *core.Connection, which provides GetRemoteAddr() via promoted method —
// so this assertion succeeds for RTSP, WebRTC, etc.
type remoteAddrer interface {
	GetRemoteAddr() string
}

// kickGracePeriod is how long producers are held alive after kickConsumers
// fires so kicked clients (e.g. UniFi Protect) can reconnect without
// hitting a cold producer that has to spin back up. 30s easily covers
// UniFi's normal 1–3s reconnect window and tolerates cloud-API throttles
// (Google's Nest API briefly rate-limits successive WebRTC re-establishes).
// Exposed as a package variable so tests can shorten it.
var kickGracePeriod = 30 * time.Second

// kickConsumers disconnects all consumers attached to this stream.
// Called after a Producer reconnect when downstream consumers may be
// holding stale codec parameters (SPS/PPS) from the previous source
// session. By calling Stop() on each consumer, their underlying
// transports (e.g. RTSP TCP connections) close, prompting them to
// reconnect and re-DESCRIBE with the new producer's SDP.
//
// Consumers are not removed from s.consumers here — the natural
// close-path in each transport (e.g. tcpHandler in internal/rtsp)
// calls RemoveConsumer when its handler loop exits, keeping the list
// in sync.
//
// To keep producers alive during the reconnect window, this method
// bumps s.pending for kickGracePeriod. While pending is non-zero,
// stopProducers() short-circuits — so the producers we just
// reconnected stay attached and ready for the kicked consumers'
// near-immediate re-DESCRIBE. Without this, the cascade is
// kick → consumers gone → producers stop (no senders) → consumer
// reconnects to a cold producer → cold-start latency → consumer
// timeout, back-off cycle.
//
// reason is logged for observability; pass something specific like
// "producer reconnect: <scheme>".
func (s *Stream) kickConsumers(reason string) {
	s.mu.Lock()
	consumers := make([]core.Consumer, len(s.consumers))
	copy(consumers, s.consumers)
	s.mu.Unlock()

	if len(consumers) == 0 {
		return
	}

	// Collect remote addresses for the log line so a single grep on the
	// kick event tells you exactly which clients were notified.
	remotes := make([]string, 0, len(consumers))
	for _, cons := range consumers {
		if r, ok := cons.(remoteAddrer); ok {
			if addr := r.GetRemoteAddr(); addr != "" {
				remotes = append(remotes, addr)
			}
		}
	}

	log.Debug().
		Int("count", len(consumers)).
		Strs("remotes", remotes).
		Str("reason", reason).
		Msg("[streams] kicking consumers")

	// Hold producers alive during the grace window. See kickGracePeriod.
	s.pending.Add(1)
	time.AfterFunc(kickGracePeriod, func() {
		if s.pending.Add(-1) == 0 {
			s.stopProducers()
		}
	})

	for _, cons := range consumers {
		_ = cons.Stop()
	}
}

func (s *Stream) AddProducer(prod core.Producer) {
	producer := &Producer{conn: prod, state: stateExternal, url: "external", stream: s}
	s.mu.Lock()
	s.producers = append(s.producers, producer)
	s.mu.Unlock()
}

func (s *Stream) RemoveProducer(prod core.Producer) {
	s.mu.Lock()
	for i, producer := range s.producers {
		if producer.conn == prod {
			s.producers = append(s.producers[:i], s.producers[i+1:]...)
			break
		}
	}
	s.mu.Unlock()
}

func (s *Stream) stopProducers() {
	if s.pending.Load() > 0 {
		log.Trace().Msg("[streams] skip stop pending producer")
		return
	}

	s.mu.Lock()
producers:
	for _, producer := range s.producers {
		for _, track := range producer.receivers {
			if len(track.Senders()) > 0 {
				continue producers
			}
		}
		for _, track := range producer.senders {
			if len(track.Senders()) > 0 {
				continue producers
			}
		}
		producer.stop()
	}
	s.mu.Unlock()
}

func (s *Stream) MarshalJSON() ([]byte, error) {
	var info = struct {
		Producers []*Producer     `json:"producers"`
		Consumers []core.Consumer `json:"consumers"`
	}{
		Producers: s.producers,
		Consumers: s.consumers,
	}
	return json.Marshal(info)
}
