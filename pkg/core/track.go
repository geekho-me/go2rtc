package core

import (
	"encoding/json"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/pion/rtp"
)

var ErrCantGetTrack = errors.New("can't get track")

type Receiver struct {
	Node

	// Deprecated: should be removed
	Media *Media `json:"-"`
	// Deprecated: should be removed
	ID byte `json:"-"` // Channel for RTSP, PayloadType for MPEG-TS

	Bytes   int `json:"bytes,omitempty"`
	Packets int `json:"packets,omitempty"`

	// RTP continuity rewriting state. See NewReceiver for the rationale
	// and Replace for the swap-time transfer logic. Package-private so
	// the rewrite contract is fully owned by this package; callers see
	// only continuous RTP downstream.
	outgoingSSRC    uint32
	lastOutgoingSeq uint16
	lastOutgoingTS  uint32
	lastPacketTime  time.Time
	upstreamSSRC    uint32
	seqOffset       int32
	tsOffset        int64
	initialized     bool
	rewriteRTP      bool
}

func NewReceiver(media *Media, codec *Codec) *Receiver {
	r := &Receiver{
		Node:  Node{id: NewID(), Codec: codec},
		Media: media,
	}

	// For RTP-based codecs, normalize outgoing RTP so downstream sees
	// one continuous stream across upstream session swaps
	// (Receiver.Replace, triggered by Producer.reconnect — including the
	// proactive reconnect on lifetime-limited sources like Nest WebRTC's
	// ~60 min cap).
	//
	// Without this, every reconnect produces a fresh upstream session
	// with a new SSRC, fresh sequence numbers, and a new RTP timestamp
	// clock. Strict consumers (UniFi Protect 7.1.69 confirmed) interpret
	// the SSRC change as "the stream I was recording has ended" and stop
	// writing to disk — even though the RTSP/TCP session is still alive
	// and packets keep arriving. With rewriting:
	//   - outgoingSSRC stays constant for the life of this Receiver and
	//     propagates through Replace to its successors
	//   - sequence numbers continue monotonically from where we left off
	//   - timestamps advance by an amount roughly matching the wall-clock
	//     time elapsed during the reconnect (clock-rate × elapsed)
	// so the swap is invisible to RTP-layer logic in consumers.
	if codec != nil && codec.IsRTP() {
		r.rewriteRTP = true
		// Random 32-bit SSRC. Collision probability across a single
		// stream's consumers is negligible (~1 in 4 billion).
		r.outgoingSSRC = rand.Uint32()
	}

	r.Input = func(packet *Packet) {
		if r.rewriteRTP {
			now := time.Now()
			// SSRC change signals either the cold start of this receiver
			// (Packets == 0) or an upstream session swap mid-stream
			// (initialized && new SSRC). The cold-start case adopts the
			// upstream SSRC silently; the mid-stream case computes
			// continuity offsets so downstream sequence/timestamp stay
			// monotonic from where we left off.
			if packet.SSRC != r.upstreamSSRC {
				if r.initialized {
					// Estimate how much RTP clock time should appear to
					// have elapsed across the swap. Use wall-clock
					// elapsed × codec ClockRate, clamped to a sane
					// range so a long pause or a clock skew doesn't
					// produce an enormous timestamp jump. For typical
					// proactive reconnects this is ~2 s × 90 kHz =
					// 180 000 samples (video) or ~2 s × 48 kHz =
					// 96 000 (Opus audio).
					elapsed := now.Sub(r.lastPacketTime)
					if elapsed < time.Millisecond || elapsed > 30*time.Second {
						elapsed = 100 * time.Millisecond
					}
					clockRate := uint32(90000)
					if r.Codec != nil && r.Codec.ClockRate > 0 {
						clockRate = r.Codec.ClockRate
					}
					clockAdvance := int64(elapsed.Seconds() * float64(clockRate))
					if clockAdvance < 1 {
						clockAdvance = 1
					}

					r.seqOffset = int32(r.lastOutgoingSeq+1) - int32(packet.SequenceNumber)
					r.tsOffset = int64(r.lastOutgoingTS) + clockAdvance - int64(packet.Timestamp)
				}
				r.upstreamSSRC = packet.SSRC
				r.initialized = true
			}

			if r.seqOffset != 0 {
				packet.SequenceNumber = uint16(int32(packet.SequenceNumber) + r.seqOffset)
			}
			if r.tsOffset != 0 {
				packet.Timestamp = uint32(int64(packet.Timestamp) + r.tsOffset)
			}
			packet.SSRC = r.outgoingSSRC

			r.lastOutgoingSeq = packet.SequenceNumber
			r.lastOutgoingTS = packet.Timestamp
			r.lastPacketTime = now
		}

		r.Bytes += len(packet.Payload)
		r.Packets++
		for _, child := range r.childs {
			child.Input(packet)
		}
	}
	return r
}

// Deprecated: should be removed
func (r *Receiver) WriteRTP(packet *rtp.Packet) {
	r.Input(packet)
}

// Deprecated: should be removed
func (r *Receiver) Senders() []*Sender {
	if len(r.childs) > 0 {
		return []*Sender{{}}
	} else {
		return nil
	}
}

// Deprecated: should be removed
func (r *Receiver) Replace(target *Receiver) {
	// Transfer outgoing RTP continuity state to the successor receiver so
	// downstream consumers see one uninterrupted stream across the swap.
	// target.Input's closure captures target by pointer, so updating these
	// fields here is visible to the next Input call without any further
	// wiring. Only applies when both ends are RTP — for raw-codec
	// receivers (PayloadTypeRAW) we leave packets untouched.
	if r.rewriteRTP && target.rewriteRTP {
		target.outgoingSSRC = r.outgoingSSRC
		target.lastOutgoingSeq = r.lastOutgoingSeq
		target.lastOutgoingTS = r.lastOutgoingTS
		target.lastPacketTime = r.lastPacketTime
		// initialized = true (so the new-SSRC branch in Input recognises
		// this as a session change, not a cold start) but upstreamSSRC =
		// 0 (so any real upstream SSRC triggers the change branch). On
		// the next packet, Input computes seq/ts offsets anchored on the
		// inherited lastOutgoingSeq / lastOutgoingTS.
		//
		// Cold-start case (first packet through a brand-new receiver
		// that was never Replaced) still works: initialized is the
		// zero-value false there, so the offset-compute branch is
		// skipped and packets pass through with only SSRC rewriting.
		target.initialized = true
		target.upstreamSSRC = 0
		target.seqOffset = 0
		target.tsOffset = 0
	}

	MoveNode(&target.Node, &r.Node)
}

func (r *Receiver) Close() {
	r.Node.Close()
}

type Sender struct {
	Node

	// Deprecated:
	Media *Media `json:"-"`
	// Deprecated:
	Handler HandlerFunc `json:"-"`

	Bytes   int `json:"bytes,omitempty"`
	Packets int `json:"packets,omitempty"`
	Drops   int `json:"drops,omitempty"`

	buf  chan *Packet
	done chan struct{}
}

func NewSender(media *Media, codec *Codec) *Sender {
	var bufSize uint16

	if GetKind(codec.Name) == KindVideo {
		if codec.IsRTP() {
			// in my tests 40Mbit/s 4K-video can generate up to 1500 items
			// for the h264.RTPDepay => RTPPay queue
			bufSize = 4096
		} else {
			bufSize = 64
		}
	} else {
		bufSize = 128
	}

	buf := make(chan *Packet, bufSize)
	s := &Sender{
		Node:  Node{id: NewID(), Codec: codec},
		Media: media,
		buf:   buf,
	}
	s.Input = func(packet *Packet) {
		s.mu.Lock()
		// unblock write to nil chan - OK, write to closed chan - panic
		select {
		case s.buf <- packet:
			s.Bytes += len(packet.Payload)
			s.Packets++
		default:
			s.Drops++
		}
		s.mu.Unlock()
	}
	s.Output = func(packet *Packet) {
		s.Handler(packet)
	}
	return s
}

// Deprecated: should be removed
func (s *Sender) HandleRTP(parent *Receiver) {
	s.WithParent(parent)
	s.Start()
}

// Deprecated: should be removed
func (s *Sender) Bind(parent *Receiver) {
	s.WithParent(parent)
}

func (s *Sender) WithParent(parent *Receiver) *Sender {
	s.Node.WithParent(&parent.Node)
	return s
}

func (s *Sender) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.buf == nil || s.done != nil {
		return
	}
	s.done = make(chan struct{})

	// pass buf directly so that it's impossible for buf to be nil
	go func(buf chan *Packet) {
		for packet := range buf {
			s.Output(packet)
		}
		close(s.done)
	}(s.buf)
}

func (s *Sender) Wait() {
	if done := s.done; done != nil {
		<-done
	}
}

func (s *Sender) State() string {
	if s.buf == nil {
		return "closed"
	}
	if s.done == nil {
		return "new"
	}
	return "connected"
}

func (s *Sender) Close() {
	// close buffer if exists
	s.mu.Lock()
	if s.buf != nil {
		close(s.buf) // exit from for range loop
		s.buf = nil  // prevent writing to closed chan
	}
	s.mu.Unlock()

	s.Node.Close()
}

func (r *Receiver) MarshalJSON() ([]byte, error) {
	v := struct {
		ID      uint32   `json:"id"`
		Codec   *Codec   `json:"codec"`
		Childs  []uint32 `json:"childs,omitempty"`
		Bytes   int      `json:"bytes,omitempty"`
		Packets int      `json:"packets,omitempty"`
	}{
		ID:      r.Node.id,
		Codec:   r.Node.Codec,
		Bytes:   r.Bytes,
		Packets: r.Packets,
	}
	for _, child := range r.childs {
		v.Childs = append(v.Childs, child.id)
	}
	return json.Marshal(v)
}

func (s *Sender) MarshalJSON() ([]byte, error) {
	v := struct {
		ID      uint32 `json:"id"`
		Codec   *Codec `json:"codec"`
		Parent  uint32 `json:"parent,omitempty"`
		Bytes   int    `json:"bytes,omitempty"`
		Packets int    `json:"packets,omitempty"`
		Drops   int    `json:"drops,omitempty"`
	}{
		ID:      s.Node.id,
		Codec:   s.Node.Codec,
		Bytes:   s.Bytes,
		Packets: s.Packets,
		Drops:   s.Drops,
	}
	if s.parent != nil {
		v.Parent = s.parent.id
	}
	return json.Marshal(v)
}
