package core

import (
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

func TestSenser(t *testing.T) {
	recv := make(chan *Packet) // blocking receiver

	sender := NewSender(nil, &Codec{})
	sender.Output = func(packet *Packet) {
		recv <- packet
	}
	require.Equal(t, "new", sender.State())

	sender.Start()
	require.Equal(t, "connected", sender.State())

	sender.Input(&Packet{})
	sender.Input(&Packet{})

	require.Equal(t, 2, sender.Packets)
	require.Equal(t, 0, sender.Drops)

	// important to read one before close
	// because goroutine in Start() can run with nil chan
	// it's OK in real life, but bad for test
	_, ok := <-recv
	require.True(t, ok)

	sender.Close()
	require.Equal(t, "closed", sender.State())

	sender.Input(&Packet{})

	require.Equal(t, 2, sender.Packets)
	require.Equal(t, 1, sender.Drops)

	// read 2nd
	_, ok = <-recv
	require.True(t, ok)

	// read 3rd
	select {
	case <-recv:
		ok = true
	default:
		ok = false
	}
	require.False(t, ok)
}

// rtpPkt is a test helper for constructing a minimal rtp.Packet with the
// fields used by the continuity-rewrite logic. Other fields are zeroed.
func rtpPkt(ssrc uint32, seq uint16, ts uint32) *Packet {
	return &Packet{
		Header: rtp.Header{
			Version:        2,
			SSRC:           ssrc,
			SequenceNumber: seq,
			Timestamp:      ts,
		},
		Payload: []byte{0xFF},
	}
}

// captureChild is a child Node whose Input copies the packet header into
// a slice so tests can assert what downstream sees after Receiver
// rewriting.
func captureChild(out *[]rtp.Header) *Node {
	n := &Node{id: NewID()}
	n.Input = func(p *Packet) { *out = append(*out, p.Header) }
	return n
}

// TestReceiverRTPContinuity verifies the SSRC / seq / timestamp rewriting
// that keeps downstream RTP continuous across an upstream session swap
// (i.e. Receiver.Replace from Producer.reconnect). Without this, strict
// consumers like UniFi Protect treat the SSRC change as "stream ended"
// and stop recording even though the RTSP/TCP session is still alive.
func TestReceiverRTPContinuity(t *testing.T) {
	t.Run("non-RTP codec leaves packets untouched", func(t *testing.T) {
		// PayloadTypeRAW means the codec doesn't use RTP packets; the
		// rewrite path must be a no-op so raw-payload pipelines work
		// unchanged.
		var out []rtp.Header
		r := NewReceiver(nil, &Codec{Name: "RAW", PayloadType: PayloadTypeRAW})
		r.Node.AppendChild(captureChild(&out))

		p := rtpPkt(0x11111111, 1000, 50000)
		r.Input(p)

		require.Equal(t, uint32(0x11111111), out[0].SSRC, "SSRC must pass through unchanged for non-RTP codecs")
		require.Equal(t, uint16(1000), out[0].SequenceNumber)
		require.Equal(t, uint32(50000), out[0].Timestamp)
		require.False(t, r.rewriteRTP)
	})

	t.Run("stable SSRC + passthrough seq/ts for a single-session stream", func(t *testing.T) {
		var out []rtp.Header
		r := NewReceiver(nil, &Codec{Name: CodecH264, ClockRate: 90000, PayloadType: 96})
		r.Node.AppendChild(captureChild(&out))

		require.True(t, r.rewriteRTP)
		require.NotZero(t, r.outgoingSSRC)
		stableSSRC := r.outgoingSSRC

		// Three packets from the same upstream SSRC: outgoing SSRC must
		// be our chosen stable value; seq/ts pass through (offsets are
		// zero on the first session, so no recomputation occurs).
		r.Input(rtpPkt(0xAAAAAAAA, 100, 1000))
		r.Input(rtpPkt(0xAAAAAAAA, 101, 4000))
		r.Input(rtpPkt(0xAAAAAAAA, 102, 7000))

		require.Len(t, out, 3)
		for i, h := range out {
			require.Equal(t, stableSSRC, h.SSRC, "packet %d: outgoing SSRC must equal r.outgoingSSRC", i)
		}
		require.Equal(t, uint16(100), out[0].SequenceNumber)
		require.Equal(t, uint16(101), out[1].SequenceNumber)
		require.Equal(t, uint16(102), out[2].SequenceNumber)
		require.Equal(t, uint32(1000), out[0].Timestamp)
		require.Equal(t, uint32(4000), out[1].Timestamp)
		require.Equal(t, uint32(7000), out[2].Timestamp)
	})

	t.Run("upstream SSRC change anchors continuity (seq+1, ts forward)", func(t *testing.T) {
		var out []rtp.Header
		r := NewReceiver(nil, &Codec{Name: CodecH264, ClockRate: 90000, PayloadType: 96})
		r.Node.AppendChild(captureChild(&out))

		// Establish session 1: last outgoing seq=500, ts=90000.
		r.Input(rtpPkt(0xAAAAAAAA, 500, 90000))
		require.Equal(t, uint16(500), out[0].SequenceNumber)
		require.Equal(t, uint32(90000), out[0].Timestamp)

		// Force "elapsed time" to a known value by backdating
		// lastPacketTime. Without this the elapsed-clamp branch picks
		// 100 ms (1 ms..30 s sanity range), which is fine but harder
		// to assert against. 1 s at 90 kHz = 90 000 samples.
		r.lastPacketTime = time.Now().Add(-1 * time.Second)

		// Session 2: brand-new upstream SSRC, fresh seq/ts. The first
		// packet through must trigger the continuity recomputation.
		r.Input(rtpPkt(0xBBBBBBBB, 7000, 5000000))

		require.Len(t, out, 2)
		require.Equal(t, out[0].SSRC, out[1].SSRC, "outgoing SSRC must stay constant across upstream session change")
		// First packet of session 2 must be seq=lastOutgoingSeq+1=501.
		require.Equal(t, uint16(501), out[1].SequenceNumber,
			"sequence number must continue monotonically from the previous session")
		// Timestamp must advance by ~clockRate*elapsed (~90 000)
		// from the previous outgoing timestamp (90 000), so ~180 000.
		// Allow a small fudge for elapsed-time measurement jitter.
		require.InDelta(t, 180000, out[1].Timestamp, 5000,
			"timestamp must advance by ~clockRate × elapsed seconds")
	})

	t.Run("subsequent packets after session change preserve relative spacing", func(t *testing.T) {
		var out []rtp.Header
		r := NewReceiver(nil, &Codec{Name: CodecH264, ClockRate: 90000, PayloadType: 96})
		r.Node.AppendChild(captureChild(&out))

		r.Input(rtpPkt(0xAAAAAAAA, 500, 90000))
		r.lastPacketTime = time.Now().Add(-1 * time.Second)

		// New session: three packets with normal 30 fps spacing
		// (3 000 samples between frames at 90 kHz, seq +1 per packet).
		r.Input(rtpPkt(0xBBBBBBBB, 7000, 5000000))
		r.Input(rtpPkt(0xBBBBBBBB, 7001, 5003000))
		r.Input(rtpPkt(0xBBBBBBBB, 7002, 5006000))

		require.Len(t, out, 4)
		// Sequence must be 500, 501, 502, 503.
		require.Equal(t, uint16(501), out[1].SequenceNumber)
		require.Equal(t, uint16(502), out[2].SequenceNumber)
		require.Equal(t, uint16(503), out[3].SequenceNumber)
		// Timestamps must advance by exactly 3 000 between each of the
		// new-session frames (relative spacing of the upstream stream
		// is preserved).
		require.Equal(t, uint32(3000), out[2].Timestamp-out[1].Timestamp)
		require.Equal(t, uint32(3000), out[3].Timestamp-out[2].Timestamp)
	})

	t.Run("Replace transfers continuity state to successor", func(t *testing.T) {
		// downstream captures what the consumer ends up receiving. There's
		// only one consumer here — `old` has it attached as a child, and
		// after Replace that child is moved to `successor`, so packets
		// fed to either receiver land in the same slice.
		var downstream []rtp.Header
		old := NewReceiver(nil, &Codec{Name: CodecH264, ClockRate: 90000, PayloadType: 96})
		old.Node.AppendChild(captureChild(&downstream))

		old.Input(rtpPkt(0xAAAAAAAA, 1000, 100000))
		stableSSRC := old.outgoingSSRC
		require.Len(t, downstream, 1)
		require.Equal(t, stableSSRC, downstream[0].SSRC)
		require.Equal(t, uint16(1000), downstream[0].SequenceNumber)

		// Build the successor receiver as Producer.reconnect would: a
		// fresh Receiver from the new upstream connection. Its own
		// outgoingSSRC is random and different from old's — Replace must
		// overwrite it so downstream continuity holds.
		successor := NewReceiver(nil, &Codec{Name: CodecH264, ClockRate: 90000, PayloadType: 96})
		require.NotEqual(t, stableSSRC, successor.outgoingSSRC,
			"successor's pre-Replace SSRC must differ so we know the transfer happened")

		old.lastPacketTime = time.Now().Add(-2 * time.Second)
		old.Replace(successor)

		// After Replace, successor inherits old's outgoing-state.
		require.Equal(t, stableSSRC, successor.outgoingSSRC)
		require.Equal(t, uint16(1000), successor.lastOutgoingSeq)
		require.Equal(t, uint32(100000), successor.lastOutgoingTS)
		require.True(t, successor.initialized,
			"initialized must stay true so the first packet's SSRC mismatch is treated as a session change, not a cold start")
		require.Equal(t, uint32(0), successor.upstreamSSRC,
			"upstreamSSRC must be reset to 0 so any real new-session SSRC triggers the change branch")

		// MoveNode contract: old's children move to successor. old now
		// has no children of its own, so future packets fed to old go
		// nowhere — which is what Producer.reconnect relies on to make
		// the swap clean.
		require.Empty(t, old.childs, "old's children must have moved to successor")

		// First packet on successor from the new upstream session.
		// Continuity rewrite must produce: outgoing SSRC = stable
		// (unchanged), seq = lastOutgoingSeq + 1 = 1001, timestamp
		// forward (>= old's last + clockAdvance).
		successor.Input(rtpPkt(0xBBBBBBBB, 9000, 9000000))
		require.Len(t, downstream, 2)
		require.Equal(t, stableSSRC, downstream[1].SSRC,
			"outgoing SSRC must remain stable across the producer-reconnect swap")
		require.Equal(t, uint16(1001), downstream[1].SequenceNumber,
			"sequence number must continue from where old left off")
		require.Greater(t, downstream[1].Timestamp, uint32(100000),
			"timestamp must advance past where old left off")
		// Specifically: ~2 s × 90 000 Hz ≈ 180 000 advance from 100 000
		// → expect roughly 280 000. Allow some elapsed-time jitter.
		require.InDelta(t, 280000, downstream[1].Timestamp, 20000)
	})
}
