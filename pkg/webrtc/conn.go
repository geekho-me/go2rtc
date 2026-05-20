package webrtc

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

type Conn struct {
	core.Connection
	core.Listener

	Mode core.Mode `json:"mode"`

	pc *webrtc.PeerConnection

	offer  string
	closed core.Waiter
}

func NewConn(pc *webrtc.PeerConnection) *Conn {
	c := &Conn{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "webrtc",
			Transport:  pc,
		},
		pc: pc,
	}

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		// last candidate will be empty
		if candidate != nil {
			c.Fire(candidate)
		}
	})

	pc.OnDataChannel(func(channel *webrtc.DataChannel) {
		c.Fire(channel)
	})

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		if state != webrtc.ICEConnectionStateChecking {
			return
		}
		pc.SCTP().Transport().ICETransport().OnSelectedCandidatePairChange(
			func(pair *webrtc.ICECandidatePair) {
				// fix situation when candidate pair changes multiple times
				if i := strings.IndexByte(c.Protocol, '+'); i > 0 {
					c.Protocol = c.Protocol[:i]
				}
				c.Protocol += "+" + pair.Remote.Protocol.String()
				c.RemoteAddr = fmt.Sprintf(
					"%s:%d %s", sanitizeIP6(pair.Remote.Address), pair.Remote.Port, pair.Remote.Typ,
				)
				if pair.Remote.RelatedAddress != "" {
					c.RemoteAddr += fmt.Sprintf(
						" %s:%d", sanitizeIP6(pair.Remote.RelatedAddress), pair.Remote.RelatedPort,
					)
				}
			},
		)
	})

	pc.OnTrack(func(remote *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		media, codec := c.getMediaCodec(remote)
		if media == nil {
			return
		}

		track, err := c.GetTrack(media, codec)
		if err != nil {
			return
		}

		switch c.Mode {
		case core.ModePassiveProducer, core.ModeActiveProducer:
			// replace the theoretical list of codecs with the actual list of codecs
			if len(media.Codecs) > 1 {
				media.Codecs = []*core.Codec{codec}
			}
		}

		// Send PLI (Picture Loss Indication) to request keyframes from the
		// upstream WebRTC source. WebRTC SDP doesn't usually include
		// sprop-parameter-sets, so downstream RTSP consumers can't decode
		// anything until an IDR (carrying SPS/PPS inline) arrives. Without
		// PLI requests, the upstream cadence is on the sender's schedule —
		// typically 30-60s for live streams — which means new consumers see
		// extended pixelation after a connection or restart.
		//
		// PassiveProducer (e.g. a browser broadcasting in): 2s — low cost,
		// keeps tune-in latency tight for viewers.
		// ActiveProducer (e.g. Nest, where go2rtc dials out to a cloud SFU):
		// 10s — slower because keyframes cost WiFi airtime on the camera and
		// upstream bandwidth on the SFU. Still fast enough that any consumer
		// recovers from a restart within ~10s.
		if remote.Kind() == webrtc.RTPCodecTypeVideo &&
			(c.Mode == core.ModePassiveProducer || c.Mode == core.ModeActiveProducer) {
			interval := time.Second * 2
			if c.Mode == core.ModeActiveProducer {
				interval = time.Second * 10
			}
			go func() {
				pkts := []rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(remote.SSRC())}}
				// Immediate PLI so cold-start tune-in doesn't have to wait
				// for the first ticker fire.
				if err := pc.WriteRTCP(pkts); err != nil {
					return
				}
				for range time.NewTicker(interval).C {
					if err := pc.WriteRTCP(pkts); err != nil {
						return
					}
				}
			}()
		}

		// For ActiveProducer sources (e.g. Nest, where go2rtc dials out to
		// a cloud SFU), set a read deadline on the VIDEO track so a silent
		// upstream — SFU drops media but keeps the WebRTC peer connection
		// technically open — is detected and surfaces as a connection
		// error. Without this, pion's TrackRemote.Read() blocks indefinitely
		// on silence and the producer never restarts.
		//
		// Audio tracks are deliberately exempt: WebRTC Opus encoders
		// commonly use DTX (discontinuous transmission), so legitimate
		// silence in the room produces no audio packets at all. Applying a
		// deadline to audio tracks would tear down the peer connection
		// every time the scene went acoustically quiet, killing the video
		// stream that was working fine.
		//
		// Skipped entirely for PassiveProducer (browser → go2rtc) because
		// inbound silence (user mute, paused webcam) is also common there.
		//
		// 20s gives enough margin for PLI/IDR roundtrips (PLI ticker is
		// 10s) while still catching terminal silences within ~25s
		// end-to-end recovery.
		useReadDeadline := c.Mode == core.ModeActiveProducer &&
			remote.Kind() == webrtc.RTPCodecTypeVideo
		const readDeadline = 20 * time.Second

		for {
			b := make([]byte, ReceiveMTU)
			if useReadDeadline {
				_ = remote.SetReadDeadline(time.Now().Add(readDeadline))
			}
			n, _, err := remote.Read(b)
			if err != nil {
				if useReadDeadline {
					// Tear down the peer connection so OnConnectionStateChange
					// fires and the producer enters its reconnect path.
					_ = pc.Close()
				}
				return
			}

			c.Recv += n

			packet := &rtp.Packet{}
			if err := packet.Unmarshal(b[:n]); err != nil {
				return
			}

			if len(packet.Payload) == 0 {
				continue
			}

			track.WriteRTP(packet)
		}
	})

	// OK connection:
	// 15:01:46 ICE connection state changed: checking
	// 15:01:46 peer connection state changed: connected
	// 15:01:54 peer connection state changed: disconnected
	// 15:02:20 peer connection state changed: failed
	//
	// Fail connection:
	// 14:53:08 ICE connection state changed: checking
	// 14:53:39 peer connection state changed: failed
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		c.Fire(state)

		switch state {
		case webrtc.PeerConnectionStateConnected:
			for _, sender := range c.Senders {
				sender.Start()
			}
		case webrtc.PeerConnectionStateDisconnected, webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			// disconnect event comes earlier, than failed
			// but it comes only for success connections
			_ = c.Close()
		}
	})

	return c
}

func (c *Conn) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Connection)
}

func (c *Conn) Close() error {
	c.closed.Done(nil)
	return c.pc.Close()
}

func (c *Conn) AddCandidate(candidate string) error {
	// pion uses only candidate value from json/object candidate struct
	return c.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: candidate})
}

func (c *Conn) GetSenderTrack(mid string) *Track {
	if tr := c.getTranseiver(mid); tr != nil {
		if s := tr.Sender(); s != nil {
			if t := s.Track().(*Track); t != nil {
				return t
			}
		}
	}
	return nil
}

func (c *Conn) getTranseiver(mid string) *webrtc.RTPTransceiver {
	for _, tr := range c.pc.GetTransceivers() {
		if tr.Mid() == mid {
			return tr
		}
	}
	return nil
}

func (c *Conn) getMediaCodec(remote *webrtc.TrackRemote) (*core.Media, *core.Codec) {
	for _, tr := range c.pc.GetTransceivers() {
		// search Transeiver for this TrackRemote
		if tr.Receiver() == nil || tr.Receiver().Track() != remote {
			continue
		}

		// search Media for this MID
		for _, media := range c.Medias {
			if media.ID != tr.Mid() || media.Direction != core.DirectionRecvonly {
				continue
			}

			// search codec for this PayloadType
			for _, codec := range media.Codecs {
				if codec.PayloadType != uint8(remote.PayloadType()) {
					continue
				}
				return media, codec
			}
		}
	}

	// fix moment when core.ModePassiveProducer or core.ModeActiveProducer
	// sends new codec with new payload type to same media
	// check GetTrack
	panic(core.Caller())

	return nil, nil
}

func sanitizeIP6(host string) string {
	if strings.IndexByte(host, ':') > 0 {
		return "[" + host + "]"
	}
	return host
}
