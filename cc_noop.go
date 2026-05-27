package quic

// noopCC is a no-op congestion controller that allows unlimited sending.
// Used for transports where an outer TCP already provides congestion control
// (e.g., cdnws: QUIC over WebSocket over CDN TCP).

import (
	"math"
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
)

var _ SendAlgorithmWithDebugInfos = (*noopCC)(nil)

type noopCC struct{}

// NewNoopCC returns a SendAlgorithmWithDebugInfos that never throttles.
func NewNoopCC() SendAlgorithmWithDebugInfos { return &noopCC{} }

func (n *noopCC) TimeUntilSend(protocol.ByteCount) monotime.Time { return monotime.Time(0) }
func (n *noopCC) HasPacingBudget(monotime.Time) bool             { return true }
func (n *noopCC) OnPacketSent(monotime.Time, protocol.ByteCount, protocol.PacketNumber, protocol.ByteCount, bool) {
}
func (n *noopCC) CanSend(protocol.ByteCount) bool { return true }
func (n *noopCC) MaybeExitSlowStart()              {}
func (n *noopCC) OnPacketAcked(protocol.PacketNumber, protocol.ByteCount, protocol.ByteCount, monotime.Time) {
}
func (n *noopCC) OnCongestionEvent(protocol.PacketNumber, protocol.ByteCount, protocol.ByteCount) {}
func (n *noopCC) OnRetransmissionTimeout(bool)                                                     {}
func (n *noopCC) SetMaxDatagramSize(protocol.ByteCount)                                            {}
func (n *noopCC) ResetForStall()                                                                   {}
func (n *noopCC) SetMinRTT(func() time.Duration)                                                   {}
func (n *noopCC) InSlowStart() bool                                                                { return false }
func (n *noopCC) InRecovery() bool                                                                 { return false }
func (n *noopCC) GetCongestionWindow() protocol.ByteCount                                          { return protocol.ByteCount(math.MaxInt64) }
