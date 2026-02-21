package congestion

import (
	"testing"
	"time"

	"github.com/quic-go/quic-go/internal/monotime"
	"github.com/quic-go/quic-go/internal/protocol"
)

func newTestBBR() *BBRv1Sender {
	b := NewBBRv1Sender(1350)
	b.SetMinRTT(func() time.Duration { return 80 * time.Millisecond })
	return b
}

// warmupBBR sends enough ACKs to exit STARTUP and enter PROBE_BW.
func warmupBBR(b *BBRv1Sender, bw protocol.ByteCount) {
	now := monotime.Now()
	rtt := b.minRTT()
	for i := 0; i < 20; i++ {
		pktNum := protocol.PacketNumber(i)
		b.OnPacketSent(now, bw, pktNum, 1350, true)
		now += monotime.Time(rtt)
		b.OnPacketAcked(pktNum, 1350, bw, now)
	}
	// Force into PROBE_BW if warmup left us in DRAIN
	// (DRAIN exits when priorInFlight < bdp, which may not happen in warmup)
	if b.state == DRAIN {
		b.entry_PROBE_BW()
	}
}

func TestMaxBandwidthPromotionOnly(t *testing.T) {
	b := newTestBBR()
	warmupBBR(b, 100_000)

	if b.state != PROBE_BW {
		t.Fatalf("expected PROBE_BW, got state=%d", b.state)
	}

	// Fill the bandwidth window with a known high value for a clean baseline
	highBW := protocol.ByteCount(500_000) // 500 KB/s
	for i := range bw_win {
		b.latelybandwidth[i] = highBW
	}
	b.update_bandwidth_filter()

	prevMax := b.maxBandwidth
	if prevMax != highBW {
		t.Fatalf("setup failed: maxBandwidth=%d, want %d", prevMax, highBW)
	}

	// Deliver one ACK with a low delivery rate but high priorInFlight (not app-limited).
	// On buggy code: the condition (!app_limited && delivery_rate > 0) triggers a
	// downward overwrite of maxBandwidth to the low delivery_rate.
	now := monotime.Now() + monotime.Time(10*time.Second)
	highInflight := protocol.ByteCount(1_000_000) // well above bdp → not app-limited
	b.OnPacketSent(now, highInflight, 100, 1350, true)
	now += monotime.Time(80 * time.Millisecond)
	b.OnPacketAcked(100, 1350, highInflight, now)

	// maxBandwidth should NOT have decreased
	if b.maxBandwidth < prevMax {
		t.Errorf("maxBandwidth decreased from %d to %d — promotion-only violated", prevMax, b.maxBandwidth)
	}
}
