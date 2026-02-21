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

func TestOnCongestionEventPerPacketAggregation(t *testing.T) {
	b := newTestBBR()
	warmupBBR(b, 100_000)

	if b.state != PROBE_BW {
		t.Skipf("BBR not in PROBE_BW after warmup (state=%d)", b.state)
	}
	if b.inRecovery {
		t.Fatal("should not be in recovery after warmup")
	}

	// Simulate 15 individual lost packets (per-packet calls, as quic-go does)
	// Each call: lostBytes=1350, priorInFlight=100_000
	// Total lost = 15*1350 = 20_250 bytes = ~20% of 100K inflight
	inflight := protocol.ByteCount(100_000)
	for i := 0; i < 15; i++ {
		b.OnCongestionEvent(protocol.PacketNumber(50+i), 1350, inflight)
	}

	// Should NOT have entered recovery yet — threshold evaluated at round boundary
	// (loss accumulates in roundLostBytes, evaluated in OnPacketAcked at round end)

	// Now trigger a round boundary via OnPacketAcked
	now := monotime.Now() + monotime.Time(20*time.Second)
	b.OnPacketSent(now, inflight, 200, 1350, true)
	now += monotime.Time(80 * time.Millisecond)
	b.OnPacketAcked(200, 1350, inflight, now)

	if !b.inRecovery {
		t.Error("should enter recovery after round boundary with 20% aggregate loss")
	}
}

func TestOnCongestionEventLightLossNoRecovery(t *testing.T) {
	b := newTestBBR()
	warmupBBR(b, 100_000)

	if b.state != PROBE_BW {
		t.Skipf("BBR not in PROBE_BW after warmup (state=%d)", b.state)
	}

	// Simulate 3 lost packets = 4050 bytes = ~4% of 100K inflight
	inflight := protocol.ByteCount(100_000)
	for i := 0; i < 3; i++ {
		b.OnCongestionEvent(protocol.PacketNumber(50+i), 1350, inflight)
	}

	// Trigger round boundary
	now := monotime.Now() + monotime.Time(20*time.Second)
	b.OnPacketSent(now, inflight, 200, 1350, true)
	now += monotime.Time(80 * time.Millisecond)
	b.OnPacketAcked(200, 1350, inflight, now)

	if b.inRecovery {
		t.Error("should NOT enter recovery on 4% aggregate loss (below 10% threshold)")
	}
}

func TestOnCongestionEventSkipsStartup(t *testing.T) {
	b := newTestBBR()
	// Stay in STARTUP
	if b.state != STARTUP {
		t.Fatal("expected STARTUP state")
	}

	// 50% loss
	for i := 0; i < 50; i++ {
		b.OnCongestionEvent(protocol.PacketNumber(i), 1350, 100_000)
	}

	if b.inRecovery {
		t.Error("should NOT enter recovery during STARTUP")
	}
}

func TestOnCongestionEventSkipsIfAlreadyRecovering(t *testing.T) {
	b := newTestBBR()
	warmupBBR(b, 100_000)

	b.inRecovery = true
	prevGain := b.pacing_gain

	// Heavy loss
	for i := 0; i < 20; i++ {
		b.OnCongestionEvent(protocol.PacketNumber(50+i), 1350, 100_000)
	}

	// pacing_gain should not change — already in recovery
	if b.pacing_gain != prevGain {
		t.Errorf("pacing_gain changed during existing recovery: %f -> %f", prevGain, b.pacing_gain)
	}
}

func TestOnCongestionEventLateLossWithShrinkingInflight(t *testing.T) {
	b := newTestBBR()
	warmupBBR(b, 100_000)

	if b.state != PROBE_BW {
		t.Skipf("BBR not in PROBE_BW after warmup (state=%d)", b.state)
	}

	// Early in round: 2 losses with high inflight (100K)
	b.OnCongestionEvent(50, 1350, 100_000)
	b.OnCongestionEvent(51, 1350, 100_000)

	// Late in round: 5 losses with drained inflight (10K)
	// With max inflight (100K): 7*1350/100K = 9.45% → below threshold
	for i := 0; i < 5; i++ {
		b.OnCongestionEvent(protocol.PacketNumber(52+i), 1350, 10_000)
	}

	// Trigger round boundary
	now := monotime.Now() + monotime.Time(20*time.Second)
	b.OnPacketSent(now, 10_000, 200, 1350, true)
	now += monotime.Time(80 * time.Millisecond)
	b.OnPacketAcked(200, 1350, 10_000, now)

	// 7*1350/100K = 9.45% < 10% → should NOT enter recovery
	if b.inRecovery {
		t.Error("should NOT enter recovery: 9.45% loss rate (max inflight=100K) is below 10% threshold")
	}
}

func TestOnCongestionEventZeroLossIgnored(t *testing.T) {
	b := newTestBBR()
	warmupBBR(b, 100_000)

	if b.state != PROBE_BW {
		t.Skipf("BBR not in PROBE_BW after warmup (state=%d)", b.state)
	}

	// ECN path calls OnCongestionEvent with lostBytes=0 (sent_packet_handler.go:452)
	// This should not pollute round loss accounting
	b.OnCongestionEvent(50, 0, 100_000)
	b.OnCongestionEvent(51, 0, 100_000)

	if b.roundLostBytes != 0 {
		t.Errorf("zero-loss events should not increment roundLostBytes, got %d", b.roundLostBytes)
	}
	if b.roundMaxInflight != 0 {
		t.Errorf("zero-loss events should not update roundMaxInflight, got %d", b.roundMaxInflight)
	}
}
