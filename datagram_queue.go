package quic

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/quic-go/quic-go/internal/utils"
	"github.com/quic-go/quic-go/internal/utils/ringbuffer"
	"github.com/quic-go/quic-go/internal/wire"
)

// ErrDatagramSendTimeout is returned when a datagram cannot be queued
// within the send timeout. Per RFC 9221, QUIC datagrams are unreliable
// and should be dropped rather than blocking the sender indefinitely.
var ErrDatagramSendTimeout = errors.New("datagram send timeout: queue full")

const (
	defaultDatagramSendQueueLen = 1024
	defaultDatagramRcvQueueLen  = 1024
)

type datagramQueue struct {
	sendMx    sync.Mutex
	sendQueue ringbuffer.RingBuffer[*wire.DatagramFrame]
	sent      chan struct{} // used to notify Add that a datagram was dequeued

	rcvMx    sync.Mutex
	rcvQueue ringbuffer.RingBuffer[[]byte]
	rcvd     chan struct{} // used to notify Receive that a new datagram was received

	maxSendLen int
	maxRcvLen  int

	closeErr error
	closed   chan struct{}

	hasData func()

	logger utils.Logger
}

func newDatagramQueue(hasData func(), logger utils.Logger, sendLen, rcvLen int) *datagramQueue {
	if sendLen <= 0 {
		sendLen = defaultDatagramSendQueueLen
	}
	if rcvLen <= 0 {
		rcvLen = defaultDatagramRcvQueueLen
	}
	q := &datagramQueue{
		maxSendLen: sendLen,
		maxRcvLen:  rcvLen,
		hasData:    hasData,
		rcvd:       make(chan struct{}, 1),
		sent:       make(chan struct{}, 1),
		closed:     make(chan struct{}),
		logger:     logger,
	}
	q.rcvQueue.Init(rcvLen)
	return q
}

// Add queues a new DATAGRAM frame for sending.
// Up to 32 DATAGRAM frames will be queued.
// If the queue is full, Add waits up to 500ms for space. If no space opens
// (e.g., congestion control blocks packet packing), returns ErrDatagramSendTimeout.
// QUIC datagrams are unreliable (RFC 9221) — blocking forever is incorrect.
func (h *datagramQueue) Add(f *wire.DatagramFrame) error {
	h.sendMx.Lock()

	for {
		if h.sendQueue.Len() < h.maxSendLen {
			h.sendQueue.PushBack(f)
			h.sendMx.Unlock()
			h.hasData()
			return nil
		}
		select {
		case <-h.sent: // drain the queue so we don't loop immediately
		default:
		}
		h.sendMx.Unlock()
		select {
		case <-h.closed:
			return h.closeErr
		case <-h.sent:
		case <-time.After(500 * time.Millisecond):
			return ErrDatagramSendTimeout
		}
		h.sendMx.Lock()
	}
}

// Peek gets the next DATAGRAM frame for sending.
// If actually sent out, Pop needs to be called before the next call to Peek.
func (h *datagramQueue) Peek() *wire.DatagramFrame {
	h.sendMx.Lock()
	defer h.sendMx.Unlock()
	if h.sendQueue.Empty() {
		return nil
	}
	return h.sendQueue.PeekFront()
}

func (h *datagramQueue) Pop() {
	h.sendMx.Lock()
	defer h.sendMx.Unlock()
	_ = h.sendQueue.PopFront()
	select {
	case h.sent <- struct{}{}:
	default:
	}
}

// HandleDatagramFrame handles a received DATAGRAM frame.
func (h *datagramQueue) HandleDatagramFrame(f *wire.DatagramFrame) {
	data := make([]byte, len(f.Data))
	copy(data, f.Data)
	var queued bool
	h.rcvMx.Lock()
	if h.rcvQueue.Len() < h.maxRcvLen {
		h.rcvQueue.PushBack(data)
		queued = true
		select {
		case h.rcvd <- struct{}{}:
		default:
		}
	}
	h.rcvMx.Unlock()
	if !queued && h.logger.Debug() {
		h.logger.Debugf("Discarding received DATAGRAM frame (%d bytes payload)", len(f.Data))
	}
}

// Receive gets a received DATAGRAM frame.
func (h *datagramQueue) Receive(ctx context.Context) ([]byte, error) {
	for {
		h.rcvMx.Lock()
		if !h.rcvQueue.Empty() {
			data := h.rcvQueue.PopFront()
			h.rcvMx.Unlock()
			return data, nil
		}
		h.rcvMx.Unlock()
		select {
		case <-h.rcvd:
			continue
		case <-h.closed:
			return nil, h.closeErr
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (h *datagramQueue) CloseWithError(e error) {
	h.closeErr = e
	close(h.closed)
}
