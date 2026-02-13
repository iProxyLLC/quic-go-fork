package quic

import "github.com/quic-go/quic-go/internal/handshake"

// SetNullAEAD enables or disables null AEAD mode globally.
// When enabled, QUIC AEAD encryption is replaced with a no-op (copy + 16-byte zero tag).
// Both sides of a connection must use the same setting.
func SetNullAEAD(enabled bool) {
	handshake.NullAEAD = enabled
}
