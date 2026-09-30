// Package auth provides shared-secret authentication for the collector's
// agent-facing HTTP endpoints.
package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/otherlodehq/otherlode-collector/metrics"
)

const bearerPrefix = "Bearer "

// TokenSet holds the bearer tokens the ingest routes accept. It keeps
// only the SHA-256 of each token. Store replaces the whole set at once,
// so a request checks against the old set or the new one, never a mix.
// The zero value accepts no token. A TokenSet is safe for concurrent use.
type TokenSet struct {
	hashes atomic.Pointer[[][sha256.Size]byte]
}

// NewTokenSet returns a TokenSet that accepts each of tokens.
func NewTokenSet(tokens []string) *TokenSet {
	s := new(TokenSet)
	s.Store(tokens)
	return s
}

// Store replaces the accepted tokens with tokens.
func (s *TokenSet) Store(tokens []string) {
	hashes := make([][sha256.Size]byte, len(tokens))
	for i, token := range tokens {
		hashes[i] = sha256.Sum256([]byte(token))
	}
	s.hashes.Store(&hashes)
}

// Matches reports whether presented equals any accepted token.
//
// Hashing first gives both sides a fixed length, so each comparison takes
// the same time whether or not the lengths matched.
// subtle.ConstantTimeCompare alone returns at once on a length mismatch
// and would leak the token's length. Matches also compares against every
// token and never stops at the first match, so the time it takes does
// not show which token matched.
func (s *TokenSet) Matches(presented string) bool {
	hashes := s.hashes.Load()
	if hashes == nil {
		return false
	}
	got := sha256.Sum256([]byte(presented))
	match := 0
	for _, want := range *hashes {
		match |= subtle.ConstantTimeCompare(got[:], want[:])
	}
	return match == 1
}

// RequireBearerToken wraps next with a check for an "Authorization: Bearer
// <token>" header whose token is in tokens. A request without a matching
// header is rejected with 401 before it reaches next. The scheme name is
// matched without regard to case, as RFC 9110 requires. The token itself
// is compared exactly. Each request reads the set as it is at that
// moment, so a Store applies from the next request on.
func RequireBearerToken(tokens *TokenSet, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok || !tokens.Matches(presented) {
			metrics.AuthRejected.Inc()
			w.Header().Set("WWW-Authenticate", `Bearer realm="otherlode-collector"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// bearerToken returns the credential from an Authorization header value
// using the Bearer scheme, or false if the header is not a Bearer header.
func bearerToken(header string) (string, bool) {
	if len(header) < len(bearerPrefix) || !strings.EqualFold(header[:len(bearerPrefix)], bearerPrefix) {
		return "", false
	}
	return header[len(bearerPrefix):], true
}
