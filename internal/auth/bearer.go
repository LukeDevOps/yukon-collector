// Package auth provides shared-secret authentication for the collector's
// agent-facing HTTP endpoints.
package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

const bearerPrefix = "Bearer "

// RequireBearerToken wraps next with a check for an "Authorization: Bearer
// <token>" header matching token. A request without a matching header is
// rejected with 401 before it reaches next. The scheme name is matched
// without regard to case, as RFC 9110 requires; the token itself is
// compared exactly.
func RequireBearerToken(token string, next http.Handler) http.Handler {
	want := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok || !tokenMatches(presented, want) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="yukon-collector"`)
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

// tokenMatches compares a presented token against the SHA-256 of the
// expected one. Hashing first gives both sides a fixed length, so the
// comparison takes the same time whether or not the lengths matched;
// subtle.ConstantTimeCompare alone returns at once on a length mismatch
// and would leak the token's length.
func tokenMatches(presented string, want [sha256.Size]byte) bool {
	got := sha256.Sum256([]byte(presented))
	return subtle.ConstantTimeCompare(got[:], want[:]) == 1
}
