// Package auth provides shared-secret authentication for the collector's
// agent-facing HTTP endpoints.
package auth

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

const bearerPrefix = "Bearer "

// RequireBearerToken wraps next with a check for an "Authorization: Bearer
// <token>" header matching token. A request without a matching header is
// rejected with 401 before it reaches next.
func RequireBearerToken(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("Authorization")
		if !strings.HasPrefix(got, bearerPrefix) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		presented := got[len(bearerPrefix):]
		if subtle.ConstantTimeCompare([]byte(presented), []byte(token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
