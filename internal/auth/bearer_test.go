package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/otherlodehq/otherlode-collector/metrics"
)

func newTestHandler() (http.Handler, *bool) {
	reached := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})
	return RequireBearerToken(NewTokenSet([]string{"s3cret"}), next), &reached
}

func TestRequireBearerToken_CorrectToken_ReachesNext(t *testing.T) {
	handler, reached := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer s3cret")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !*reached {
		t.Fatal("next handler was not reached")
	}
}

func TestRequireBearerToken_MissingHeader_Rejected(t *testing.T) {
	handler, reached := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if *reached {
		t.Fatal("next handler should not have been reached")
	}
}

func TestRequireBearerToken_WrongToken_Rejected(t *testing.T) {
	handler, reached := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if *reached {
		t.Fatal("next handler should not have been reached")
	}
}

func TestRequireBearerToken_MalformedScheme_Rejected(t *testing.T) {
	handler, reached := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "s3cret")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if *reached {
		t.Fatal("next handler should not have been reached")
	}
}

func TestRequireBearerToken_SchemeIsCaseInsensitive(t *testing.T) {
	handler, reached := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "bearer s3cret")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (RFC 9110 scheme names are case-insensitive)", rec.Code, http.StatusOK)
	}
	if !*reached {
		t.Fatal("next handler should have been reached")
	}
}

func TestRequireBearerToken_TokenIsCaseSensitive(t *testing.T) {
	handler, _ := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer S3CRET")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestRequireBearerToken_Rejection_SetsWWWAuthenticate(t *testing.T) {
	handler, _ := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("WWW-Authenticate"); !strings.HasPrefix(got, "Bearer") {
		t.Fatalf("WWW-Authenticate = %q, want a Bearer challenge", got)
	}
}

func TestRequireBearerToken_TokenList(t *testing.T) {
	tokens := NewTokenSet([]string{"old-token", "new-token", "third-token"})

	tests := map[string]struct {
		header   string
		wantCode int
	}{
		"first listed token":  {header: "Bearer old-token", wantCode: http.StatusOK},
		"second listed token": {header: "Bearer new-token", wantCode: http.StatusOK},
		"last listed token":   {header: "Bearer third-token", wantCode: http.StatusOK},
		"unlisted token":      {header: "Bearer other-token", wantCode: http.StatusUnauthorized},
		"two tokens joined":   {header: "Bearer old-token,new-token", wantCode: http.StatusUnauthorized},
		"empty token":         {header: "Bearer ", wantCode: http.StatusUnauthorized},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			reached := false
			handler := RequireBearerToken(tokens, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reached = true
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("Authorization", tt.header)
			rec := httptest.NewRecorder()
			rejectedBefore := metrics.AuthRejected.Value()

			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantCode)
			}
			rejected := metrics.AuthRejected.Value() - rejectedBefore
			if tt.wantCode == http.StatusOK {
				if !reached {
					t.Fatal("next handler was not reached")
				}
				if rejected != 0 {
					t.Fatalf("auth rejected counter rose by %d, want 0", rejected)
				}
				return
			}
			if reached {
				t.Fatal("next handler should not have been reached")
			}
			if got := rec.Header().Get("WWW-Authenticate"); !strings.HasPrefix(got, "Bearer") {
				t.Fatalf("WWW-Authenticate = %q, want a Bearer challenge", got)
			}
			if rejected != 1 {
				t.Fatalf("auth rejected counter rose by %d, want 1", rejected)
			}
		})
	}
}

func TestTokenSet_Store_ReplacesTheWholeSet(t *testing.T) {
	tokens := NewTokenSet([]string{"old-token"})
	tokens.Store([]string{"new-token", "other-token"})

	for token, want := range map[string]bool{
		"old-token":   false,
		"new-token":   true,
		"other-token": true,
	} {
		if got := tokens.Matches(token); got != want {
			t.Errorf("Matches(%q) = %v, want %v", token, got, want)
		}
	}
}

func TestTokenSet_ZeroValue_MatchesNothing(t *testing.T) {
	var tokens TokenSet
	if tokens.Matches("") {
		t.Fatal("zero TokenSet matched an empty token")
	}
	if tokens.Matches("s3cret") {
		t.Fatal("zero TokenSet matched a token")
	}
}
