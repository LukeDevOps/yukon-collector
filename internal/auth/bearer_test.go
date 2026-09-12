package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestHandler() (http.Handler, *bool) {
	reached := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})
	return RequireBearerToken("s3cret", next), &reached
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
