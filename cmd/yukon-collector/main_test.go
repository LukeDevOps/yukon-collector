package main

import "testing"

func TestResolveAuthToken_TokenSet_ReturnsToken(t *testing.T) {
	token, err := resolveAuthToken("s3cret", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "s3cret" {
		t.Fatalf("token = %q, want %q", token, "s3cret")
	}
}

func TestResolveAuthToken_NoTokenNoOptOut_Errors(t *testing.T) {
	_, err := resolveAuthToken("", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestResolveAuthToken_NoTokenExplicitOptOut_ReturnsEmpty(t *testing.T) {
	token, err := resolveAuthToken("", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "" {
		t.Fatalf("token = %q, want empty", token)
	}
}

func TestResolveAuthToken_NoTokenOptOutFalse_Errors(t *testing.T) {
	_, err := resolveAuthToken("", "false")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestResolveAuthToken_NoTokenGarbageOptOut_Errors(t *testing.T) {
	_, err := resolveAuthToken("", "not-a-bool")
	if err == nil {
		t.Fatal("expected error for unparseable opt-out value, got nil")
	}
}

func TestResolveAuthToken_TokenSetAndOptOutSet_TokenWins(t *testing.T) {
	token, err := resolveAuthToken("s3cret", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "s3cret" {
		t.Fatalf("token = %q, want %q", token, "s3cret")
	}
}
