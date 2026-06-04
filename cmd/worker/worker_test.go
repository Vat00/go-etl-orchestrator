package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Тесты для executeShell

func TestExecuteShellSuccess(t *testing.T) {
	config := ShellConfig{Command: "echo hello"}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}

	err = executeShell(raw)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestExecuteShellFail(t *testing.T) {
	config := ShellConfig{Command: "nonexistent_command_xyz"}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}

	err = executeShell(raw)
	if err == nil {
		t.Error("expected error, got nil")
	}
}

// Тесты для executeHTTP

func TestExecuteHTTPSuccess(t *testing.T) {
	// Создаём тестовый HTTP-сервер, который отвечает 200 OK
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	config := HTTPConfig{
		URL:    ts.URL,
		Method: "GET",
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}

	err = executeHTTP(raw)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestExecuteHTTPFail(t *testing.T) {
	config := HTTPConfig{
		URL:    "https://invalid.url.that.does.not.exist",
		Method: "GET",
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}

	err = executeHTTP(raw)
	if err == nil {
		t.Error("expected error, got nil")
	}
}
