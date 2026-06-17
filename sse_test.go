package main

import (
	"net/http/httptest"
	"testing"
)

func TestWriteSSEDataSplitsMultilineMessages(t *testing.T) {
	rec := httptest.NewRecorder()
	writeSSEData(rec, "first\nsecond")

	want := "data: first\ndata: second\n\n"
	if rec.Body.String() != want {
		t.Fatalf("unexpected SSE data:\n got %q\nwant %q", rec.Body.String(), want)
	}
}

func TestWriteSSEEvent(t *testing.T) {
	rec := httptest.NewRecorder()
	writeSSEEvent(rec, "status", "success")

	want := "event: status\ndata: success\n\n"
	if rec.Body.String() != want {
		t.Fatalf("unexpected SSE event:\n got %q\nwant %q", rec.Body.String(), want)
	}
}
