package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const sseWriteWindow = 30 * time.Second
const sseHeartbeatInterval = 15 * time.Second

// SSEBroker manages per-build SSE streams
type SSEBroker struct {
	mu       sync.RWMutex
	channels map[int64][]chan string // buildID -> list of subscriber channels
}

func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		channels: make(map[int64][]chan string),
	}
}

func (b *SSEBroker) Subscribe(buildID int64) chan string {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan string, 256)
	b.channels[buildID] = append(b.channels[buildID], ch)
	return ch
}

func (b *SSEBroker) Unsubscribe(buildID int64, ch chan string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	subs := b.channels[buildID]
	for i, sub := range subs {
		if sub == ch {
			b.channels[buildID] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	if len(b.channels[buildID]) == 0 {
		delete(b.channels, buildID)
	}
}

func (b *SSEBroker) Publish(buildID int64, message string) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.channels[buildID] {
		select {
		case ch <- message:
		default:
			// drop if subscriber is too slow
		}
	}
}

func (b *SSEBroker) Close(buildID int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.channels[buildID] {
		close(ch)
	}
	delete(b.channels, buildID)
}

func handleSSEStream(broker *SSEBroker, w http.ResponseWriter, r *http.Request, buildID int64) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	controller := http.NewResponseController(w)
	setSSEWriteDeadline(controller)
	ctx := r.Context()

	// First send any existing log from DB
	build, err := getBuildContext(ctx, buildID)
	if err != nil {
		http.Error(w, "build not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Send existing log — line by line as separate SSE events
	if build.Log != "" {
		for _, line := range strings.Split(build.Log, "\n") {
			writeSSEData(w, line)
		}
		setSSEWriteDeadline(controller)
		flusher.Flush()
	}

	// If build is already done, send status and close
	if build.Status != "running" {
		writeSSEEvent(w, "status", build.Status)
		setSSEWriteDeadline(controller)
		flusher.Flush()
		return
	}

	// Subscribe to live updates
	ch := broker.Subscribe(buildID)
	defer broker.Unsubscribe(buildID, ch)

	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			fmt.Fprint(w, ": keepalive\n\n")
			setSSEWriteDeadline(controller)
			flusher.Flush()
		case msg, ok := <-ch:
			if !ok {
				// Channel closed — build is done
				// Re-fetch status
				b, _ := getBuildContext(ctx, buildID)
				if b != nil {
					writeSSEEvent(w, "status", b.Status)
					setSSEWriteDeadline(controller)
					flusher.Flush()
				}
				return
			}
			writeSSEData(w, msg)
			setSSEWriteDeadline(controller)
			flusher.Flush()
		}
	}
}

func setSSEWriteDeadline(controller *http.ResponseController) {
	logOperationalError("set SSE write deadline", controller.SetWriteDeadline(time.Now().Add(sseWriteWindow)))
}

func writeSSEData(w http.ResponseWriter, data string) {
	for _, line := range strings.Split(data, "\n") {
		fmt.Fprintf(w, "data: %s\n", line)
	}
	fmt.Fprint(w, "\n")
}

func writeSSEEvent(w http.ResponseWriter, event, data string) {
	fmt.Fprintf(w, "event: %s\n", event)
	writeSSEData(w, data)
}
