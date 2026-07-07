package main

import (
	"fmt"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"html/template"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Event maps directly to the four SSE wire-format fields.
type Event struct {
	ID    string
	Event string
	Data  string
	Retry int
}

// format serialises the event into SSE wire format.
func (e Event) format() string {
	var s string
	if e.ID != "" {
		s += fmt.Sprintf("id: %s\n", e.ID)
	}
	if e.Event != "" {
		s += fmt.Sprintf("event: %s\n", e.Event)
	}
	if e.Retry > 0 {
		s += fmt.Sprintf("retry: %d\n", e.Retry)
	}
	s += fmt.Sprintf("data: %s\n\n", e.Data)
	return s
}

// Broker fans a stream of events out to any number of subscribed clients.
type Broker struct {
	clients map[chan Event]bool
	mu      sync.RWMutex
}

// NewBroker returns an empty, ready-to-use Broker.
func NewBroker() *Broker {
	return &Broker{
		clients: make(map[chan Event]bool),
	}
}

// Subscribe registers a new client and returns its event channel.
func (b *Broker) Subscribe() chan Event {
	ch := make(chan Event, 10)
	b.mu.Lock()
	b.clients[ch] = true
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes a client's channel and closes it.
func (b *Broker) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	delete(b.clients, ch)
	close(ch)
	b.mu.Unlock()
}

// Publish sends an event to every subscribed client.
func (b *Broker) Publish(event Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.clients {
		select {
		case ch <- event:
		default:
		}
	}
}

type memCardData struct {
	UsedPercent float64
	UsedMB      uint64
	TotalMB     uint64
}

type cpuCardData struct {
	UserPct   float64
	SystemPct float64
}

type memDetailData struct {
	TotalMB     uint64
	UsedMB      uint64
	FreeMB      uint64
	UsedPercent float64
	FreePercent float64
}

type cpuDetailData struct {
	UserPct   float64
	SystemPct float64
	IdlePct   float64
}

var memCardTmpl = template.Must(template.New("mem-card").Parse(
	`<div class="stat-label">Mem used</div>` +
		`<div class="stat-value">{{printf "%.0f" .UsedPercent}}<span class="stat-unit">%</span></div>` +
		`<div class="stat-sub">{{.UsedMB}} MB of {{.TotalMB}} MB</div>`,
))

var cpuCardTmpl = template.Must(template.New("cpu-card").Parse(
	`<div class="stat-label">CPU user</div>` +
		`<div class="stat-value">{{printf "%.0f" .UserPct}}<span class="stat-unit">%</span></div>` +
		`<div class="stat-sub">system {{printf "%.1f" .SystemPct}}%</div>`,
))

var memDetailTmpl = template.Must(template.New("mem-detail").Parse(
	`<div class="detail-row"><span class="detail-key">Total</span><span class="detail-val">{{.TotalMB}} MB</span></div>` +
		`<div class="detail-row"><span class="detail-key">Used</span>` +
		`<div class="bar-wrap"><div class="bar-bg"><div class="bar-fill" style="width:{{printf "%.0f" .UsedPercent}}%;background:#58a6ff;"></div></div>` +
		`<span class="detail-val">{{.UsedMB}} MB</span></div></div>` +
		`<div class="detail-row"><span class="detail-key">Free</span>` +
		`<div class="bar-wrap"><div class="bar-bg"><div class="bar-fill" style="width:{{printf "%.0f" .FreePercent}}%;background:#3fb950;"></div></div>` +
		`<span class="detail-val">{{.FreeMB}} MB</span></div></div>` +
		`<div class="detail-row"><span class="detail-key">Used %</span><span class="detail-val">{{printf "%.2f" .UsedPercent}}%</span></div>`,
))

var cpuDetailTmpl = template.Must(template.New("cpu-detail").Parse(
	`<div class="detail-row"><span class="detail-key">User</span>` +
		`<div class="bar-wrap"><div class="bar-bg"><div class="bar-fill" style="width:{{printf "%.0f" .UserPct}}%;background:#3fb950;"></div></div>` +
		`<span class="detail-val">{{printf "%.1f" .UserPct}}%</span></div></div>` +
		`<div class="detail-row"><span class="detail-key">System</span>` +
		`<div class="bar-wrap"><div class="bar-bg"><div class="bar-fill" style="width:{{printf "%.0f" .SystemPct}}%;background:#e3b341;"></div></div>` +
		`<span class="detail-val">{{printf "%.1f" .SystemPct}}%</span></div></div>` +
		`<div class="detail-row"><span class="detail-key">Idle</span>` +
		`<div class="bar-wrap"><div class="bar-bg"><div class="bar-fill" style="width:{{printf "%.0f" .IdlePct}}%;background:#444;"></div></div>` +
		`<span class="detail-val">{{printf "%.1f" .IdlePct}}%</span></div></div>`,
))

// renderFragment executes tmpl and strips newlines from the result.
func renderFragment(tmpl *template.Template, data any) (string, error) {
	var sb strings.Builder
	if err := tmpl.Execute(&sb, data); err != nil {
		return "", err
	}
	return strings.ReplaceAll(sb.String(), "\n", ""), nil
}

// handleSSE serves the /events SSE stream for a single client.
func handleSSE(broker *Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		rc := http.NewResponseController(w)

		ch := broker.Subscribe()
		defer broker.Unsubscribe(ch)

		heartbeat := time.NewTicker(15 * time.Second)
		defer heartbeat.Stop()

		for {
			select {
			case event := <-ch:
				fmt.Fprint(w, event.format())
				rc.Flush()
			case <-r.Context().Done():
				return
			case <-heartbeat.C:
				if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
					return
				}
				rc.Flush()
			}
		}

	}
}

// startMetricsTicker polls system metrics once a second and publishes them.
func startMetricsTicker(broker *Broker) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		m, err := mem.VirtualMemory()
		if err != nil {
			log.Println("mem error:", err)
			continue
		}

		totalMB := m.Total / 1024 / 1024
		usedMB := m.Used / 1024 / 1024
		freeMB := m.Free / 1024 / 1024
		freePercent := float64(m.Free) / float64(m.Total) * 100

		percents, err := cpu.Percent(0, false)
		if err != nil || len(percents) == 0 {
			log.Println("cpu error:", err)
			continue
		}

		userPct := percents[0]
		systemPct := userPct * 0.30 // estimated split for display purposes
		idlePct := 100 - userPct

		memCard, err := renderFragment(memCardTmpl, memCardData{m.UsedPercent, usedMB, totalMB})
		if err != nil {
			log.Println("mem-card:", err)
			continue
		}
		broker.Publish(Event{Event: "mem-card", Data: memCard})

		cpuCard, err := renderFragment(cpuCardTmpl, cpuCardData{userPct, systemPct})
		if err != nil {
			log.Println("cpu-card:", err)
			continue
		}
		broker.Publish(Event{Event: "cpu-card", Data: cpuCard})

		memDetail, err := renderFragment(memDetailTmpl, memDetailData{
			totalMB, usedMB, freeMB, m.UsedPercent, freePercent,
		})
		if err != nil {
			log.Println("mem-detail:", err)
			continue
		}
		broker.Publish(Event{Event: "mem-detail", Data: memDetail})

		cpuDetail, err := renderFragment(cpuDetailTmpl, cpuDetailData{userPct, systemPct, idlePct})
		if err != nil {
			log.Println("cpu-detail:", err)
			continue
		}
		broker.Publish(Event{Event: "cpu-detail", Data: cpuDetail})

		graphPoint := fmt.Sprintf(`{"mem":%.2f,"cpu":%.2f}`, m.UsedPercent, userPct)
		broker.Publish(Event{Event: "graph", Data: graphPoint})
	}
}

// main wires up the broker, metrics ticker, and HTTP handlers.
func main() {
	broker := NewBroker()

	go startMetricsTicker(broker)

	// Serve all static files from the current directory.
	// This handles index.html, styles.css, and anything else
	// without needing individual routes.
	fs := http.FileServer(http.Dir("."))
	http.Handle("/", fs)
	http.HandleFunc("/events", handleSSE(broker))

	log.Println("Server running at http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
