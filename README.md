# SSE Monitor

sse-monitor is a live system monitor that streams real-time CPU and memory metrics from a Go server to the browser using Server-Sent Events — no WebSockets, no client-side polling, no JS framework.

For the wire format, the broker/fan-out pattern, and how HTMX + Alpine + Chart.js split the rendering, see the companion article: **[Streaming Real-Time Updates in Go with Server-Sent Events](https://blog.karoko.dev/streaming-real-time-updates-in-go-with-server-sent-events-836b04f0f05d)** 

## Table of Contents
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Running the Application](#running-the-application)
- [Endpoints](#endpoints)
- [Deploying Behind a Proxy](#deploying-behind-a-proxy)

## Project Structure

```
sse-monitor/
├── main.go        # Broker, SSE handler, metrics ticker, HTML templates
├── index.html     # HTMX + Alpine + Chart.js dashboard
├── styles.css     # Dashboard styling (full-viewport flex layout)
├── go.mod
└── go.sum
```

## Prerequisites

- Go 1.21 or later
- A terminal and a browser

## Running the Application

Clone the repository
```
git clone https://github.com/iamkaroko/sse-monitor.git
cd sse-monitor
```

Install dependencies
```
go mod tidy
```

Run the server
```
go run main.go
```

Open [http://localhost:8080](http://localhost:8080) — the stat cards fill in within a second, and the rolling graph fills in over the next 60 seconds.

Optional: watch the raw stream
```
curl -N http://localhost:8080/events
```

## Endpoints

| Method | Path      | Description                                              |
|--------|-----------|------------------------------------------------------------|
| GET    | `/`       | The dashboard (serves `index.html`, `styles.css`, and any other static file in the directory) |
| GET    | `/events` | The SSE stream                                            |

## Deploying Behind a Proxy

Reverse proxies buffer responses by default, which silently breaks SSE. If you put this behind Nginx, disable buffering and raise the read timeout above the heartbeat interval:

```nginx
location /events {
    proxy_pass         http://localhost:8080;
    proxy_buffering    off;
    proxy_cache        off;
    proxy_read_timeout 3600s;
}
```
