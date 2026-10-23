package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// Use the IPv4 UDP payload limit, which also fits in an IPv6 datagram.
const maxUDPPayload = 65507

type bridge struct {
	ctx       context.Context
	target    *net.UDPAddr
	discovery bool
	upgrader  websocket.Upgrader
	logger    *log.Logger
}

func newBridge(ctx context.Context, target, origin string, logger *log.Logger) (*bridge, error) {
	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return nil, fmt.Errorf("resolve UDP target: %w", err)
	}
	if addr.Port == 0 {
		return nil, errors.New("UDP target must have a nonzero port")
	}

	b := &bridge{
		ctx:    ctx,
		target: addr,
		logger: logger,
		upgrader: websocket.Upgrader{
			HandshakeTimeout: 5 * time.Second,
			ReadBufferSize:   1024,
			WriteBufferSize:  128,
		},
	}
	if origin != "" {
		b.upgrader.CheckOrigin = func(r *http.Request) bool {
			values := r.Header.Values("Origin")
			if len(values) == 0 {
				return true // Non-browser clients need not send Origin.
			}
			return len(values) == 1 && values[0] == origin
		}
	}
	return b, nil
}

func newDiscoveryBridge(ctx context.Context, target, origin string, logger *log.Logger) (*bridge, error) {
	b, err := newBridge(ctx, target, origin, logger)
	if err != nil {
		return nil, err
	}
	if b.target.IP.To4() == nil {
		return nil, errors.New("discovery target must resolve to an IPv4 address")
	}
	b.discovery = true
	return b, nil
}

func (b *bridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var udp *net.UDPConn
	var err error
	if b.discovery {
		// Discovery replies are unicast from many camera addresses, rather than
		// from the multicast destination. A connected UDP socket filters them out.
		udp, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	} else {
		udp, err = net.DialUDP("udp", nil, b.target)
	}
	if err != nil {
		b.logger.Printf("open UDP connection: %v", err)
		http.Error(w, "UDP target unavailable", http.StatusBadGateway)
		return
	}
	defer udp.Close()
	// Some platforms default to a send buffer smaller than a full UDP payload.
	if err := udp.SetWriteBuffer(2 * maxUDPPayload); err != nil {
		b.logger.Printf("set UDP send buffer: %v", err)
		http.Error(w, "UDP socket configuration failed", http.StatusInternalServerError)
		return
	}
	if err := udp.SetReadBuffer(2 * maxUDPPayload); err != nil {
		b.logger.Printf("set UDP receive buffer: %v", err)
		http.Error(w, "UDP socket configuration failed", http.StatusInternalServerError)
		return
	}

	conn, err := b.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote the HTTP error response.
	}
	conn.SetReadLimit(maxUDPPayload)
	responsesDone := make(chan struct{})
	go func() {
		defer close(responsesDone)
		defer conn.Close() // Unblock the WebSocket reader if UDP or WebSocket writes fail.
		b.forwardResponses(conn, udp)
	}()
	defer func() {
		conn.Close()
		udp.Close()
		<-responsesDone
	}()

	// HTTP shutdown does not close upgraded connections.
	stop := context.AfterFunc(b.ctx, func() {
		closeWebSocket(conn, websocket.CloseGoingAway, "server shutting down")
		conn.Close()
	})
	defer stop()

	// Reuse storage for all messages on this connection. Read the entire message
	// before writing so fragmented frames still produce exactly one UDP datagram,
	// and truncated or oversized messages never forward partial commands.
	buffer := make([]byte, maxUDPPayload+1)
	for {
		kind, reader, err := conn.NextReader()
		if err != nil {
			return
		}
		if kind != websocket.BinaryMessage {
			closeWebSocket(conn, websocket.CloseUnsupportedData, "binary messages required")
			return
		}

		n, err := io.ReadFull(reader, buffer)
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			// Gorilla sends code 1009 itself when its read limit is hit.
			if err == nil {
				closeWebSocket(conn, websocket.CloseMessageTooBig, "UDP payload too large")
			}
			return
		}

		if err := udp.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
			b.logger.Printf("set UDP write deadline: %v", err)
			closeWebSocket(conn, websocket.CloseInternalServerErr, "UDP write failed")
			return
		}
		if b.discovery {
			_, err = udp.WriteToUDP(buffer[:n], b.target)
		} else {
			_, err = udp.Write(buffer[:n])
		}
		if err != nil {
			b.logger.Printf("forward UDP command: %v", err)
			closeWebSocket(conn, websocket.CloseInternalServerErr, "UDP write failed")
			return
		}
	}
}

// This is the only WebSocket data writer. It runs independently of the command
// reader so ACKs, completion messages, and multiple discovery replies all pass
// through without assuming one response per command.
func (b *bridge) forwardResponses(conn *websocket.Conn, udp *net.UDPConn) {
	buffer := make([]byte, maxUDPPayload+1)
	for {
		n, err := udp.Read(buffer)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && b.ctx.Err() == nil {
				b.logger.Printf("read UDP response: %v", err)
				closeWebSocket(conn, websocket.CloseInternalServerErr, "UDP read failed")
			}
			return
		}
		if n > maxUDPPayload {
			closeWebSocket(conn, websocket.CloseMessageTooBig, "UDP response too large")
			return
		}
		// A slow or disconnected browser must not leave a UDP reader stuck forever.
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, buffer[:n]); err != nil {
			return
		}
	}
}

func closeWebSocket(conn *websocket.Conn, code int, reason string) {
	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(code, reason), time.Now().Add(time.Second))
}

// newTestWebSocketHandler logs and echoes text and binary messages without opening a UDP socket.
func newTestWebSocketHandler(ctx context.Context, upgrader websocket.Upgrader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetReadLimit(maxUDPPayload)
		stop := context.AfterFunc(ctx, func() {
			closeWebSocket(conn, websocket.CloseGoingAway, "server shutting down")
			conn.Close()
		})
		defer stop()

		for {
			kind, message, err := conn.ReadMessage()
			if err != nil {
				fmt.Println("/ws/test read error: %v", err)
				return
			}
			fmt.Printf("/ws/test received (type %d): %q\n", kind, message)
			if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				return
			}
			if err := conn.WriteMessage(kind, message); err != nil {
				fmt.Println("write error:", err, "kind:", kind, "message:", string(message), "->")
				return
			}
		}
	}
}

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "HTTP/WebSocket listen address")
	target := flag.String("udp", "127.0.0.1:52381", "UDP destination host:port")
	discoveryTarget := flag.String("discovery-udp", "239.255.255.250:3702", "IPv4 WS-Discovery destination host:port")
	origin := flag.String("origin", "", "allowed browser Origin (default: same-origin only)")
	flag.Parse()

	logger := log.New(os.Stderr, "ptz-udp: ", log.LstdFlags)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	b, err := newBridge(ctx, *target, *origin, logger)
	if err != nil {
		logger.Fatal(err)
	}
	discovery, err := newDiscoveryBridge(ctx, *discoveryTarget, *origin, logger)
	if err != nil {
		logger.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/ws", b)
	mux.Handle("/ws/discovery", discovery)
	mux.Handle("/ws/test", newTestWebSocketHandler(ctx, b.upgrader))
	server := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Printf("HTTP shutdown: %v", err)
		}
	}()
	logger.Printf("WebSocket /ws listening on %s -> UDP %s", *listen, b.target)
	logger.Printf("WebSocket /ws/discovery -> UDP %s", discovery.target)
	logger.Printf("WebSocket /ws/test echo endpoint")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal(err)
	}
}
