package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type testBridge struct {
	udp    *net.UDPConn
	url    string
	cancel context.CancelFunc
}

func startTestBridge(t *testing.T, origin string) *testBridge {
	return startTestBridgeMode(t, origin, false)
}

func startTestBridgeMode(t *testing.T, origin string, discovery bool) *testBridge {
	t.Helper()
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { udp.Close() })
	if err := udp.SetReadBuffer(2 * maxUDPPayload); err != nil {
		t.Fatal(err)
	}
	if err := udp.SetWriteBuffer(2 * maxUDPPayload); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b, err := newBridge(ctx, udp.LocalAddr().String(), origin, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	path := "/ws"
	if discovery {
		b, err = newDiscoveryBridge(ctx, udp.LocalAddr().String(), origin, log.New(io.Discard, "", 0))
		if err != nil {
			t.Fatal(err)
		}
		path = "/ws/discovery"
	}
	mux := http.NewServeMux()
	mux.Handle(path, b)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return &testBridge{udp: udp, url: "ws" + strings.TrimPrefix(server.URL, "http") + path, cancel: cancel}
}

func dialTestBridge(t *testing.T, server *testBridge) *websocket.Conn {
	t.Helper()
	// A small write buffer forces larger messages to span multiple frames.
	dialer := websocket.Dialer{WriteBufferSize: 32, HandshakeTimeout: 2 * time.Second}
	conn, _, err := dialer.Dial(server.url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn
}

func readDatagram(t *testing.T, udp *net.UDPConn) []byte {
	t.Helper()
	payload, _ := readDatagramFrom(t, udp)
	return payload
}

func readDatagramFrom(t *testing.T, udp *net.UDPConn) ([]byte, *net.UDPAddr) {
	t.Helper()
	if err := udp.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, maxUDPPayload+1)
	n, source, err := udp.ReadFromUDP(buffer)
	if err != nil {
		t.Fatal(err)
	}
	return buffer[:n], source
}

func assertBinaryResponse(t *testing.T, conn *websocket.Conn, want []byte) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	kind, got, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if kind != websocket.BinaryMessage || !bytes.Equal(got, want) {
		t.Fatalf("expected binary response %x, got type %d, payload %x", want, kind, got)
	}
}

func writeDatagram(t *testing.T, udp *net.UDPConn, payload []byte, target *net.UDPAddr) {
	t.Helper()
	if err := udp.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := udp.WriteToUDP(payload, target); err != nil {
		t.Fatal(err)
	}
}

func assertNoDatagram(t *testing.T, udp *net.UDPConn) {
	t.Helper()
	if err := udp.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, maxUDPPayload+1)
	_, _, err := udp.ReadFromUDP(buffer)
	if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("expected no UDP datagram, got error %v", err)
	}
}

func assertCloseCode(t *testing.T, conn *websocket.Conn, code int) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, _, err := conn.ReadMessage()
	if !websocket.IsCloseError(err, code) {
		t.Fatalf("expected WebSocket close code %d, got %v", code, err)
	}
}

func TestForwardBinaryMessages(t *testing.T) {
	server := startTestBridge(t, "")
	conn := dialTestBridge(t, server)
	for _, size := range []int{9, 0, 256, 8193, maxUDPPayload, 9} {
		t.Logf("forwarding %d-byte payload", size)
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i)
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
			t.Fatal(err)
		}
		if got := readDatagram(t, server.udp); !bytes.Equal(got, payload) {
			t.Fatalf("payload of %d bytes changed: received %d bytes", size, len(got))
		}
	}
	assertNoDatagram(t, server.udp)
}

func TestFragmentedMessageWithPing(t *testing.T) {
	server := startTestBridge(t, "")
	conn := dialTestBridge(t, server)
	writer, err := conn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x00, 0xff, 0x81}, 1000)
	if _, err := writer.Write(payload[:1500]); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(payload[1500:]); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readDatagram(t, server.udp); !bytes.Equal(got, payload) {
		t.Fatalf("fragmented payload changed: received %d bytes", len(got))
	}
	assertNoDatagram(t, server.udp)
}

func TestRejectInvalidMessages(t *testing.T) {
	tests := []struct {
		name    string
		kind    int
		payload []byte
		code    int
	}{
		{"text", websocket.TextMessage, []byte("command"), websocket.CloseUnsupportedData},
		{"oversized", websocket.BinaryMessage, make([]byte, maxUDPPayload+1), websocket.CloseMessageTooBig},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := startTestBridge(t, "")
			conn := dialTestBridge(t, server)
			if err := conn.WriteMessage(test.kind, test.payload); err != nil {
				t.Fatal(err)
			}
			assertCloseCode(t, conn, test.code)
			assertNoDatagram(t, server.udp)
		})
	}
}

func TestTruncatedMessageIsNotForwarded(t *testing.T) {
	server := startTestBridge(t, "")
	conn := dialTestBridge(t, server)
	writer, err := conn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(make([]byte, 2048)); err != nil {
		t.Fatal(err)
	}
	// Close the transport before sending the final WebSocket frame.
	conn.Close()
	assertNoDatagram(t, server.udp)
}

func TestBrowserOriginPolicy(t *testing.T) {
	tests := []struct {
		name       string
		configured string
		request    string
		allowed    bool
	}{
		{name: "non-browser", allowed: true},
		{name: "same host", request: "same-host", allowed: true},
		{name: "foreign host", request: "http://untrusted.example"},
		{name: "configured", configured: "http://localhost:5173", request: "http://localhost:5173", allowed: true},
		{name: "wrong port", configured: "http://localhost:5173", request: "http://localhost:5174"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := startTestBridge(t, test.configured)
			origin := test.request
			if origin == "same-host" {
				origin = "http" + strings.TrimSuffix(strings.TrimPrefix(server.url, "ws"), "/ws")
			}
			header := http.Header{}
			if origin != "" {
				header.Set("Origin", origin)
			}
			conn, response, err := websocket.DefaultDialer.Dial(server.url, header)
			if test.allowed {
				if err != nil {
					t.Fatal(err)
				}
				conn.Close()
			} else {
				if conn != nil {
					conn.Close()
				}
				if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
					t.Fatalf("expected HTTP 403, got response %v and error %v", response, err)
				}
			}
			if response != nil {
				response.Body.Close()
			}
		})
	}
}

func TestShutdownClosesWebSocket(t *testing.T) {
	server := startTestBridge(t, "")
	conn := dialTestBridge(t, server)
	server.cancel()
	assertCloseCode(t, conn, websocket.CloseGoingAway)
}

func TestVISCACommandAndMultipleResponses(t *testing.T) {
	server := startTestBridge(t, "")
	conn := dialTestBridge(t, server)
	// Eight-byte VISCA-over-IP header followed by a nine-byte command.
	command := []byte{0x01, 0x00, 0x00, 0x09, 0, 0, 0, 1, 0x81, 0x01, 0x06, 0x01, 1, 1, 3, 3, 0xff}
	if err := conn.WriteMessage(websocket.BinaryMessage, command); err != nil {
		t.Fatal(err)
	}
	got, source := readDatagramFrom(t, server.udp)
	if !bytes.Equal(got, command) {
		t.Fatalf("VISCA command or header changed: %x", got)
	}
	ack := []byte{0x01, 0x11, 0x00, 0x03, 0, 0, 0, 1, 0x90, 0x41, 0xff}
	complete := []byte{0x01, 0x11, 0x00, 0x03, 0, 0, 0, 1, 0x90, 0x51, 0xff}
	// Both replies arrive without another WebSocket request.
	writeDatagram(t, server.udp, ack, source)
	writeDatagram(t, server.udp, complete, source)
	assertBinaryResponse(t, conn, ack)
	assertBinaryResponse(t, conn, complete)
	// The command direction remains active after receiving replies.
	if err := conn.WriteMessage(websocket.BinaryMessage, command); err != nil {
		t.Fatal(err)
	}
	if got := readDatagram(t, server.udp); !bytes.Equal(got, command) {
		t.Fatalf("subsequent command changed: %x", got)
	}
}

func TestResponseDatagramBoundaries(t *testing.T) {
	server := startTestBridge(t, "")
	conn := dialTestBridge(t, server)
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte{1}); err != nil {
		t.Fatal(err)
	}
	_, source := readDatagramFrom(t, server.udp)
	for _, size := range []int{0, 9, 8193, maxUDPPayload, 9} {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i)
		}
		writeDatagram(t, server.udp, payload, source)
		assertBinaryResponse(t, conn, payload)
	}
}

func TestClientResponseIsolation(t *testing.T) {
	server := startTestBridge(t, "")
	first := dialTestBridge(t, server)
	second := dialTestBridge(t, server)
	if err := first.WriteMessage(websocket.BinaryMessage, []byte{1}); err != nil {
		t.Fatal(err)
	}
	_, firstSource := readDatagramFrom(t, server.udp)
	if err := second.WriteMessage(websocket.BinaryMessage, []byte{2}); err != nil {
		t.Fatal(err)
	}
	_, secondSource := readDatagramFrom(t, server.udp)
	if firstSource.String() == secondSource.String() {
		t.Fatal("WebSocket clients unexpectedly share a UDP source")
	}
	writeDatagram(t, server.udp, []byte{0x82}, secondSource)
	writeDatagram(t, server.udp, []byte{0x81}, firstSource)
	assertBinaryResponse(t, first, []byte{0x81})
	assertBinaryResponse(t, second, []byte{0x82})
}

func TestDiscoveryRepliesFromMultipleSources(t *testing.T) {
	// A local receiver stands in for the multicast destination. Responses must
	// also be accepted from a different source address/port than that receiver.
	server := startTestBridgeMode(t, "", true)
	conn := dialTestBridge(t, server)
	probe := []byte("<Probe/>")
	if err := conn.WriteMessage(websocket.BinaryMessage, probe); err != nil {
		t.Fatal(err)
	}
	got, source := readDatagramFrom(t, server.udp)
	if !bytes.Equal(got, probe) {
		t.Fatalf("discovery probe changed: %s", got)
	}
	otherCamera, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer otherCamera.Close()
	firstReply := []byte(`<ProbeMatches><XAddrs>http://192.0.2.10/onvif/device_service</XAddrs></ProbeMatches>`)
	secondReply := []byte(`<ProbeMatches><XAddrs>http://192.0.2.11/onvif/device_service</XAddrs></ProbeMatches>`)
	writeDatagram(t, server.udp, firstReply, source)
	assertBinaryResponse(t, conn, firstReply)
	writeDatagram(t, otherCamera, secondReply, source)
	assertBinaryResponse(t, conn, secondReply)
	// Another probe uses the same listening socket for subsequent replies.
	if err := conn.WriteMessage(websocket.BinaryMessage, probe); err != nil {
		t.Fatal(err)
	}
	_, nextSource := readDatagramFrom(t, server.udp)
	if nextSource.String() != source.String() {
		t.Fatal("discovery socket changed between probes")
	}
}
