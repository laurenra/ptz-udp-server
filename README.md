# WebSocket / UDP camera bridge

A small Go server for bidirectional VISCA-over-IP traffic and ONVIF camera
discovery. Each binary WebSocket message becomes one UDP datagram; each UDP reply
becomes one binary WebSocket message on the same connection. Bytes and packet
boundaries are preserved in both directions, with no JSON or base64 conversion.
Packets up to 65,507 bytes are supported.

```sh
go run . -listen 127.0.0.1:8080 -udp 192.168.1.100:52381 -origin http://localhost:5173
```

Replace the VISCA destination with your camera's IP and command port. The default
VISCA destination is `127.0.0.1:52381`. All WebSocket endpoints run on the same
HTTP listener:

| Endpoint | UDP destination | Replies |
| --- | --- | --- |
| `/ws` | Camera set by `-udp` | Raw VISCA packets from that camera |
| `/ws/discovery` | `239.255.255.250:3702` (override with `-discovery-udp`) | Raw WS-Discovery XML from multiple cameras |
| `/ws/test` | None | Echoes each text or binary message unchanged (up to 65,507 bytes) |

Without `-origin`, browser connections must come from the same host as the server.
With `-origin`, that exact browser origin is allowed instead. Non-browser clients
may omit the Origin header.

## Test WebSocket connection

Connect to `ws://127.0.0.1:8080/ws/test` to test WebSocket connectivity without a
camera. Each received message is printed to the server console and echoed with
the same content and message type.

```js
const test = new WebSocket("ws://127.0.0.1:8080/ws/test");
test.onopen = () => test.send("hello");
test.onmessage = (event) => console.log("Echo:", event.data);
```

## VISCA commands and replies

Send the **complete VISCA-over-IP packet**: an 8-byte header followed by the VISCA
payload (typically 7–9 bytes for your commands). The server forwards the header
unchanged; it does not add a second header or assign sequence numbers.

```js
const socket = new WebSocket("ws://127.0.0.1:8080/ws");
socket.binaryType = "arraybuffer";
let sequence = 1;

function sendVISCA(payload) {
  const packet = new Uint8Array(8 + payload.byteLength);
  const header = new DataView(packet.buffer);
  header.setUint16(0, 0x0100);             // VISCA command, big-endian.
  header.setUint16(2, payload.byteLength);
  header.setUint32(4, sequence++);
  packet.set(payload, 8);
  socket.send(packet.buffer);
}

socket.addEventListener("open", () => {
  // Example pan/tilt stop command: 8-byte header + 9-byte payload = 17 bytes.
  sendVISCA(new Uint8Array([0x81, 0x01, 0x06, 0x01, 1, 1, 3, 3, 0xff]));
});

socket.addEventListener("message", ({ data }) => {
  const packet = new Uint8Array(data); // Includes the camera's response header.
  console.log("VISCA response:", packet);
});
```

ACKs, completions, errors, and inquiry responses are forwarded as separate binary
messages whenever they arrive. There is no one-request/one-response assumption.
The client handles response parsing, sequence correlation, retries, and any
sequence reset required by its camera. Inquiries use payload type `0x0110` rather
than the command type `0x0100` shown above.

Each VISCA WebSocket has a persistent, connected UDP socket with its own source
port. The camera must reply to that source port from the configured camera
address/port. Replies are isolated between WebSocket connections.

## ONVIF camera discovery

ONVIF discovery uses WS-Discovery SOAP/XML over UDP multicast. Other ONVIF
operations, such as capabilities, media, or PTZ requests, normally use SOAP over
HTTP/HTTPS and are outside this UDP bridge.

The discovery endpoint uses an **unconnected** UDP socket so that unicast replies
from multiple camera addresses can return to the probe's source port. It does not
need to join the multicast group to receive those replies. It uses the operating
system's default IPv4 multicast route; the machine running Go must have access to
the cameras' network. Probes originate from that machine, not from the browser.

```js
const discovery = new WebSocket("ws://127.0.0.1:8080/ws/discovery");
discovery.binaryType = "arraybuffer";
const seen = new Set();

function discover() {
  const probe = `<?xml version="1.0" encoding="UTF-8"?>
<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"
            xmlns:a="http://schemas.xmlsoap.org/ws/2004/08/addressing"
            xmlns:d="http://schemas.xmlsoap.org/ws/2005/04/discovery"
            xmlns:dn="http://www.onvif.org/ver10/network/wsdl">
  <s:Header>
    <a:MessageID>urn:uuid:${crypto.randomUUID()}</a:MessageID>
    <a:To>urn:schemas-xmlsoap-org:ws:2005:04:discovery</a:To>
    <a:Action>http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</a:Action>
    <a:ReplyTo>
      <a:Address>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</a:Address>
    </a:ReplyTo>
  </s:Header>
  <s:Body>
    <d:Probe><d:Types>dn:NetworkVideoTransmitter</d:Types></d:Probe>
  </s:Body>
</s:Envelope>`;
  discovery.send(new TextEncoder().encode(probe)); // Binary, despite containing XML.
}

discovery.addEventListener("open", discover);
discovery.addEventListener("message", ({ data }) => {
  const xml = new TextDecoder().decode(data);
  const document = new DOMParser().parseFromString(xml, "application/xml");
  if (document.getElementsByTagName("parsererror").length) return;
  for (const element of document.getElementsByTagNameNS("*", "XAddrs")) {
    for (const address of element.textContent.trim().split(/\s+/).filter(Boolean)) {
      if (seen.has(address)) continue;
      seen.add(address);
      console.log("Camera:", new URL(address).hostname, "ONVIF service:", address);
    }
  }
});
```

The raw XML contains `XAddrs` service URLs, usually including the camera's IP
address. No extra source-address envelope is added. Keep the connection open for
multiple or delayed replies; call `discover()` again if needed and deduplicate
results. Discovery is best-effort: UDP loss, multicast routing, firewalls, VLANs,
or disabled ONVIF discovery can prevent a camera from appearing.

## Transport and lifecycle

`binaryType = "arraybuffer"` controls received browser messages. Sending an
`ArrayBuffer` or typed array produces an outgoing binary message. For a view into
a larger buffer, send the view itself so only its bytes are transmitted.

The server reuses buffers and uses independent command and response loops for
each connection. Fragmented WebSocket frames are reassembled before forwarding,
so no partial UDP commands are sent. Text messages close the WebSocket with code
1003; oversized messages close it with code 1009. Network errors close the
connection, and closing a WebSocket releases its UDP socket and response reader.
Slow WebSocket writes time out after five seconds. WebSockets use TCP, so transport
latency still depends on the network; UDP delivery and ordering are not guaranteed.

The server binds to loopback by default. It has no authentication; if you expose
it beyond a trusted local network, put it behind an authenticated TLS proxy
(`wss://` is also required for clients served over HTTPS).

## Build and verify

Go 1.22 or newer is required.

### Run server

```sh
go run .
```

### Build and test
```sh
go build -o ptz-udp-server .
go test -race ./...
go vet ./...
```

Tests use local simulated cameras to verify complete VISCA packets, multiple
responses, discovery replies from different UDP sources, per-client isolation,
packet boundaries, invalid messages, origins, and shutdown. Real camera multicast
discovery must also be checked on your network.

Stop with Ctrl+C to close the listener and active WebSocket connections.
