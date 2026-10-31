# THREAT: who could abuse it and what stops them

Checked against the same code as [FLOW.md](FLOW.md). This system has no customers yet, and these are the threats to rehearse before it does. The display shows personal data: person IDs, such as employee IDs, and face images.

| Actor | Can reach | Worst realistic abuse | What stops it today | Gap |
|---|---|---|---|---|
| Anonymous device on the network | Port 9090 | Reads every person ID and face on the display, live, and stores them | Network placement only | **Open.** No auth on the page or socket, and no TLS, so anyone on that network can also sniff the traffic |
| Any website a kiosk or user visits | Socket.IO, because `cors_allowed_origins="*"` | A page opened on the network connects and reads person IDs and faces | Nothing | **Open.** Restrict origins |
| Anonymous client | Socket.IO event handlers | Floods `check_updates`, each returning all images, to use up CPU and bandwidth | Nothing | **Open.** No rate limit |
| Anonymous client | `client_log` handler | Fills the disk | PR #6 removes the handler | Closed once PR #6 merges |
| A client's display on a shared host | The wrong image server | Shows another client's faces | `IMAGE_SERVER_ADDRESS` per display | Depends on correct settings. Nothing checks the address belongs to this client |
| A compromised or wrong Kafka producer | The topic | Injects a false person ID, or an `ERROR` message that takes over the screen | SASL login is supported here, and Kafka access by `<client>.` prefix is planned (CCTV D-036, step 3) | **Open until step 3.** Messages are not signed, and text is rendered with `textContent`, so this cannot inject script |
| Anyone on the path to Kafka | Traffic to port 9092 | Reads person IDs. `SASL_PLAINTEXT` logs in without encrypting | Trusted network only | **Open until `SASL_SSL`.** The password itself is protected by SCRAM, and only `KAFKA_PASSWORD` in the environment holds it |
| A compromised image server | The JSON response | Sends a false person ID or image | Rendered with `textContent` and a `data:` URL | Trusted by design, and no schema check |
| A compromised CDN | `socket.io.js` | Runs script on the page, reads everything shown | Nothing | **Open.** No Subresource Integrity. Vendoring the file also fixes the offline case |
| A compromised dependency | Install time | Runs code in the image | `~=` pins, `pip check`, pinned base image, non-root user | Transitive dependencies are not locked in the image |
| Someone with repo write access | `main` through a PR | Ships code with no human review | CI and gitleaks | **Accepted.** Green CI is the only merge gate. Forks are never merged |

## Not done on purpose

Person IDs and face images are never written to disk or logs here, so a stolen display host holds no attendance history. Keep it that way (see [CONSTRAINTS.md](CONSTRAINTS.md)).

## Fix order before a real user

1. Keep port 9090 on a trusted network and restrict `cors_allowed_origins`.
2. Add auth or a shared kiosk secret on the page and socket.
3. Vendor `socket.io.js`.
4. Land Kafka prefix permissions and turn on SASL login (CCTV D-036, step 3), then move to `SASL_SSL`.
