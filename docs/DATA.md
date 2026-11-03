# DATA: structures and why they fit

Checked against the same code as [FLOW.md](FLOW.md). All of this lives in one process and is lost on restart, which is fine: a restart shows new recognitions, and there is nothing to recover.

The shape of Kafka messages and of the `/post_images` response is owned by the CCTV system. The contract is in [ARCHITECTURE.md](ARCHITECTURE.md#contract-with-the-cctv-system).

## `message_queue`

- **Shape:** a Python list of strings, newest first. Each is a plain person ID.
- **Size:** at most `DISPLAY_LIST_SIZE` (4).
- **Written by:** the consumer thread only.
- **Read by:** the consumer thread only, to build the POST body.
- **Why a list:** at 4 items, a linear scan for duplicates and `insert(0, ...)` cost nothing, and order is the point. Membership lookup is O(n) and insert at the front is O(n), which only matters if `DISPLAY_LIST_SIZE` grows a lot.
- **Duplicates:** a message already in the queue is ignored, so one person seen by several cameras or frames shows once, which the CCTV side confirmed is intended. The cost is that a repeat sighting does not move to the top.

## `messages_to_display`

- **Shape:** the JSON body from the image server, a list in request order of one-key objects: `{"<id>": "<base64 jpg>"}`. The value is `null` for a missing image once the CCTV side switches (D-017).
- **Size:** at most 4 rows, each with a base64 image. Image size is set by the image server.
- **Written by:** the consumer thread. It replaces the value on a 200, and clears it on an outage or after 15 idle seconds.
- **Read by:** the Socket.IO handlers, once every 100 ms per open page.
- **Why whole-list replacement:** the browser redraws everything, so there is no diffing to get wrong. The cost is that every poll resends every image, about 10 times a second per screen.
- **Concurrency:** there is no lock. A replacement swaps the reference, so a reader sees the old or new list, never a mix. The `clear()` paths mutate in place, and a reader can at worst see an empty list a moment early.

## `names_to_display`

- **Shape:** a dict from person ID to name, from `POST /post_names`. IDs with a null or empty name are left out.
- **Size:** at most 4 entries, the IDs in the last request.
- **Written by:** the consumer thread, which replaces it after each image request, and empties it on any name server error. It is also emptied with `messages_to_display`, after 15 idle seconds or on an outage, so a name never outlives its card. Before `v0.7.2` it was not, and the last names kept going to every connected page.
- **Read by:** the Socket.IO handlers, sent as `names` with every poll.
- **Privacy:** names are personal data. They stay in memory and are never logged, here or on the CCTV side.

## `camera_status`

- **Shape:** a dict from camera to `{level, text, expires}`. `expires` is empty for an `ERROR`, and 5 seconds ahead for a `SUCCESS`.
- **Size:** one entry per camera at the entry point.
- **Written by:** the consumer thread, in `update_camera_status`. It is emptied when a new consumer is created.
- **Read by:** the Socket.IO handlers, once every 100 ms per open page, through `current_camera_status`, which skips expired entries.
- **Why a dict:** the latest state per camera is the whole need. A new message for a camera replaces the old one, so a repeated `ERROR` does not add lines.
- **Concurrency:** no lock. The reader copies the items with `list()` before sorting, so a write during a read cannot raise.
- **Trade-off:** it is not persisted. A display restart starts empty and refills from the producer's repeated `ERROR` messages. Each repeat only overwrites one entry and makes no network call, so frequent re-sends cost little here.

## `assigned_partitions`

- **Shape:** a Python set of partition numbers.
- **Size:** one per camera at the entry point.
- **Written by:** the consumer thread, in `assign_new_partitions`. It is cleared when a new consumer is created.
- **Read by:** the consumer thread only.
- **Why a set:** the only question is which partitions are new, and that is a set difference.

## Consumer group offsets

- **Shape:** committed offsets stored by Kafka under `<topic>.display`.
- **Written by:** auto commit from the consumer.
- **Read by:** nothing. The consumer uses `assign()` at `OFFSET_END`, so it ignores them.
- **Why keep it:** `group.id` is required, and the name sits under the client's prefix so that a client's Kafka permissions cover it.

## Announcement flags

- **Shape:** four module-level booleans, such as `connection_failure_message_sent`, and two counters inside the loop.
- **Written by:** the consumer thread.
- **Why flags:** each status is pushed once per outage rather than on every pass. They survive a watchdog restart because they are module-level. The counters do not, because they are local to the loop.

## Log files

- **Shape:** `logs/<year>/<month>/raspberry.log`, plain text, rotated at midnight, with 31 backups.
- **Written by:** `feedback_client_logger`.
- **Holds:** status changes and errors. Debug lines that would log Kafka messages are commented out.
- **Trade-off:** `backupCount` only prunes the current month's folder, because the path moves each month, so total disk use is not strictly capped.
