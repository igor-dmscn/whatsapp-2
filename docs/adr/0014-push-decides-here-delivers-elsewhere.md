# Push notifications decide here and deliver behind a port

The consumer that reacts to `messaging.entry_appended` contains the whole decision — who should be woken up, whether they are already looking, and whether this notification has already gone out — and hands the result to a `Sender` interface with one method. The implementation that ships writes a log line.

APNs and FCM are not implemented. Not deferred with a shrug: they cannot be. Both require credentials issued to a real application by a real vendor account, and NF-15 says the whole system starts locally with one command and no cloud dependencies. An integration nobody in this repository can run is not a feature, it is a file that compiles.

We rejected inventing a device-token table to make the seam look complete. Tokens come from a mobile SDK during app registration, and there is no mobile client here; a table with no writer, populated by a test fixture, would assert only that a join works.

We rejected sending notifications from the send path. A person waiting for their message to be accepted must not also wait for a provider on the other side of the internet, and a provider being down must not fail a send. The entry commits, the request is answered, and the notification happens afterwards from the durable log ([ADR-0003](./0003-postgres-is-truth-kafka-carries-events.md)).

## Consequences

- **The judgement is testable and tested; the transport is not.** Three rules decide whether anybody is woken: not the author, not somebody whose device is currently connected, and not twice for the same entry. Each is a complaint if it is missing, and the third is the one at-least-once delivery makes inevitable rather than unlikely.
- **A notification carries no message body.** The server holds payloads it does not read ([ADR-0001](./0001-content-opaque-server-e2ee-deferred.md)), and handing one to a third-party provider would put message text on somebody else's infrastructure — a decision about privacy rather than about notifications. A provider is told who and where; the client fetches what.
- **Presence is a dependency of push**, which is why phase 10 built presence first. A notification on the phone in your hand while you read the message on it is worse than silence: it teaches people to ignore notifications.
- **Suppression fails toward sending.** If Redis cannot say whether a notification already went out, it is sent — a duplicate is a smaller failure than silence about a message somebody needed. Presence failing does the same thing for the same reason.
- Adding a provider is one type implementing one method, plus wherever a deployment chooses to keep device tokens. Nothing above the port changes.
