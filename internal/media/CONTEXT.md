# Media

Accepting photos and video, and processing them into servable variants. The one place the server deliberately reads payload content.

## Language

**Attachment**:
A photo or video carried by a message, stored and processed independently of the conversation log. A message may reference an attachment that is not yet ready.
_Avoid_: media, file, upload, blob

**Variant**:
One derived rendition of an attachment — a thumbnail or a size- or format-normalised copy — produced so clients need not download the original.
_Avoid_: version, resize, derivative, transcode

**Ready**:
The state of an attachment whose variants all exist and can be served. Until then it is referenced but not displayable.
_Avoid_: processed, complete, available

**Original**:
The bytes as uploaded, kept unchanged forever. Every variant is derived from it, so a variant can be regenerated at any time and none of them is authoritative.
_Avoid_: source, master, raw, full-size

**Pending**:
An attachment whose bytes have not arrived. A reference to one is legitimate — the identifier is issued before the transfer starts, so a message can carry an attachment that is still uploading.
_Avoid_: uploading, incomplete, in progress

**Presigned URL**:
A URL that authorises exactly one request against the object store, for a bounded time, and nothing else. It is how a client transfers bytes without credentials and without them passing through the server. The length and content type are part of the signature, which is what makes a declared size binding.
_Avoid_: signed link, temporary URL, token URL

## What this context does not decide

Who may attach to a conversation, and who may see an attachment. Both are Messaging's: an attachment is exactly as visible as the entry that references it, and entitlement to add one is entitlement to write. Media asks and honours the answer. A second copy of the visibility rule here is the thing most likely to drift out of agreement with the log.

## Known limit

Video is stored and served as uploaded. Deriving a poster frame or a lower-bitrate copy needs a transcoder — a native dependency, a process pool, and a queue with different scaling properties from everything else here — so clients render video with the player's own first frame. The lifecycle is unchanged: a video still goes pending, uploaded, ready, so a client has one shape of state to handle.

Photo orientation is likewise not applied. A phone records rotation in EXIF and a re-encoded rendition loses the tag, so a sideways photo has a sideways thumbnail. The fix is to read the orientation tag and apply one of eight transforms before scaling; it is bounded work and it is not done.
