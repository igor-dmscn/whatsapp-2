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
