/**
 * clientEntryID mints the identifier a client puts on a write so that a retry cannot
 * become a second entry (MS-3).
 *
 * `crypto.randomUUID()` would be the whole of this function, and it is not available
 * everywhere the client runs: it is restricted to secure contexts, and `http://192.168.0.7`
 * is not one — browsers exempt localhost and nothing else. So the app worked on the machine
 * serving it and crashed on every other device on the network, in the composer, the moment a
 * conversation was opened. `crypto.getRandomValues` carries no such restriction, which is
 * what makes the fallback possible at all.
 *
 * The format stays a v4 UUID even though the server stores this column as text and validates
 * nothing. Two clients write this field and the CLI's are real UUIDs; a second shape would be
 * something for a reader to wonder about with nothing to find.
 */
export function clientEntryID(): string {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID()

  // Version 4 in the high nibble of byte 6, variant 1 in the top bits of byte 8. Without
  // those it is a random string wearing a UUID's punctuation. Stamped while mapping rather
  // than by index, because indexing a typed array is `number | undefined` under this
  // project's TypeScript settings and the two casts to silence that read worse than this.
  const hex = Array.from(crypto.getRandomValues(new Uint8Array(16)), (byte, index) => {
    if (index === 6) byte = (byte & 0x0f) | 0x40
    if (index === 8) byte = (byte & 0x3f) | 0x80
    return byte.toString(16).padStart(2, '0')
  }).join('')
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join('-')
}
