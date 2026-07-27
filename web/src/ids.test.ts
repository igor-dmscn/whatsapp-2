// The insecure-context case is the reason this module exists, so it is what is tested.
//
// Deleting crypto.randomUUID is not a contrived setup: it is exactly the environment the
// browser presents on http://192.168.0.7, where the app crashed for every device that was
// not the one serving it.
import { afterEach, describe, expect, it } from 'vitest'

import { clientEntryID } from './ids'

const v4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

const native = crypto.randomUUID

afterEach(() => {
  Object.defineProperty(crypto, 'randomUUID', { value: native, configurable: true })
})

function withoutRandomUUID() {
  Object.defineProperty(crypto, 'randomUUID', { value: undefined, configurable: true })
}

describe('clientEntryID', () => {
  it('is a v4 uuid where crypto.randomUUID exists', () => {
    expect(clientEntryID()).toMatch(v4)
  })

  it('is still a v4 uuid where it does not — an insecure context', () => {
    withoutRandomUUID()
    expect(clientEntryID()).toMatch(v4)
  })

  it('does not repeat itself in either context', () => {
    const secure = new Set(Array.from({ length: 500 }, clientEntryID))
    withoutRandomUUID()
    const insecure = new Set(Array.from({ length: 500 }, clientEntryID))

    expect(secure.size).toBe(500)
    expect(insecure.size).toBe(500)
    // Two ids colliding across the two paths would mean the fallback is not random at all.
    expect(new Set([...secure, ...insecure]).size).toBe(1000)
  })
})
