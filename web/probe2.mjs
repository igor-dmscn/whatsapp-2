import { chromium } from 'playwright-core'
import { readdirSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
const cache = join(homedir(), '.cache', 'ms-playwright')
const rev = readdirSync(cache).filter((n) => /^chromium-\d+$/.test(n)).sort().pop()
const browser = await chromium.launch({ headless: true, executablePath: join(cache, rev, 'chrome-linux64', 'chrome') })
const context = await browser.newContext()
const page = await context.newPage()
page.on('console', (m) => { if (m.type() === 'error') console.log('[err]', m.text()) })
page.on('pageerror', (e) => console.log('[pageerror]', e.message))
await page.goto('http://localhost:5173')
await page.getByRole('button', { name: 'Create an account instead' }).click()
const h = 'probe' + Math.random().toString(36).slice(2, 8)
const form = page.locator('form.card')
await form.getByLabel('Handle').fill(h)
await form.getByLabel('Email').fill(h + '@example.test')
await form.getByLabel('Passphrase').fill('correct horse battery staple')
await form.getByRole('button', { name: 'Create account' }).click()
await page.waitForSelector('.status-live', { timeout: 20000 })
console.log('signed in as', h)
await page.waitForTimeout(2500)
const dump = await page.evaluate(async () => {
  const { LocalStore } = await import('/src/store.ts')
  const store = await LocalStore.open()
  return { persistent: store.persistent, conversations: store.conversations().length, cursor: store.cursor() }
})
console.log('store after sign-in:', JSON.stringify(dump))
await browser.close()
