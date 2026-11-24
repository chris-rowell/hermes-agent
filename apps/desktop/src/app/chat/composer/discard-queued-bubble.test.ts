// A server-queued admission that is withdrawn, steered into the running turn, replaced by an edit or discarded as
// lost never starts as its own turn: the prompt this window kept for its start (store/queued-prompt-bubbles.ts)
// is forgotten once the owner accepts that, and kept when the owner refuses (the admission stays queued).
import { afterEach, describe, expect, it, vi } from 'vitest'

import { textPart } from '@/lib/chat-messages'
import { clearQueuedPromptBubbles, deferQueuedPrompt, waitingQueuedPromptCount } from '@/store/queued-prompt-bubbles'

import { cancelQueuedPrompt, discardLostPrompt, replaceQueuedPrompt, steerQueuedPrompt } from './discard-lost-prompt'

const owner = vi.hoisted(() => ({ refuse: false }))

vi.mock('@/store/session-states', () => ({
  requestForOwnedSession: vi.fn(async () => {
    if (owner.refuse) {
      throw Object.assign(new Error('refused'), { data: { reason: 'admission_started' } })
    }

    return { ok: true }
  })
}))

const request = vi.fn() as never

const discards = {
  cancel: () => cancelQueuedPrompt('rt-1', 'stored-1', 'adm-1', request),
  steer: () => steerQueuedPrompt('rt-1', 'stored-1', 'adm-1', request),
  replace: () => replaceQueuedPrompt('rt-1', 'stored-1', 'adm-1', 'edited', 'sub-edit', request),
  lost: () => discardLostPrompt('rt-1', 'stored-1', 'adm-1', request)
}

afterEach(() => {
  owner.refuse = false
  clearQueuedPromptBubbles()
})

describe.each(Object.entries(discards))('%s', (_name, discard) => {
  it('forgets the kept prompt once the owner accepts', async () => {
    deferQueuedPrompt('rt-1', 'adm-1', { id: 'p1', role: 'user', parts: [textPart('hi')] })
    deferQueuedPrompt('rt-1', 'adm-2', { id: 'p2', role: 'user', parts: [textPart('next')] })

    await discard()

    expect(waitingQueuedPromptCount()).toBe(1)
  })

  it('keeps it when the owner refuses', async () => {
    owner.refuse = true
    deferQueuedPrompt('rt-1', 'adm-1', { id: 'p1', role: 'user', parts: [textPart('hi')] })

    await expect(discard()).rejects.toThrow('refused')

    expect(waitingQueuedPromptCount()).toBe(1)
  })
})
