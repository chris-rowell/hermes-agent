import { afterEach, describe, expect, it } from 'vitest'

import { type ChatMessage, textPart } from '@/lib/chat-messages'

import {
  clearQueuedPromptBubbles,
  deferQueuedPrompt,
  dropQueuedPrompt,
  forgetQueuedPrompts,
  MAX_WAITING_QUEUED_PROMPTS,
  takeQueuedPrompt,
  waitingQueuedPromptCount,
  withQueuedPrompt
} from './queued-prompt-bubbles'
import { clearAllSessionStates } from './session-states'

const prompt = (id: string, text = id): ChatMessage => ({ id, role: 'user', parts: [textPart(text)] })
const row = (id: string, role: ChatMessage['role'] = 'assistant'): ChatMessage => ({ id, role, parts: [textPart(id)] })

afterEach(() => clearQueuedPromptBubbles())

describe('deferQueuedPrompt / takeQueuedPrompt', () => {
  it('keeps the prompt until its own admission starts (receipt before start)', () => {
    expect(deferQueuedPrompt('s1', 'adm-1', prompt('p1'))).toBeUndefined()
    expect(takeQueuedPrompt('s1', 'adm-other')).toBeUndefined()
    expect(takeQueuedPrompt('s1', 'adm-1')?.id).toBe('p1')
    // Taken once: a repeated start draws nothing more.
    expect(takeQueuedPrompt('s1', 'adm-1')).toBeUndefined()
    expect(waitingQueuedPromptCount()).toBe(0)
  })

  it('hands the prompt straight back when its start beat the receipt', () => {
    expect(takeQueuedPrompt('s1', 'adm-1')).toBeUndefined()
    expect(deferQueuedPrompt('s1', 'adm-1', prompt('p1'))?.id).toBe('p1')
    expect(waitingQueuedPromptCount()).toBe(0)
  })

  it('a start for another admission does not release this one', () => {
    takeQueuedPrompt('s1', 'adm-0')
    expect(deferQueuedPrompt('s1', 'adm-1', prompt('p1'))).toBeUndefined()
    expect(waitingQueuedPromptCount()).toBe(1)
  })

  it('ignores a start with no admission id', () => {
    deferQueuedPrompt('s1', 'adm-1', prompt('p1'))
    expect(takeQueuedPrompt('s1', undefined)).toBeUndefined()
    expect(takeQueuedPrompt('s1', '')).toBeUndefined()
    expect(waitingQueuedPromptCount()).toBe(1)
  })

  it('a start the Stop overrode (draw=false) drops the prompt, before or after the receipt', () => {
    deferQueuedPrompt('s1', 'adm-1', prompt('p1'))
    expect(takeQueuedPrompt('s1', 'adm-1', false)).toBeUndefined()
    expect(waitingQueuedPromptCount()).toBe(0)
    expect(takeQueuedPrompt('s1', 'adm-1')).toBeUndefined()

    expect(takeQueuedPrompt('s2', 'adm-2', false)).toBeUndefined()
    expect(deferQueuedPrompt('s2', 'adm-2', prompt('p2'))).toBeUndefined()
    expect(waitingQueuedPromptCount()).toBe(0)
  })
})

describe('cleanup', () => {
  it(`keeps at most ${MAX_WAITING_QUEUED_PROMPTS} waiting prompts, dropping the oldest`, () => {
    const total = MAX_WAITING_QUEUED_PROMPTS + 5

    for (let n = 0; n < total; n += 1) {
      deferQueuedPrompt('s1', `adm-${n}`, prompt(`p${n}`))
    }

    expect(waitingQueuedPromptCount()).toBe(MAX_WAITING_QUEUED_PROMPTS)
    expect(takeQueuedPrompt('s1', 'adm-0')).toBeUndefined()
    expect(takeQueuedPrompt('s1', 'adm-4')).toBeUndefined()
    expect(takeQueuedPrompt('s1', 'adm-5')?.id).toBe('p5')
    expect(takeQueuedPrompt('s1', `adm-${total - 1}`)?.id).toBe(`p${total - 1}`)
  })

  it('re-deferring an admission refreshes it instead of counting it twice', () => {
    deferQueuedPrompt('s1', 'adm-1', prompt('p1'))
    deferQueuedPrompt('s1', 'adm-1', prompt('p1-retry'))
    expect(waitingQueuedPromptCount()).toBe(1)
    expect(takeQueuedPrompt('s1', 'adm-1')?.id).toBe('p1-retry')
  })

  it('a re-deferred admission counts as the newest, so the cap drops an older one first', () => {
    for (let n = 0; n < MAX_WAITING_QUEUED_PROMPTS; n += 1) {
      deferQueuedPrompt('s1', `adm-${n}`, prompt(`p${n}`))
    }

    deferQueuedPrompt('s1', 'adm-0', prompt('p0-retry'))
    deferQueuedPrompt('s1', 'adm-new', prompt('p-new'))

    expect(waitingQueuedPromptCount()).toBe(MAX_WAITING_QUEUED_PROMPTS)
    expect(takeQueuedPrompt('s1', 'adm-1')).toBeUndefined()
    expect(takeQueuedPrompt('s1', 'adm-0')?.id).toBe('p0-retry')
  })

  it('a discarded admission (cancel, steer, edit) is forgotten', () => {
    deferQueuedPrompt('s1', 'adm-1', prompt('p1'))
    dropQueuedPrompt('adm-1')
    expect(waitingQueuedPromptCount()).toBe(0)
    expect(takeQueuedPrompt('s1', 'adm-1')).toBeUndefined()
  })

  it('a removed session forgets its waiting prompts and its last start, and only its own', () => {
    deferQueuedPrompt('s1', 'adm-1', prompt('p1'))
    deferQueuedPrompt('s2', 'adm-2', prompt('p2'))
    takeQueuedPrompt('s1', 'adm-started')

    forgetQueuedPrompts('s1')

    expect(waitingQueuedPromptCount()).toBe(1)
    // Its start record is gone too: a late receipt for that admission waits instead of drawing.
    expect(deferQueuedPrompt('s1', 'adm-started', prompt('late'))).toBeUndefined()
    expect(waitingQueuedPromptCount()).toBe(2)
    expect(takeQueuedPrompt('s1', 'adm-1')).toBeUndefined()
    expect(takeQueuedPrompt('s2', 'adm-2')?.id).toBe('p2')
  })

  it('clearAllSessionStates (gateway switch) forgets every kept prompt', () => {
    deferQueuedPrompt('s1', 'adm-1', prompt('p1'))
    deferQueuedPrompt('s2', 'adm-2', prompt('p2'))
    clearAllSessionStates()
    expect(waitingQueuedPromptCount()).toBe(0)
  })

  it('clearQueuedPromptBubbles forgets everything', () => {
    deferQueuedPrompt('s1', 'adm-1', prompt('p1'))
    takeQueuedPrompt('s2', 'adm-2')
    clearQueuedPromptBubbles()
    expect(waitingQueuedPromptCount()).toBe(0)
    expect(deferQueuedPrompt('s2', 'adm-2', prompt('p2'))).toBeUndefined()
  })
})

describe('withQueuedPrompt', () => {
  const history = [row('u0', 'user'), row('a0'), row('a1')]

  it('draws the prompt right after the anchor', () => {
    expect(withQueuedPrompt(history, prompt('p'), 'a0').map(m => m.id)).toEqual(['u0', 'a0', 'p', 'a1'])
  })

  it('draws it at the end with no anchor, or an anchor no longer on screen', () => {
    expect(withQueuedPrompt(history, prompt('p'), null).map(m => m.id)).toEqual(['u0', 'a0', 'a1', 'p'])
    expect(withQueuedPrompt(history, prompt('p'), 'gone').map(m => m.id)).toEqual(['u0', 'a0', 'a1', 'p'])
  })

  it('never draws it twice', () => {
    const drawn = withQueuedPrompt(history, prompt('p'), null)
    expect(withQueuedPrompt(drawn, prompt('p'), null)).toBe(drawn)
  })

  it('leaves a not-yet-started resume projection of the queued send to stand for it', () => {
    const projected = [...history, { ...row('user-queued-s1', 'user') }]
    expect(withQueuedPrompt(projected, prompt('p'), null)).toBe(projected)
  })

  it('a projection that already started (an earlier turn) does not block it', () => {
    const projected = [...history, { ...row('user-queued-s1', 'user'), queuedTurnStarted: true }]
    expect(withQueuedPrompt(projected, prompt('p'), null).map(m => m.id)).toEqual([
      'u0',
      'a0',
      'a1',
      'user-queued-s1',
      'p'
    ])
  })
})
