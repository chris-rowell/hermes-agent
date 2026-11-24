import type { ChatMessage } from '@/lib/chat-messages'

/**
 * A server-queue send (submit.ts `queueAdmission`) draws no bubble at send time: while it waits, its queue card
 * stands for it, and a bubble beside the card would show one message twice. Nothing else draws it either:
 * `message.start` of an ordinary admission carries no text. So the sender keeps the prompt here, keyed by the
 * server's admission id from the receipt, and draws it when that admission's turn starts (every canonical event
 * names the executing admission in its envelope).
 *
 * Module state, bounded: an entry leaves when its admission starts, when the owner accepts a withdraw, steer,
 * edit or lost-discard of it (chat/composer/discard-lost-prompt.ts), when its session is dropped
 * (store/session-states.ts), and past `MAX_WAITING_QUEUED_PROMPTS` the oldest goes first.
 */
export const MAX_WAITING_QUEUED_PROMPTS = 32

interface WaitingPrompt {
  sessionId: string
  message: ChatMessage
}

/** Admission id → the prompt kept for its start, oldest first (a Map keeps insertion order). */
const waiting = new Map<string, WaitingPrompt>()

/** Runtime session id → the admission of the latest `message.start` seen on it, and whether that start may draw
 *  its prompt (false when a Stop had already overridden the start). */
const startedAdmission = new Map<string, { admissionId: string; draw: boolean }>()

/** Keep `message` until `admissionId` starts. Returns it instead when that start already arrived (it raced the
 *  receipt), so the caller draws it at once; returns nothing when a Stop had overridden that start. */
export function deferQueuedPrompt(
  sessionId: string,
  admissionId: string,
  message: ChatMessage
): ChatMessage | undefined {
  const started = startedAdmission.get(sessionId)

  if (started?.admissionId === admissionId) {
    return started.draw ? message : undefined
  }

  waiting.delete(admissionId) // a retried receipt refreshes its place instead of counting twice
  waiting.set(admissionId, { sessionId, message })

  while (waiting.size > MAX_WAITING_QUEUED_PROMPTS) {
    const oldest = waiting.keys().next().value

    if (oldest === undefined) {
      break
    }

    waiting.delete(oldest)
  }

  return undefined
}

/** `message.start` on `sessionId` for `admissionId`: the prompt this window queued for that admission, if any.
 *  `draw` false (the session was stopped, so the start arms nothing) drops the prompt instead. */
export function takeQueuedPrompt(sessionId: string, admissionId: unknown, draw = true): ChatMessage | undefined {
  if (typeof admissionId !== 'string' || !admissionId) {
    return undefined
  }

  startedAdmission.set(sessionId, { admissionId, draw })
  const kept = waiting.get(admissionId)
  waiting.delete(admissionId)

  return draw ? kept?.message : undefined
}

/** The owner accepted a withdraw, steer, edit or lost-discard of `admissionId`: it never starts as its own turn. */
export function dropQueuedPrompt(admissionId: string): void {
  waiting.delete(admissionId)
}

/** The session's runtime state was dropped: forget what was kept for it. */
export function forgetQueuedPrompts(sessionId: string): void {
  startedAdmission.delete(sessionId)

  for (const [admissionId, kept] of waiting) {
    if (kept.sessionId === sessionId) {
      waiting.delete(admissionId)
    }
  }
}

/** Every session state was dropped (gateway switch). */
export function clearQueuedPromptBubbles(): void {
  waiting.clear()
  startedAdmission.clear()
}

/** How many prompts wait for their start (tests and diagnostics). */
export function waitingQueuedPromptCount(): number {
  return waiting.size
}

/** The prompt as the started turn's first row: right after `anchorId` (the newest row when the turn started), else
 *  at the end. Unchanged when it is already drawn, or when a projected queued row (`user-queued-…`, a resume of the
 *  still-queued send) is about to become this turn's prompt. */
export function withQueuedPrompt(
  messages: ChatMessage[],
  prompt: ChatMessage,
  anchorId: string | null | undefined
): ChatMessage[] {
  if (messages.some(m => m.id === prompt.id || (m.id.startsWith('user-queued-') && !m.queuedTurnStarted))) {
    return messages
  }

  const anchor = anchorId ? messages.findIndex(m => m.id === anchorId) : -1
  const at = anchor >= 0 ? anchor + 1 : messages.length

  return [...messages.slice(0, at), prompt, ...messages.slice(at)]
}
