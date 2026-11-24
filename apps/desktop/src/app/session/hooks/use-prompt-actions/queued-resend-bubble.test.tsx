// A message typed while the agent looked busy (busy-input mode `interrupt`) goes to redirectPrompt first. When that
// correction is refused — the gateway answers `rejected` (the reply finished before the redirect took the lock), or
// the composer's busy belief is stale and the refusal happens before any RPC — the composer resends the words through
// the server-owned queue (`fromQueue: true`). Claim under test: that resend must still put the user's message in the
// transcript once its turn runs, without a reload.
//
// Real composer submit hook, real prompt actions (redirect + submit), real session-state cache and real message-stream
// reducer; only the RPC answers are stand-ins.
import type { GatewayEventName } from '@hermes/shared'
import { QueryClient } from '@tanstack/react-query'
import { act, cleanup, renderHook, waitFor } from '@testing-library/react'
import type { PropsWithChildren } from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { useComposerSubmit } from '@/app/chat/composer/hooks/use-composer-submit'
import { ComposerScopeProvider, ComposerSurfaceProvider, MAIN_COMPOSER_SCOPE } from '@/app/chat/composer/scope'
import { PaneVisibleContext } from '@/components/pane-shell/pane-visibility'
import { type ChatMessage, chatMessageText, textPart } from '@/lib/chat-messages'
import { clearQueuedPrompts } from '@/store/composer-queue'
import { clearQueuedPromptBubbles, waitingQueuedPromptCount } from '@/store/queued-prompt-bubbles'
import { $connection, setMessages } from '@/store/session'
import { dropSessionState } from '@/store/session-states'

import { useMessageStream } from '../use-message-stream'
import { useSessionStateCache } from '../use-session-state-cache'

import { usePromptActions } from '.'

const TYPED = 'actually, check the staging logs instead'
const REPLY = 'Checked the staging logs: nothing new.'
const noop = async () => undefined
let seq = 0

type RedirectAnswer = 'rejected' | 'redirected' | 'queued'
type Receipt = 'queued' | 'started'

interface MountOptions {
  /** What session.redirect answers. */
  redirect?: RedirectAnswer
  /** prompt.submit's receipt status. */
  receipt?: Receipt
  /** Emit this admission's message.start BEFORE prompt.submit's receipt returns (an idle owner claims at once). */
  startBeforeReceipt?: boolean
  /** The composer's busy belief outlives the turn (the cache never syncs it back to false). */
  staleBusy?: boolean
}

function mount(opts: MountOptions = {}) {
  seq += 1
  const SID = `queued-resend-runtime-${seq}`
  const STORED = `queued-resend-stored-${seq}`
  const calls: Array<{ method: string; params?: Record<string, unknown> }> = []
  const admissionOf = (submissionId: unknown) => `adm-${String(submissionId)}`
  const submissions: string[] = []
  const busyRef = { current: false }
  let emitStart: (admission: string) => void = () => undefined

  const rpc = async (method: string, params?: Record<string, unknown>) => {
    calls.push({ method, params })

    if (method === 'session.redirect' || method === 'session.steer') {
      return { status: opts.redirect ?? 'rejected' }
    }

    if (method === 'prompt.submit') {
      submissions.push(String(params?.submission_id))

      if (opts.startBeforeReceipt) {
        emitStart(admissionOf(params?.submission_id))
      }

      // The canonical receipt: the SERVER's admission id plus the client's submission id echoed back.
      return {
        admission_id: admissionOf(params?.submission_id),
        session_id: SID,
        status: opts.receipt ?? 'started',
        submission_id: params?.submission_id
      }
    }

    return {}
  }

  const queryClient = new QueryClient()

  const core = renderHook(() => {
    const cache = useSessionStateCache({
      activeSessionId: SID,
      selectedStoredSessionId: STORED,
      busyRef,
      setAwaitingResponse: () => undefined,
      setBusy: value => {
        if (!opts.staleBusy) {
          busyRef.current = value
        }
      },
      setMessages
    })

    const stream = useMessageStream({
      activeSessionIdRef: cache.activeSessionIdRef,
      sessionStateByRuntimeIdRef: cache.sessionStateByRuntimeIdRef,
      updateSessionState: cache.updateSessionState,
      queryClient,
      hydrateFromStoredSession: noop,
      refreshHermesConfig: noop,
      refreshSessions: noop
    })

    const actions = usePromptActions({
      activeSessionId: SID,
      activeSessionIdRef: cache.activeSessionIdRef,
      branchCurrentSession: async () => true,
      busyRef,
      createBackendSessionForSend: async () => SID,
      getRoutedStoredSessionId: () => STORED,
      getRuntimeIdForStoredSession: () => SID,
      getRouteToken: () => 'queued-resend-route',
      handleSkinCommand: () => '',
      openMemoryGraph: () => undefined,
      refreshSessions: noop,
      requestGateway: rpc as never,
      resumeStoredSession: noop,
      runtimeIdByStoredSessionIdRef: cache.runtimeIdByStoredSessionIdRef,
      selectedStoredSessionIdRef: cache.selectedStoredSessionIdRef,
      startFreshSessionDraft: () => undefined,
      sttEnabled: false,
      updateSessionState: cache.updateSessionState
    })

    return { cache, stream, actions }
  })

  act(() => core.result.current.cache.ensureSessionState(SID, STORED))

  const event = (type: GatewayEventName, admission: string, payload: Record<string, unknown> = {}) =>
    core.result.current.stream.handleGatewayEvent({ type, session_id: SID, admission_id: admission, payload } as never)

  emitStart = admission => event('message.start', admission)

  // The composer, as the chat bar mounts it, with onSteer/onSubmit wired to the real prompt actions.
  const draftRef = { current: '' }
  const editor = window.document.createElement('div')
  editor.dataset.slot = 'composer-rich-input'

  const Wrapper = ({ children }: PropsWithChildren) => (
    <ComposerScopeProvider value={{ ...MAIN_COMPOSER_SCOPE, target: 'main' }}>
      <ComposerSurfaceProvider value={`queued-resend-surface-${seq}`}>
        <PaneVisibleContext.Provider value={true}>
          <div data-composer-surface-id={`queued-resend-surface-${seq}`} data-composer-target="main">
            {children}
          </div>
        </PaneVisibleContext.Provider>
      </ComposerSurfaceProvider>
    </ComposerScopeProvider>
  )

  const composer = renderHook(
    () =>
      useComposerSubmit({
        activeQueueSessionKey: STORED,
        activeQueueSessionKeyRef: { current: STORED },
        attachments: [],
        busy: true,
        busyInputMode: 'interrupt',
        clearDraft: () => {
          draftRef.current = ''
          editor.textContent = ''
        },
        disabled: false,
        draftScopeRef: { current: STORED },
        draftRef,
        drainNextQueued: vi.fn(async () => false),
        editorRef: { current: editor },
        exitQueuedEdit: vi.fn(() => false),
        focusInput: vi.fn(),
        inputDisabled: false,
        loadIntoComposer: vi.fn(),
        onCancel: vi.fn(),
        onSteer: (text: string, mode?: 'interrupt' | 'steer') => core.result.current.actions.redirectPrompt(text, mode),
        onSteerHidden: vi.fn(async () => true),
        onSubmit: ((text: string, options: never) => core.result.current.actions.submitText(text, options)) as never,
        queueCurrentDraft: vi.fn(() => true),
        queueEdit: null,
        queuedPrompts: [],
        sessionId: SID,
        setComposerText: vi.fn(),
        stashAt: vi.fn()
      }),
    { wrapper: Wrapper }
  )

  return {
    SID,
    STORED,
    calls,
    busyRef,
    submissions,
    admissionOf,
    methods: () => calls.map(call => call.method),
    messages: (): ChatMessage[] =>
      core.result.current.cache.sessionStateByRuntimeIdRef.current.get(SID)?.messages ?? [],
    update: (updater: Parameters<typeof core.result.current.cache.updateSessionState>[1]) =>
      act(() => {
        core.result.current.cache.updateSessionState(SID, updater, STORED)
      }),
    /** The user types into the busy composer and presses Enter. */
    typeAndEnter: async (text: string) => {
      draftRef.current = text
      editor.textContent = text
      act(() => {
        composer.result.current.submitDraft()
      })
      // The steer → refusal → keep() → submit chain runs on promises.
      await waitFor(() =>
        expect(calls.some(call => call.method === 'prompt.submit' || call.method.startsWith('session.'))).toBe(true)
      )
      await act(async () => new Promise(resolve => setTimeout(resolve, 0)))
    },
    event: (type: GatewayEventName, admission: string, payload: Record<string, unknown> = {}) =>
      act(() => event(type, admission, payload)),
    /** The idle ChatBar Enter: a plain submit with no queue flag. */
    submit: (text: string) =>
      act(async () => {
        await core.result.current.actions.submitText(text, { attachments: [] })
      }),
    /** The resent admission's turn, as the gateway streams it: start (no text), one delta, complete with its receipt. */
    runQueuedTurn: async (submissionId: string) => {
      const admission = admissionOf(submissionId)

      if (!opts.startBeforeReceipt) {
        act(() => event('message.start', admission))
      }

      act(() => event('message.delta', admission, { text: REPLY }))
      act(() =>
        event('message.complete', admission, {
          text: REPLY,
          persisted_turn: { complete: true, row_ids: [21, 22], submission_id: submissionId, user_row_ids: [21] }
        })
      )
    },
    dispose: () => {
      composer.unmount()
      core.unmount()
      queryClient.clear()
    }
  }
}

type Harness = ReturnType<typeof mount>

const HISTORY: ChatMessage[] = [
  { id: 'user-history', role: 'user', parts: [textPart('look at the prod logs')], rowId: 1 },
  { id: 'assistant-history', role: 'assistant', parts: [textPart('Prod logs look clean.')], rowId: 2 }
]

/** The previous reply is still streaming: the session is busy and its turn is live. */
const liveTurn = (h: Harness) =>
  h.update(state => ({ ...state, messages: HISTORY, busy: true, awaitingResponse: true, turnLive: true }))

/** The previous reply already ended: the slice is idle. */
const settledTurn = (h: Harness) =>
  h.update(state => ({ ...state, messages: HISTORY, busy: false, awaitingResponse: false, turnLive: false }))

const userRows = (h: Harness, text: string) =>
  h.messages().filter(message => message.role === 'user' && chatMessageText(message) === text)

beforeEach(() => {
  window.localStorage.clear()
  // The real Desktop talks the canonical wire (native dial): the server owns the composer queue.
  $connection.set({
    mode: 'local',
    connectionId: 'local',
    profile: 'default',
    baseUrl: 'http://127.0.0.1:1',
    wsUrl: 'ws://127.0.0.1:1/api/ws?native_dial=1',
    token: '',
    logs: [],
    isFullscreen: false,
    nativeOverlayWidth: 0,
    windowButtonPosition: null
  })
})

afterEach(() => {
  cleanup()
  clearQueuedPrompts(`queued-resend-stored-${seq}`)
  clearQueuedPromptBubbles()
  $connection.set(null)
  setMessages([])
  window.localStorage.clear()
  vi.restoreAllMocks()
})

describe('a busy-Enter message resent through the server queue', () => {
  it.each([
    { receipt: 'started', startBeforeReceipt: false },
    { receipt: 'queued', startBeforeReceipt: false },
    { receipt: 'started', startBeforeReceipt: true }
  ] as const)(
    'gateway rejects the redirect, receipt=$receipt, startBeforeReceipt=$startBeforeReceipt: the typed message is drawn as its turn runs',
    async ({ receipt, startBeforeReceipt }) => {
      const h = mount({ redirect: 'rejected', receipt, startBeforeReceipt })
      liveTurn(h)
      h.busyRef.current = true

      await h.typeAndEnter(TYPED)

      // The path under test: redirect refused by the gateway, then the composer's canonical keep() resend.
      expect(h.methods()).toEqual(['session.redirect', 'prompt.submit'])
      expect(h.calls[1].params).toMatchObject({ queued: true, session_id: h.SID, text: TYPED })

      // The previous reply ends, then the resent admission runs as the next turn.
      h.event('message.complete', 'adm-previous', { text: 'Prod logs look clean.' })
      await h.runQueuedTurn(h.submissions[0])

      const rows = userRows(h, TYPED)
      expect(rows).toHaveLength(1)
      // Drawn before the reply it prompted, and bound to its stored row (no duplicate on a later history load).
      const ids = h.messages().map(message => message.id)
      const replyIndex = h.messages().findIndex(m => m.role === 'assistant' && chatMessageText(m).includes(REPLY))
      expect(ids.indexOf(rows[0].id)).toBeLessThan(replyIndex)
      expect(rows[0].rowId).toBe(21)
      h.dispose()
    }
  )

  it('a resume projection of the still-queued send (user-queued- row) is not drawn twice when its turn starts', async () => {
    const h = mount({ redirect: 'rejected', receipt: 'queued' })
    liveTurn(h)
    h.busyRef.current = true

    await h.typeAndEnter(TYPED)
    h.event('message.complete', 'adm-previous', { text: 'Prod logs look clean.' })
    // A resume while the admission waited projected it as the next-turn queued row.
    h.update(state => ({
      ...state,
      messages: [...state.messages, { id: `user-queued-${h.SID}`, role: 'user', parts: [textPart(TYPED)] }]
    }))
    await h.runQueuedTurn(h.submissions[0])

    expect(userRows(h, TYPED)).toHaveLength(1)
    h.dispose()
  })

  it('stale composer busy flag (no redirect RPC): the typed message is drawn as its turn runs', async () => {
    const h = mount({ receipt: 'started', staleBusy: true })
    settledTurn(h)
    h.busyRef.current = true

    await h.typeAndEnter(TYPED)

    // Refused before any RPC (index.ts stale-belief guard), then resent through the server queue.
    expect(h.methods()).toEqual(['prompt.submit'])
    expect(h.calls[0].params).toMatchObject({ queued: true, session_id: h.SID, text: TYPED })

    await h.runQueuedTurn(h.submissions[0])

    const rows = userRows(h, TYPED)
    expect(rows).toHaveLength(1)
    expect(rows[0].rowId).toBe(21)
    h.dispose()
  })

  // Stop (cancelRun sets `interrupted`) before the queued admission's start: message.start treats that start as
  // stale and arms nothing, so the prompt is not drawn either, and the kept prompt is dropped rather than drawn by
  // a later start. The stored turn still holds the user's words; a history load shows them.
  it('Stop before the queued turn starts: the stale start draws nothing and the kept prompt is dropped', async () => {
    const h = mount({ redirect: 'rejected', receipt: 'queued' })
    liveTurn(h)
    h.busyRef.current = true

    await h.typeAndEnter(TYPED)
    expect(waitingQueuedPromptCount()).toBe(1)
    h.event('message.complete', 'adm-previous', { text: 'Prod logs look clean.' })
    h.update(state => ({ ...state, interrupted: true }))

    h.event('message.start', h.admissionOf(h.submissions[0]))

    expect(userRows(h, TYPED)).toHaveLength(0)
    expect(waitingQueuedPromptCount()).toBe(0)

    // Even a start for the same admission once the Stop is over draws nothing: the prompt was dropped.
    h.update(state => ({ ...state, interrupted: false }))
    h.event('message.start', h.admissionOf(h.submissions[0]))
    expect(userRows(h, TYPED)).toHaveLength(0)
    h.dispose()
  })

  it('Stop before a start that beat the receipt: the receipt does not draw it either', async () => {
    const h = mount({ redirect: 'rejected', receipt: 'started', startBeforeReceipt: true })
    liveTurn(h)
    h.busyRef.current = true
    h.update(state => ({ ...state, interrupted: true }))

    await h.typeAndEnter(TYPED)

    expect(h.methods()).toContain('prompt.submit')
    expect(userRows(h, TYPED)).toHaveLength(0)
    expect(waitingQueuedPromptCount()).toBe(0)
    h.dispose()
  })

  it('a removed session forgets the prompt it was keeping', async () => {
    const h = mount({ redirect: 'rejected', receipt: 'queued' })
    liveTurn(h)
    h.busyRef.current = true

    await h.typeAndEnter(TYPED)
    expect(waitingQueuedPromptCount()).toBe(1)

    act(() => dropSessionState(h.SID))

    expect(waitingQueuedPromptCount()).toBe(0)
    h.dispose()
  })
})

describe('controls', () => {
  it('an idle plain send (no queue, no busy) draws its bubble at once and keeps it through the turn', async () => {
    const h = mount({ receipt: 'started' })
    settledTurn(h)

    // The ChatBar's idle Enter: submitText with no queue flag.
    await h.submit(TYPED)
    expect(h.calls[0].params).not.toHaveProperty('queued')
    expect(userRows(h, TYPED)).toHaveLength(1) // drawn at send, before any event

    await h.runQueuedTurn(h.submissions[0])

    const rows = userRows(h, TYPED)
    expect(rows).toHaveLength(1)
    expect(rows[0].rowId).toBe(21)
    h.dispose()
  })

  it.each(['redirected', 'queued'] as const)(
    'an accepted redirect (%s) keeps its own bubble and sends nothing through the queue',
    async redirect => {
      const h = mount({ redirect })
      liveTurn(h)
      h.busyRef.current = true

      await h.typeAndEnter(TYPED)

      expect(h.methods()).toEqual(['session.redirect'])
      expect(userRows(h, TYPED)).toHaveLength(1)

      if (redirect === 'queued') {
        // The redirect was queued as the next turn; that turn runs. Still one row.
        h.event('message.complete', 'adm-previous', { text: 'Prod logs look clean.' })
        h.event('message.start', 'adm-redirect')
        h.event('message.delta', 'adm-redirect', { text: REPLY })
        h.event('message.complete', 'adm-redirect', { text: REPLY })
        expect(userRows(h, TYPED)).toHaveLength(1)
      }

      h.dispose()
    }
  )
})
