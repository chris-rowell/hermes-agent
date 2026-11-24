// Lowest-level confirmation: submit.ts's optimistic seed for a server-queue send. With the canonical wire the composer
// queue is server-owned, so `fromQueue: true` makes the send a queue admission even when the session is IDLE, and
// seedOptimistic returns before drawing anything. This pins today's behavior (no row at send time); the transcript
// gap it leaves is covered end to end by queued-resend-bubble.test.tsx.
import { act, cleanup, renderHook } from '@testing-library/react'
import { useRef } from 'react'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'

import { en } from '@/i18n/en'
import { chatMessageText } from '@/lib/chat-messages'
import {
  $connection,
  setActiveSessionId,
  setAwaitingResponse,
  setBusy,
  setMessages,
  setSelectedStoredSessionId,
  setSessions
} from '@/store/session'
import { clearAllSessionStates } from '@/store/session-states'
import { makeSessionInfo } from '@/test/session-info'

import { useSessionStateCache } from '../use-session-state-cache'

import { useSubmitPrompt } from './submit'

const SID = 'local-idle-queue-seed'
const INPUT = 'resent after a refused redirect'

function mount() {
  const request = vi.fn(async (_method: string, params: Record<string, unknown>) => ({
    admission_id: 'admission-1',
    submission_id: params.submission_id,
    session_id: SID,
    status: 'started'
  }))

  const hook = renderHook(() => {
    const busyRef = useRef(false)

    const cache = useSessionStateCache({
      activeSessionId: SID,
      selectedStoredSessionId: SID,
      busyRef,
      setMessages,
      setBusy,
      setAwaitingResponse
    })

    const submit = useSubmitPrompt({
      ...cache,
      busyRef,
      copy: en.desktop,
      createBackendSessionForSend: async () => null,
      getRoutedStoredSessionId: () => SID,
      getRouteToken: () => `/${SID}`,
      requestGateway: request as Parameters<typeof useSubmitPrompt>[0]['requestGateway'],
      resumeStoredSession: async () => undefined,
      syncAttachmentsForSubmit: async (sessionId, attachments) => ({ sessionId, attachments })
    })

    return { cache, submit }
  })

  act(() => {
    hook.result.current.cache.updateSessionState(SID, state => state, SID)
  })

  return { hook, request }
}

beforeEach(() => {
  window.localStorage.clear()
  clearAllSessionStates()
  setMessages([])
  setBusy(false)
  setAwaitingResponse(false)
  setActiveSessionId(SID)
  setSelectedStoredSessionId(SID)
  setSessions([makeSessionInfo({ id: SID, source: 'gui', message_count: 0 })])
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
  clearAllSessionStates()
  setMessages([])
  setBusy(false)
  setAwaitingResponse(false)
  setActiveSessionId(null)
  setSelectedStoredSessionId(null)
  setSessions([])
  $connection.set(null)
  vi.restoreAllMocks()
})

it('an IDLE session: a fromQueue send is a queue admission and seeds no user row', async () => {
  const { hook, request } = mount()
  const state = () => hook.result.current.cache.sessionStateByRuntimeIdRef.current.get(SID)

  expect(state()?.busy ?? false).toBe(false)

  await act(async () => {
    expect(await hook.result.current.submit(INPUT, { fromQueue: true })).toBe(true)
  })

  expect(request).toHaveBeenCalledWith(
    'prompt.submit',
    expect.objectContaining({ queued: true, text: INPUT }),
    expect.any(Number)
  )
  expect((state()?.messages ?? []).some(message => message.role === 'user' && chatMessageText(message) === INPUT)).toBe(
    false
  )
})

it('control: the same idle send WITHOUT fromQueue seeds the user row at once', async () => {
  const { hook, request } = mount()
  const state = () => hook.result.current.cache.sessionStateByRuntimeIdRef.current.get(SID)

  await act(async () => {
    expect(await hook.result.current.submit(INPUT)).toBe(true)
  })

  expect(request).toHaveBeenCalledWith(
    'prompt.submit',
    expect.not.objectContaining({ queued: true }),
    expect.any(Number)
  )
  expect((state()?.messages ?? []).filter(message => message.role === 'user').map(chatMessageText)).toEqual([INPUT])
})
