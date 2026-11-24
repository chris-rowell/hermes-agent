export type LiveTurnVerdict = 'ended' | 'running' | 'unknown'

export interface LiveTurnStatusResponse {
  sessions?: { id?: string; status?: string }[]
}

/** What one `session.active_list` snapshot says about `runtimeId`'s turn. A
 *  runtime missing from a well-formed list has been reaped: its turn is over.
 *  `starting` is an agent build for a turn the backend accepted. */
export function liveTurnVerdict(
  response: LiveTurnStatusResponse | null | undefined,
  runtimeId: string
): LiveTurnVerdict {
  if (!Array.isArray(response?.sessions)) {
    return 'unknown'
  }

  const status = response.sessions.find(session => session.id?.trim() === runtimeId)?.status

  if (status === undefined || status === 'idle') {
    return 'ended'
  }

  return status === 'starting' || status === 'waiting' || status === 'working' ? 'running' : 'unknown'
}
