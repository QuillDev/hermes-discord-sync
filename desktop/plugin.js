import { host, PALETTE_AREA } from '@hermes/plugin-sdk'

const AUTO_LINK_DELAY_MS = 700
const LINK_TIMEOUT_MS = 120000

function reportError(error) {
  host.notify({ kind: 'error', message: `discord sync: ${error.message}` })
}

export default {
  id: 'discord-sync',
  name: 'Discord sync',
  defaultEnabled: false,
  register(ctx) {
    let disposed = false
    let autoLink = true
    let starting = null
    let pendingAutoLink = null
    const linked = new Set()
    const refreshing = new Set()

    function rememberSettings(result) {
      autoLink = result.auto_link !== false
    }

    function retryStartLater(error) {
      starting = null
      throw error
    }

    function start() {
      if (!starting) {
        starting = ctx.rest('/start', { method: 'POST' }).then(rememberSettings).catch(retryStartLater)
      }
      return starting
    }

    function focusedDefaultSession() {
      const profile = host.state.focusedSessionProfile.get()
      const session = host.state.focusedStoredSessionId.get()
      const otherProfile = Boolean(profile) && profile !== 'default'
      if (disposed || !session || otherProfile) {
        return null
      }
      return session
    }

    async function linkSession(session) {
      const result = await ctx.rest('/link', {
        method: 'POST',
        body: { session_id: session },
        timeoutMs: LINK_TIMEOUT_MS
      })
      linked.add(session)
      return result
    }

    async function autoLinkFocused() {
      const session = focusedDefaultSession()
      if (!session) {
        return
      }
      await start()
      const shouldLink = autoLink && !linked.has(session)
      if (!shouldLink) {
        return
      }
      await linkSession(session)
    }

    async function linkFocusedAndOpen() {
      const session = focusedDefaultSession()
      if (!session) {
        return
      }
      await start()
      const result = await linkSession(session)
      await ctx.os.openExternal(result.url)
    }

    function runAutoLink() {
      void autoLinkFocused().catch(reportError)
    }

    function scheduleAutoLink() {
      clearTimeout(pendingAutoLink)
      pendingAutoLink = setTimeout(runAutoLink, AUTO_LINK_DELAY_MS)
    }

    function refreshTranscript(event) {
      const session = event.payload?.session_id
      if (disposed || !session || refreshing.has(session)) {
        return
      }
      refreshing.add(session)
      // This SDK operation refreshes existing tiles in place. It never focuses
      // or opens an unrelated conversation, nor submits a second agent turn.
      void host
        .openSession(session, { profile: 'default', refreshInPlace: true })
        .catch(reportError)
        .finally(() => refreshing.delete(session))
    }

    function dispose() {
      disposed = true
      clearTimeout(pendingAutoLink)
      unsubscribeFocus()
      unsubscribeEvents()
    }

    const unsubscribeFocus = host.state.focusedStoredSessionId.subscribe(scheduleAutoLink)
    const unsubscribeEvents = host.onEvent('plugin.discord-sync.transcript', refreshTranscript)
    ctx.register({
      id: 'sync-current-session',
      area: PALETTE_AREA,
      data: {
        id: 'discord-sync.sync-current-session',
        label: 'Sync this session with Discord',
        run: () => linkFocusedAndOpen().catch(reportError)
      }
    })
    void start().then(scheduleAutoLink).catch(reportError)
    ctx.onDispose(dispose)
  }
}
