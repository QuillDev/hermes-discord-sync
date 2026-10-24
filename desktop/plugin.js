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
    let profiles = new Set(['default'])
    let starting = null
    let pendingAutoLink = null
    const linked = new Set()
    const refreshing = new Set()

    function rememberSettings(result) {
      autoLink = result.auto_link !== false
      profiles = new Set(result.profiles || ['default'])
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

    // Each synced profile has its own forum; sessions of other profiles stay off Discord.
    function focusedSyncedSession() {
      const profile = host.state.focusedSessionProfile.get() || 'default'
      const session = host.state.focusedStoredSessionId.get()
      if (disposed || !session || !profiles.has(profile)) {
        return null
      }
      return { session, profile }
    }

    async function linkSession({ session, profile }) {
      const result = await ctx.rest('/link', {
        method: 'POST',
        body: { session_id: session, profile },
        timeoutMs: LINK_TIMEOUT_MS
      })
      linked.add(`${profile}/${session}`)
      return result
    }

    async function autoLinkFocused() {
      await start()
      const focused = focusedSyncedSession()
      if (!focused) {
        return
      }
      const shouldLink = autoLink && !linked.has(`${focused.profile}/${focused.session}`)
      if (!shouldLink) {
        return
      }
      await linkSession(focused)
    }

    async function linkFocusedAndOpen() {
      await start()
      const focused = focusedSyncedSession()
      if (!focused) {
        return
      }
      const result = await linkSession(focused)
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
      const profile = event.payload?.profile || 'default'
      const key = `${profile}/${session}`
      if (disposed || !session || refreshing.has(key)) {
        return
      }
      refreshing.add(key)
      // This SDK operation refreshes existing tiles in place. It never focuses
      // or opens an unrelated conversation, nor submits a second agent turn.
      void host
        .openSession(session, { profile, refreshInPlace: true })
        .catch(reportError)
        .finally(() => refreshing.delete(key))
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
