import { host, PALETTE_AREA } from '@hermes/plugin-sdk'

export default {
  id: 'discord-sync',
  name: 'Discord sync',
  defaultEnabled: false,
  register(ctx) {
    let disposed = false
    let autoLink = true
    let starting = null
    let pending = null
    const linked = new Set()
    const refreshing = new Set()

    function start() {
      if (!starting) {
        starting = ctx.rest('/start', { method: 'POST' }).then(result => {
          autoLink = result.auto_link !== false
        }).catch(error => {
          starting = null
          throw error
        })
      }
      return starting
    }

    async function syncFocused(explicit = false) {
      const profile = host.state.focusedSessionProfile.get()
      const session = host.state.focusedStoredSessionId.get()
      if (disposed || !session || (profile && profile !== 'default')) return
      await start()
      if (!explicit && (!autoLink || linked.has(session))) return
      const result = await ctx.rest('/link', {
        method: 'POST', body: { session_id: session }, timeoutMs: 120000
      })
      linked.add(session)
      if (explicit) await ctx.os.openExternal(result.url)
    }

    function maybeSync() {
      clearTimeout(pending)
      pending = setTimeout(() => {
        void syncFocused().catch(error => host.notify({ kind: 'error', message: `discord sync: ${error.message}` }))
      }, 700)
    }

    const unsubscribe = host.state.focusedStoredSessionId.subscribe(maybeSync)
    const events = host.onEvent('plugin.discord-sync.transcript', event => {
      const session = event.payload?.session_id
      if (disposed || !session || refreshing.has(session)) return
      refreshing.add(session)
      // This SDK operation refreshes existing tiles in place. It never focuses
      // or opens an unrelated conversation, nor submits a second agent turn.
      void host.openSession(session, { profile: 'default', refreshInPlace: true })
        .catch(error => host.notify({ kind: 'error', message: `discord sync: ${error.message}` }))
        .finally(() => refreshing.delete(session))
    })

    ctx.register({
      id: 'sync-current-session', area: PALETTE_AREA,
      data: {
        id: 'discord-sync.sync-current-session',
        label: 'Sync this session with Discord',
        run: () => syncFocused(true).catch(error => host.notify({ kind: 'error', message: error.message }))
      }
    })
    void start().then(maybeSync).catch(error => host.notify({ kind: 'error', message: `discord sync: ${error.message}` }))
    ctx.onDispose(() => {
      disposed = true
      clearTimeout(pending)
      unsubscribe()
      events()
    })
  }
}
