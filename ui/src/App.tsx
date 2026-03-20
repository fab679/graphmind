import { useEffect, useCallback, useRef, useState } from 'react'
import { ThemeProvider } from '@/components/theme-provider'
import { AppShell } from '@/components/layout/AppShell'
import { KeyboardShortcutsHelp } from '@/components/graph/KeyboardShortcutsHelp'
import { LoginScreen } from '@/components/auth/LoginScreen'
import { useKeyboardShortcuts } from '@/lib/shortcuts'
import {
  getStatus,
  getSchema,
  listGraphs,
  login,
  setBasicAuth,
  clearAuth,
  isAuthenticated,
} from '@/api/client'
import { useUiStore } from '@/stores/uiStore'
import { useQueryStore } from '@/stores/queryStore'
import { useGraphStore } from '@/stores/graphStore'
import { useGraphSettingsStore } from '@/stores/graphSettingsStore'

export default function App() {
  const setConnectionStatus = useUiStore((s) => s.setConnectionStatus)
  const setServerInfo = useUiStore((s) => s.setServerInfo)
  const setSchema = useUiStore((s) => s.setSchema)
  const [showShortcuts, setShowShortcuts] = useState(false)
  const [authRequired, setAuthRequired] = useState<boolean | null>(null) // null = checking
  const [loggedIn, setLoggedIn] = useState(isAuthenticated())

  const prevNodeCountRef = useRef<number | null>(null)

  const checkStatus = useCallback(async () => {
    try {
      const activeGraph = useUiStore.getState().activeGraph
      const status = await getStatus(activeGraph)
      setConnectionStatus('connected')
      setServerInfo(status.version, status.storage.nodes, status.storage.edges)

      // Clear persisted settings when database becomes empty (new session)
      if (status.storage.nodes === 0 && prevNodeCountRef.current !== null && prevNodeCountRef.current > 0) {
        useGraphSettingsStore.getState().resetAll()
        useGraphStore.getState().clearGraph()
      }
      prevNodeCountRef.current = status.storage.nodes

      // If we got status successfully, auth is either not required or we're authenticated
      if (authRequired === null) {
        setAuthRequired(false)
        setLoggedIn(true)
      }
    } catch (err) {
      if (err instanceof Error && (err.message.includes('401') || err.message.includes('Unauthorized'))) {
        setConnectionStatus('disconnected')
        if (!isAuthenticated()) {
          setAuthRequired(true)
          setLoggedIn(false)
        }
      } else {
        setConnectionStatus('disconnected')
        // Server not reachable — not an auth issue, let them through
        if (authRequired === null) {
          setAuthRequired(false)
          setLoggedIn(true)
        }
      }
    }
  }, [setConnectionStatus, setServerInfo, authRequired])

  const loadSchema = useCallback(async () => {
    try {
      const activeGraph = useUiStore.getState().activeGraph
      const schema = await getSchema(activeGraph)
      setSchema(schema)
    } catch {
      // Schema not available yet
    }
  }, [setSchema])

  useEffect(() => {
    checkStatus()
    loadSchema()
    listGraphs()
      .then((graphs) => useUiStore.getState().setAvailableGraphs(graphs))
      .catch(() => {})

    const interval = setInterval(checkStatus, 5000)
    return () => clearInterval(interval)
  }, [checkStatus, loadSchema])

  const handleLogin = useCallback(async (username: string, password: string): Promise<boolean> => {
    try {
      const result = await login(username, password)
      if (result.authenticated) {
        setBasicAuth(username, password)
        setLoggedIn(true)
        setAuthRequired(false)
        // Re-check status with new credentials
        checkStatus()
        loadSchema()
        return true
      }
      return false
    } catch {
      return false
    }
  }, [checkStatus, loadSchema])

  const handleSkipAuth = useCallback(() => {
    clearAuth()
    setLoggedIn(true)
    setAuthRequired(false)
  }, [])

  // Global keyboard shortcuts
  useKeyboardShortcuts([
    {
      key: 'F5',
      description: 'Run query',
      action: () => useQueryStore.getState().executeQuery(),
    },
    {
      key: 'Escape',
      description: 'Clear selection',
      action: () => useGraphStore.getState().clearSelection(),
    },
    {
      key: 'Delete',
      description: 'Remove selected from canvas',
      action: () => {
        const { selectedNode, nodes, edges, setGraphData } = useGraphStore.getState()
        if (selectedNode) {
          const filtered = nodes.filter((n) => n.id !== selectedNode.id)
          const filteredEdges = edges.filter(
            (e) => e.source !== selectedNode.id && e.target !== selectedNode.id
          )
          setGraphData(filtered, filteredEdges)
        }
      },
    },
    {
      key: 'ctrl+shift+h',
      description: 'Toggle highlight mode',
      action: () => useGraphSettingsStore.getState().toggleHighlightMode(),
    },
    {
      key: '?',
      description: 'Show keyboard shortcuts',
      action: () => setShowShortcuts(true),
    },
  ])

  // Show login screen if auth is required and user hasn't logged in
  if (authRequired === true && !loggedIn) {
    return (
      <ThemeProvider defaultTheme="dark" storageKey="graphmind-theme">
        <LoginScreen onLogin={handleLogin} onSkip={handleSkipAuth} />
      </ThemeProvider>
    )
  }

  // Still checking or ready — show the main app
  return (
    <ThemeProvider defaultTheme="dark" storageKey="graphmind-theme">
      <AppShell />
      <KeyboardShortcutsHelp open={showShortcuts} onClose={() => setShowShortcuts(false)} />
    </ThemeProvider>
  )
}
