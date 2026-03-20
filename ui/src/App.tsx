import { useEffect, useCallback, useRef, useState } from 'react'
import { ThemeProvider } from '@/components/theme-provider'
import { AppShell } from '@/components/layout/AppShell'
import { KeyboardShortcutsHelp } from '@/components/graph/KeyboardShortcutsHelp'
import { useKeyboardShortcuts } from '@/lib/shortcuts'
import { getStatus, getSchema, listGraphs } from '@/api/client'
import { useUiStore } from '@/stores/uiStore'
import { useQueryStore } from '@/stores/queryStore'
import { useGraphStore } from '@/stores/graphStore'
import { useGraphSettingsStore } from '@/stores/graphSettingsStore'

export default function App() {
  const setConnectionStatus = useUiStore((s) => s.setConnectionStatus)
  const setServerInfo = useUiStore((s) => s.setServerInfo)
  const setSchema = useUiStore((s) => s.setSchema)
  const [showShortcuts, setShowShortcuts] = useState(false)

  const prevNodeCountRef = useRef<number | null>(null)

  const checkStatus = useCallback(async () => {
    try {
      const status = await getStatus()
      setConnectionStatus('connected')
      setServerInfo(status.version, status.storage.nodes, status.storage.edges)

      // Clear persisted settings when database becomes empty (new session)
      if (status.storage.nodes === 0 && prevNodeCountRef.current !== null && prevNodeCountRef.current > 0) {
        useGraphSettingsStore.getState().resetAll()
        useGraphStore.getState().clearGraph()
      }
      prevNodeCountRef.current = status.storage.nodes
    } catch (err) {
      if (err instanceof Error && err.message.includes('401')) {
        setConnectionStatus('disconnected')
        // Auth required but no token — status endpoint is exempt but query endpoints will fail
      } else {
        setConnectionStatus('disconnected')
      }
    }
  }, [setConnectionStatus, setServerInfo])

  const loadSchema = useCallback(async () => {
    try {
      const schema = await getSchema()
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

  return (
    <ThemeProvider defaultTheme="dark" storageKey="graphmind-theme">
      <AppShell />
      <KeyboardShortcutsHelp open={showShortcuts} onClose={() => setShowShortcuts(false)} />
    </ThemeProvider>
  )
}
