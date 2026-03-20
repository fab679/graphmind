import { createContext, useContext, useState } from 'react'
import type { ReactNode } from 'react'
import { cn } from '@/lib/utils'

const TabsContext = createContext<{ value: string; onChange: (v: string) => void }>({ value: '', onChange: () => {} })

export function Tabs({ defaultValue, children, className }: { defaultValue: string; children: ReactNode; className?: string }) {
  const [value, setValue] = useState(defaultValue)
  return (
    <TabsContext.Provider value={{ value, onChange: setValue }}>
      <div className={cn('flex flex-col', className)}>{children}</div>
    </TabsContext.Provider>
  )
}

export function TabsList({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn('flex gap-1 border-b border-border px-2', className)}>{children}</div>
}

export function TabsTrigger({ value, children, className }: { value: string; children: ReactNode; className?: string }) {
  const ctx = useContext(TabsContext)
  return (
    <button
      className={cn(
        'px-3 py-1.5 text-xs font-medium transition-colors',
        ctx.value === value ? 'text-primary border-b-2 border-primary' : 'text-muted-foreground hover:text-foreground',
        className
      )}
      onClick={() => ctx.onChange(value)}
    >
      {children}
    </button>
  )
}

export function TabsContent({ value, children, className }: { value: string; children: ReactNode; className?: string }) {
  const ctx = useContext(TabsContext)
  if (ctx.value !== value) return null
  return <div className={cn('flex-1 overflow-auto', className)}>{children}</div>
}
