import { useState, useEffect, useRef } from 'react'
import embed from 'vega-embed'
import { useTheme } from 'next-themes'
import { CodeBlock } from './CodeBlock'

interface BSLQueryResultProps {
  data: any
  name: string
}

type TabType = 'table' | 'chart' | 'sql' | 'plan'

function BSLQueryResult({ data, name }: BSLQueryResultProps) {
  const [activeTab, setActiveTab] = useState<TabType>('table')
  const [copied, setCopied] = useState(false)

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const code = data.code  // Query code from data
  const sql = data.sql    // Generated SQL from data
  const plan = data.plan  // Query plan from data

  // Handle error state
  if (data.error) {
    return (
      <div style={{
        margin: '2rem 0',
        padding: '1rem',
        background: '#fee',
        border: '1px solid #fcc',
        borderRadius: '6px'
      }}>
        <h4 style={{ color: '#c33' }}>Query Error: {name}</h4>
        <pre style={{ color: '#c33', fontSize: '0.9rem' }}>
          {data.error}
        </pre>
      </div>
    )
  }

  // Handle semantic table definition
  if (data.semantic_table) {
    return (
      <div style={{
        margin: '2rem 0',
        padding: '1rem',
        background: '#f0f9ff',
        border: '1px solid #bfdbfe',
        borderRadius: '6px'
      }}>
        <p style={{ margin: 0, color: '#1e40af' }}>
          ✓ Semantic table <strong>{data.name}</strong> defined
        </p>
      </div>
    )
  }

  // Render nothing if no table or chart data
  if (!data.table && !data.chart) {
    return null
  }

  const hasChart = data.chart && (data.chart.spec || data.chart.type)
  const hasTable = data.table && data.table.data && data.table.data.length > 0
  const hasSQL = sql && sql.length > 0
  const hasQueryPlan = plan && plan.length > 0

  return (
    <div className="-mt-6 mb-6 border rounded-lg overflow-hidden bg-card">
      {/* Tabs */}
      <div className="flex border-b bg-muted/30">
        <TabButton
          active={activeTab === 'table'}
          onClick={() => setActiveTab('table')}
          disabled={!hasTable}
        >
          <span className={`font-semibold ${activeTab === 'table' ? 'text-foreground' : 'text-muted-foreground'}`}>📊 Table:</span> <code className={activeTab === 'table' ? 'text-foreground' : 'text-muted-foreground'}>.execute()</code>
        </TabButton>
        <TabButton
          active={activeTab === 'chart'}
          onClick={() => setActiveTab('chart')}
          disabled={!hasChart}
        >
          <span className={`font-semibold ${activeTab === 'chart' ? 'text-foreground' : 'text-muted-foreground'}`}>📈 Chart:</span> <code className={activeTab === 'chart' ? 'text-foreground' : 'text-muted-foreground'}>.chart()</code>
        </TabButton>
        {hasSQL && (
          <TabButton
            active={activeTab === 'sql'}
            onClick={() => setActiveTab('sql')}
          >
            <span className={`font-semibold ${activeTab === 'sql' ? 'text-foreground' : 'text-muted-foreground'}`}>💻 SQL:</span> <code className={activeTab === 'sql' ? 'text-foreground' : 'text-muted-foreground'}>.sql()</code>
          </TabButton>
        )}
        {hasQueryPlan && (
          <TabButton
            active={activeTab === 'plan'}
            onClick={() => setActiveTab('plan')}
          >
            <span className={`font-semibold ${activeTab === 'plan' ? 'text-foreground' : 'text-muted-foreground'}`}>🗺️ Query Plan</span>
          </TabButton>
        )}
      </div>

      {/* Content */}
      <div className="p-6 bg-card">
        {activeTab === 'table' && hasTable && (
          <TableView data={data.table} />
        )}

        {activeTab === 'chart' && hasChart && (
          <ChartView data={data.chart} />
        )}

        {activeTab === 'sql' && hasSQL && (
          <SQLView sql={sql} />
        )}

        {activeTab === 'plan' && hasQueryPlan && (
          <QueryPlanView plan={plan} />
        )}
      </div>
    </div>
  )
}

// Tab Button Component
function TabButton({
  active,
  onClick,
  children,
  disabled = false
}: {
  active: boolean
  onClick: () => void
  children: React.ReactNode
  disabled?: boolean
}) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`
        px-6 py-3 border-none transition-all text-sm font-medium
        ${active
          ? 'bg-card border-b-2 border-primary text-primary font-semibold'
          : 'bg-transparent border-b-2 border-transparent text-muted-foreground hover:text-foreground'
        }
        ${disabled ? 'cursor-not-allowed opacity-50' : 'cursor-pointer'}
      `}
    >
      {children}
    </button>
  )
}

// Helper function to format cell values
function formatCellValue(cell: any): string {
  if (cell === null || cell === undefined) {
    return 'null'
  }

  if (typeof cell === 'number') {
    return cell.toLocaleString()
  }

  if (typeof cell === 'string') {
    return cell
  }

  if (typeof cell === 'boolean') {
    return cell.toString()
  }

  // Handle arrays (including nested structs)
  if (Array.isArray(cell)) {
    // If it's an array of objects (nested structs), format nicely
    if (cell.length > 0 && typeof cell[0] === 'object' && cell[0] !== null) {
      return JSON.stringify(cell, null, 2)
    }
    // For simple arrays, use compact format
    return JSON.stringify(cell)
  }

  // Handle objects (structs)
  if (typeof cell === 'object') {
    return JSON.stringify(cell, null, 2)
  }

  return String(cell)
}

// Table View Component
function TableView({ data }: { data: { columns: string[], data: any[][] } }) {
  return (
    <div className="overflow-x-auto rounded-lg border">
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr className="bg-muted/50">
            <th className="px-4 py-3 text-left font-semibold text-muted-foreground border-b w-16">

            </th>
            {data.columns.map((col, idx) => (
              <th key={idx} className="px-4 py-3 text-left font-semibold text-foreground border-b">
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.data.map((row, rowIdx) => (
            <tr key={rowIdx} className={`border-b last:border-b-0 ${rowIdx % 2 === 0 ? 'bg-card' : 'bg-muted/20'}`}>
              <td className="px-4 py-3 text-muted-foreground font-medium">
                {rowIdx}
              </td>
              {row.map((cell, cellIdx) => {
                const formattedValue = formatCellValue(cell)
                const isComplex = formattedValue.includes('\n') || formattedValue.length > 50

                return (
                  <td key={cellIdx} className="px-4 py-3 text-foreground">
                    {isComplex ? (
                      <pre className="text-xs bg-muted/50 p-2 rounded overflow-x-auto max-w-md">
                        {formattedValue}
                      </pre>
                    ) : (
                      formattedValue
                    )}
                  </td>
                )
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// Chart View Component
function ChartView({ data }: { data: { type?: string; spec: any } }) {
  const chartRef = useRef<HTMLDivElement>(null)
  const { theme, resolvedTheme } = useTheme()
  const chartType = data.type || 'vega' // Default to vega for backward compatibility

  // Render Plotly chart
  if (chartType === 'plotly') {
    // Plotly support is currently disabled to avoid dependency bloat
    // since no charts in the docs currently use Plotly backend
    return (
      <div className="my-8 border rounded-lg overflow-hidden bg-card p-6">
        <div className="text-muted-foreground">
          Plotly charts are not currently supported in the documentation viewer.
          The chart data is available in the query results.
        </div>
      </div>
    )
  }

  // Render Vega-Lite chart (Altair)
  useEffect(() => {
    if (chartRef.current && data.spec) {
      const currentTheme = resolvedTheme || theme
      const isDark = currentTheme === 'dark'

      // Vega-Lite config for dark/light mode
      const config = isDark ? {
        background: 'transparent',
        axis: {
          domainColor: '#666',
          gridColor: '#444',
          tickColor: '#666',
          labelColor: '#ccc',
          titleColor: '#fff'
        },
        legend: {
          labelColor: '#ccc',
          titleColor: '#fff'
        },
        title: {
          color: '#fff'
        },
        view: {
          stroke: '#444'
        }
      } : {
        background: 'transparent',
        axis: {
          domainColor: '#ccc',
          gridColor: '#e5e5e5',
          tickColor: '#ccc',
          labelColor: '#666',
          titleColor: '#333'
        },
        legend: {
          labelColor: '#666',
          titleColor: '#333'
        },
        title: {
          color: '#333'
        },
        view: {
          stroke: '#e5e5e5'
        }
      }

      // Merge config with the spec
      const themedSpec = {
        ...data.spec,
        config: {
          ...data.spec.config,
          ...config
        }
      }

      embed(chartRef.current, themedSpec, {
        actions: false,
        renderer: 'svg'
      }).catch(err => console.error('Error rendering chart:', err))
    }
  }, [data.spec, theme, resolvedTheme])

  return (
    <div ref={chartRef} style={{ width: '100%' }} />
  )
}

// SQL View Component
function SQLView({ sql }: { sql: string }) {
  return (
    <div className="sql-view">
      <CodeBlock code={sql} language="sql" />
    </div>
  )
}

// Query Plan View Component
function QueryPlanView({ plan }: { plan: string }) {
  return (
    <div className="query-plan-view">
      <CodeBlock code={plan} language="text" />
    </div>
  )
}

export default BSLQueryResult
