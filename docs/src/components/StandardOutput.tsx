import { CodeBlock } from './CodeBlock'

interface StandardOutputProps {
  data: any
  name?: string
}

/**
 * Component for displaying standard Python output (not BSL query results)
 * Used for showing dimensions, measures, and other non-tabular data
 */
export function StandardOutput({ data, name }: StandardOutputProps) {
  // Handle error output
  if (data.error) {
    return (
      <div className="my-6 p-4 bg-destructive/10 border border-destructive/30 rounded-lg">
        <h4 className="text-destructive font-semibold mb-2">Error{name ? `: ${name}` : ''}</h4>
        <pre className="text-destructive text-sm font-mono overflow-x-auto">
          {data.error}
        </pre>
      </div>
    )
  }

  // Handle standard output (like print statements)
  if (data.output) {
    return (
      <div className="my-6 p-4 bg-muted/50 border border-border rounded-lg">
        {name && <h4 className="text-sm font-semibold text-muted-foreground mb-2">{name}</h4>}
        <pre className="text-sm font-mono text-foreground overflow-x-auto whitespace-pre-wrap">
          {data.output}
        </pre>
      </div>
    )
  }

  // Handle dictionary/list display
  if (data.display) {
    return (
      <div className="my-6 p-4 bg-muted/50 border border-border rounded-lg">
        {name && <h4 className="text-sm font-semibold text-muted-foreground mb-2">{name}</h4>}
        <CodeBlock code={data.display} language="python" />
      </div>
    )
  }

  // Handle message display
  if (data.message) {
    return (
      <div className="my-6 p-4 bg-primary/10 border border-primary/30 rounded-lg">
        <p className="text-sm text-foreground">{data.message}</p>
      </div>
    )
  }

  return null
}
