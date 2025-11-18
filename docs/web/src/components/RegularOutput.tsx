import { useMemo } from 'react'

interface RegularOutputProps {
  code: string | string[]
}

export function RegularOutput({ code }: RegularOutputProps) {
  if (!code) {
    console.error('RegularOutput: code prop is missing')
    return <div className="my-6 p-6 bg-destructive/10 border border-destructive rounded-lg text-destructive">Error: No output data</div>
  }

  // Handle array of outputs (multiple variables) - display in same component, separate rows
  if (Array.isArray(code)) {
    return (
      <div className="-mt-6 mb-6 p-6 bg-muted border border-border rounded-lg">
        <div className="text-xs font-medium text-muted-foreground mb-3 uppercase tracking-wide">
          Output
        </div>
        <div className="text-foreground space-y-3">
          {code.map((output, index) => (
            <div key={index} className={index > 0 ? 'pt-3 border-t border-border' : ''}>
              <OutputContent code={output} />
            </div>
          ))}
        </div>
      </div>
    )
  }

  // Single output
  return (
    <div className="-mt-6 mb-6 p-6 bg-muted border border-border rounded-lg">
      <div className="text-xs font-medium text-muted-foreground mb-3 uppercase tracking-wide">
        Output
      </div>
      <div className="text-foreground">
        <OutputContent code={code} />
      </div>
    </div>
  )
}

// Separate component to format output content
function OutputContent({ code }: { code: string }) {
  const formattedOutput = useMemo(() => {
    // Try to parse the output as JSON first (for structured data)
    try {
      const parsed = JSON.parse(code)

      // Handle arrays
      if (Array.isArray(parsed)) {
        return (
          <ul className="list-disc list-inside space-y-2">
            {parsed.map((item, idx) => (
              <li key={idx} className="ml-4">
                {typeof item === 'object' ? JSON.stringify(item, null, 2) : String(item)}
              </li>
            ))}
          </ul>
        )
      }

      // Handle objects
      if (typeof parsed === 'object' && parsed !== null) {
        return (
          <div className="space-y-2">
            {Object.entries(parsed).map(([key, value]) => (
              <div key={key} className="flex gap-2">
                <span className="font-semibold">{key}:</span>
                <span>{typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value)}</span>
              </div>
            ))}
          </div>
        )
      }

      // Primitive JSON value
      return <div>{String(parsed)}</div>
    } catch {
      // Not JSON, try to detect structured text patterns
      const lines = code.trim().split('\n')

      // Check if it looks like a list (lines starting with -, *, or numbers)
      const isList = lines.every(line =>
        /^\s*[-*•]\s/.test(line) || /^\s*\d+\.\s/.test(line)
      )

      if (isList) {
        return (
          <ul className="list-disc list-inside space-y-2">
            {lines.map((line, idx) => (
              <li key={idx} className="ml-4">
                {line.replace(/^\s*[-*•]\s/, '').replace(/^\s*\d+\.\s/, '')}
              </li>
            ))}
          </ul>
        )
      }

      // Check if it's a Python list/tuple/dict representation
      if (/^[\[\(\{]/.test(code.trim()) && /[\]\)\}]$/.test(code.trim())) {
        return (
          <pre className="font-mono text-sm overflow-x-auto whitespace-pre-wrap">
            {code}
          </pre>
        )
      }

      // Plain text with multiple lines
      if (lines.length > 1) {
        return (
          <div className="space-y-1">
            {lines.map((line, idx) => (
              <div key={idx}>{line}</div>
            ))}
          </div>
        )
      }

      // Single line text
      return <div>{code}</div>
    }
  }, [code])

  return <>{formattedOutput}</>
}
