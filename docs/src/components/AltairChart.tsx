import { useEffect, useRef } from 'react'
import embed from 'vega-embed'
import { useTheme } from 'next-themes'

interface AltairChartProps {
  spec: any
}

/**
 * Component to display Altair/Vega-Lite charts
 * Used with <altairchart code-block="..."></altairchart> in markdown
 */
export function AltairChart({ spec }: AltairChartProps) {
  const chartRef = useRef<HTMLDivElement>(null)
  const { theme, resolvedTheme } = useTheme()

  useEffect(() => {
    if (chartRef.current && spec) {
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
        ...spec,
        config: {
          ...spec.config,
          ...config
        }
      }

      embed(chartRef.current, themedSpec, {
        actions: false,
        renderer: 'svg'
      }).catch(err => console.error('Error rendering Altair chart:', err))
    }
  }, [spec, theme, resolvedTheme])

  return (
    <div className="my-8 border rounded-lg overflow-hidden bg-card p-6">
      <div ref={chartRef} style={{ width: '100%' }} />
    </div>
  )
}
