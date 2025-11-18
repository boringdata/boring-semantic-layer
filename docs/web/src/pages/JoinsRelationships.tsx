import { useEffect, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import BSLQueryResult from '../components/BSLQueryResult'
import { CodeBlock } from '@/components/CodeBlock'
import { Footer } from '@/components/Footer'
import { TableOfContents } from '@/components/TableOfContents'

interface PageData {
  markdown: string
  queries: Record<string, any>
}

// Helper function to generate IDs from heading text
function generateId(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '')
}

export default function JoinsRelationships() {
  const [pageData, setPageData] = useState<PageData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    // Load pre-computed page data
    fetch('/bsl-data/joins.json')
      .then(res => {
        if (!res.ok) throw new Error('Page not found')
        return res.json()
      })
      .then(data => {
        setPageData(data)
        setLoading(false)
      })
      .catch(err => {
        console.error('Failed to load page:', err)
        setError(err.message)
        setLoading(false)
      })
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-lg">Loading...</div>
      </div>
    )
  }

  if (error || !pageData) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-red-500">Page not found: joins</div>
      </div>
    )
  }

  // Replace <BSLQuery query="name" /> with actual components
  const processedMarkdown = pageData.markdown.replace(
    /<BSLQuery\s+query="(\w+)"\s*\/>/g,
    (match, queryName) => {
      return `{{BSL_QUERY:${queryName}}}`
    }
  )

  return (
    <>
      <div className="relative">
        <section className="px-6 py-24 bg-background">
          <div className="max-w-7xl mx-auto">
            <div className="flex gap-8">
              <main className="flex-1 max-w-4xl space-y-8 min-w-0">
              <ReactMarkdown
                components={{
                  // H1
                  h1({ children }) {
                    const text = String(children)
                    const id = generateId(text)
                    return (
                      <h1 id={id} className="text-3xl md:text-4xl font-bold mb-4">
                        {children}
                      </h1>
                    )
                  },
                  // H2
                  h2({ children }) {
                    const text = String(children)
                    const id = generateId(text)
                    return (
                      <h2 id={id} className="text-2xl md:text-3xl font-bold mt-12 mb-6">
                        {children}
                      </h2>
                    )
                  },
                  // H3
                  h3({ children }) {
                    const text = String(children)
                    const id = generateId(text)
                    return (
                      <h3 id={id} className="text-xl md:text-2xl font-semibold mt-8 mb-4">
                        {children}
                      </h3>
                    )
                  },
              // Paragraph
              p({ children }) {
                const text = String(children)
                const match = /\{\{BSL_QUERY:(\w+)\}\}/.exec(text)

                if (match) {
                  const queryName = match[1]
                  if (pageData.queries[queryName]) {
                    return <BSLQueryResult data={pageData.queries[queryName]} name={queryName} />
                  }
                }

                return <p className="text-lg text-muted-foreground mb-4">{children}</p>
              },
              // Code blocks
              code(props) {
                const { children, className, ...rest } = props
                const codeString = String(children).replace(/\n$/, '')
                const match = /language-(\w+)/.exec(className || '')

                // Check if inline by looking for className (block code has language-* class)
                const isInline = !className || !className.startsWith('language-')

                // Inline code - single backticks
                if (isInline) {
                  return (
                    <code className="px-1.5 py-0.5 rounded bg-muted text-sm font-mono" {...rest}>
                      {children}
                    </code>
                  )
                }

                // Block code with language specified (triple backticks with language)
                if (match) {
                  return <CodeBlock code={codeString} language={match[1]} />
                }

                // Block code without language (triple backticks without language)
                return <CodeBlock code={codeString} language="python" />
              },
              // Lists
              ul({ children }) {
                return <ul className="list-disc list-inside space-y-2 text-lg text-muted-foreground mb-4">{children}</ul>
              },
              li({ children }) {
                return <li className="ml-4">{children}</li>
              },
                }}
              >
                {processedMarkdown}
              </ReactMarkdown>
              </main>
              <aside className="w-48 shrink-0">
                {/* Empty space for TOC */}
              </aside>
            </div>
          </div>
        </section>

        {/* Fixed TOC outside the scrolling section */}
        <div className="fixed top-24 right-8 w-48 hidden lg:block">
          <TableOfContents />
        </div>
      </div>
      <Footer />
    </>
  )
}
