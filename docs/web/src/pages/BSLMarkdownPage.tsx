import { useEffect, useState } from 'react'
import { useParams, useLocation, Link } from 'react-router-dom'
import ReactMarkdown from 'react-markdown'
import rehypeRaw from 'rehype-raw'
import remarkGfm from 'remark-gfm'
import BSLQueryResult from '../components/BSLQueryResult'
import { StandardOutput } from '@/components/StandardOutput'
import { CollapsibleSetup } from '@/components/CollapsibleSetup'
import { RegularOutput } from '@/components/RegularOutput'
import { Note } from '@/components/Note'
import { CodeBlock } from '@/components/CodeBlock'
import { Footer } from '@/components/Footer'
import { TableOfContents } from '@/components/TableOfContents'
import { AltairChart } from '@/components/AltairChart'
import { YamlContent } from '@/components/YamlContent'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'

interface PageData {
  markdown: string
  queries: Record<string, any>
  files?: Record<string, string>
}

// Helper function to generate IDs from heading text
function generateId(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '')
}

interface BSLMarkdownPageProps {
  pageSlug?: string  // Optional: if provided, use this instead of route param
}

export default function BSLMarkdownPage({ pageSlug }: BSLMarkdownPageProps = {}) {
  const { slug: routeSlug } = useParams()
  const location = useLocation()
  const slug = pageSlug || routeSlug
  const [pageData, setPageData] = useState<PageData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [collapsedBlocks, setCollapsedBlocks] = useState<Set<string>>(new Set())

  useEffect(() => {
    if (!slug) {
      setError('No page slug provided')
      setLoading(false)
      return
    }

    // Load pre-computed page data
    fetch(`${import.meta.env.BASE_URL}bsl-data/${slug}.json`)
      .then(res => {
        if (!res.ok) throw new Error('Page not found')
        return res.json()
      })
      .then(data => {
        setPageData(data)
        // Find all collapsedcodeblock references in the markdown
        const collapsed = new Set<string>()
        const regex = /<collapsedcodeblock\s+code-block="([^"]+)"/g
        let match
        while ((match = regex.exec(data.markdown)) !== null) {
          collapsed.add(match[1])
        }
        setCollapsedBlocks(collapsed)
        setLoading(false)
      })
      .catch(err => {
        console.error('Failed to load page:', err)
        setError(err.message)
        setLoading(false)
      })
  }, [slug])

  // Handle hash navigation and scrolling
  useEffect(() => {
    if (!loading && location.hash) {
      // Small delay to ensure content is rendered
      setTimeout(() => {
        const id = location.hash.replace('#', '')
        const element = document.getElementById(id)
        if (element) {
          element.scrollIntoView({ behavior: 'smooth', block: 'start' })
        }
      }, 100)
    }
  }, [loading, location.hash])

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
        <div className="text-red-500">Page not found: {slug}</div>
      </div>
    )
  }

  return (
    <>
      <div className="relative">
        <section className="px-6 py-24 bg-background">
          <div className="max-w-7xl mx-auto">
            <div className="flex gap-8">
              <main className="flex-1 max-w-4xl space-y-8 min-w-0">
              <ReactMarkdown
                remarkPlugins={[remarkGfm]}
                rehypePlugins={[rehypeRaw]}
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
              // Custom components
              bslquery({ node }: any) {
                const codeBlock = node.properties?.codeBlock || node.properties?.['code-block']
                if (codeBlock && pageData.queries[codeBlock]) {
                  return <BSLQueryResult data={pageData.queries[codeBlock]} name={codeBlock} />
                }
                return null
              },
              altairchart({ node }: any) {
                const codeBlock = node.properties?.codeBlock || node.properties?.['code-block']
                if (codeBlock && pageData.queries[codeBlock]?.chart_spec) {
                  return <AltairChart spec={pageData.queries[codeBlock].chart_spec} />
                }
                return null
              },
              collapsedcodeblock({ node }: any) {
                const codeBlock = node.properties?.codeBlock || node.properties?.['code-block']
                const title = node.properties?.title || 'Setup code'
                if (codeBlock && pageData.queries[codeBlock]?.code) {
                  return <CollapsibleSetup code={pageData.queries[codeBlock].code} title={title} />
                }
                return null
              },
              regularoutput({ node }: any) {
                const codeBlock = node.properties?.codeBlock || node.properties?.['code-block']
                if (codeBlock && pageData.queries[codeBlock]?.output) {
                  return <RegularOutput code={pageData.queries[codeBlock].output} />
                }
                return null
              },
              note({ node, children }: any) {
                const type = node.properties?.type || 'info'
                return <Note type={type}>{children}</Note>
              },
              yamlcontent({ node }: any) {
                const path = node.properties?.path
                if (path && pageData.files && pageData.files[path]) {
                  return <YamlContent path={path} content={pageData.files[path]} />
                }
                return <YamlContent path={path || 'unknown'} />
              },
              // Paragraph
              p({ children }) {
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

                const language = match ? match[1] : 'python'

                // Hide blocks that are referenced by collapsedcodeblock components
                if (collapsedBlocks.has(language)) {
                  return null
                }

                // Regular code block display
                return <CodeBlock code={codeString} language={language} />
              },
              // Lists
              ul({ children }) {
                return <ul className="list-disc list-inside space-y-2 text-lg text-muted-foreground mb-4">{children}</ul>
              },
              li({ children }) {
                return <li className="ml-4">{children}</li>
              },
              // Links
              a({ href, children }) {
                // Check if the link is internal (starts with /)
                const isInternal = href && href.startsWith('/')

                if (isInternal) {
                  return (
                    <Link
                      to={href}
                      className="text-primary underline decoration-primary/30 hover:decoration-primary transition-colors font-medium"
                    >
                      {children}
                    </Link>
                  )
                }

                // External links
                return (
                  <a
                    href={href}
                    className="text-primary underline decoration-primary/30 hover:decoration-primary transition-colors font-medium"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    {children}
                  </a>
                )
              },
              // Images
              img({ src, alt, ...props }) {
                // Handle root-relative paths for GitHub Pages
                const imageSrc = src?.startsWith('/')
                  ? `${import.meta.env.BASE_URL}${src.slice(1)}`
                  : src
                return (
                  <img
                    src={imageSrc}
                    alt={alt || ''}
                    className="my-6 rounded-lg max-w-full"
                    {...props}
                  />
                )
              },
              // Tables
              table({ children }) {
                return <Table className="my-6">{children}</Table>
              },
              thead({ children }) {
                return <TableHeader>{children}</TableHeader>
              },
              tbody({ children }) {
                return <TableBody>{children}</TableBody>
              },
              tr({ children }) {
                return <TableRow>{children}</TableRow>
              },
              th({ children }) {
                return <TableHead>{children}</TableHead>
              },
              td({ children }) {
                return <TableCell>{children}</TableCell>
              },
                }}
              >
                {pageData.markdown}
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
