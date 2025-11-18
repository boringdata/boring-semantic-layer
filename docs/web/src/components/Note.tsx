import React from 'react'
import { Info } from 'lucide-react'
import ReactMarkdown from 'react-markdown'

interface NoteProps {
  children: React.ReactNode
  type?: 'info' | 'warning' | 'tip'
}

export function Note({ children, type = 'info' }: NoteProps) {
  const styles = {
    info: {
      container: 'bg-blue-50 dark:bg-blue-950/30 border-blue-200 dark:border-blue-800',
      icon: 'text-blue-600 dark:text-blue-400',
      text: 'text-blue-900 dark:text-blue-100'
    },
    warning: {
      container: 'bg-amber-50 dark:bg-amber-950/30 border-amber-200 dark:border-amber-800',
      icon: 'text-amber-600 dark:text-amber-400',
      text: 'text-amber-900 dark:text-amber-100'
    },
    tip: {
      container: 'bg-green-50 dark:bg-green-950/30 border-green-200 dark:border-green-800',
      icon: 'text-green-600 dark:text-green-400',
      text: 'text-green-900 dark:text-green-100'
    }
  }

  const style = styles[type]

  // Extract text content if children is a string or can be converted
  const content = typeof children === 'string'
    ? children
    : React.Children.toArray(children).map(child =>
        typeof child === 'string' ? child : ''
      ).join('')

  return (
    <div className={`my-6 p-4 rounded-lg border ${style.container}`}>
      <div className="flex gap-3">
        <Info className={`h-5 w-5 mt-0.5 flex-shrink-0 ${style.icon}`} />
        <div className={`text-sm leading-relaxed ${style.text} prose-sm prose-blue dark:prose-invert max-w-none [&_a]:underline [&_a]:font-medium [&_strong]:font-bold [&_code]:px-1 [&_code]:py-0.5 [&_code]:rounded [&_code]:bg-black/10 dark:[&_code]:bg-white/10`}>
          <ReactMarkdown
            components={{
              p: ({ children }) => <>{children}</>,
              strong: ({ children }) => <strong className="font-bold">{children}</strong>,
              code: ({ children }) => <code>{children}</code>,
              a: ({ href, children }) => <a href={href} className="underline font-medium">{children}</a>
            }}
          >
            {content}
          </ReactMarkdown>
        </div>
      </div>
    </div>
  )
}
