import { useState } from 'react'
import { ChevronRight } from 'lucide-react'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { CodeBlock } from './CodeBlock'

interface CollapsibleSetupProps {
  code: string
  title?: string
}

export function CollapsibleSetup({ code, title = "Setup code" }: CollapsibleSetupProps) {
  const [isOpen, setIsOpen] = useState(false)

  return (
    <Collapsible open={isOpen} onOpenChange={setIsOpen} className="my-6">
      <CollapsibleTrigger className="flex items-center gap-2 w-full p-4 bg-muted/30 hover:bg-muted/50 rounded-lg border border-border transition-colors">
        <ChevronRight className={`h-4 w-4 transition-transform ${isOpen ? 'rotate-90' : ''}`} />
        <span className="font-medium">📦 {title}</span>
        <span className="ml-auto text-sm text-muted-foreground">(click to expand)</span>
      </CollapsibleTrigger>
      <CollapsibleContent className="mt-2">
        <div className="border border-border rounded-lg overflow-hidden">
          <CodeBlock code={code} language="python" />
        </div>
      </CollapsibleContent>
    </Collapsible>
  )
}
