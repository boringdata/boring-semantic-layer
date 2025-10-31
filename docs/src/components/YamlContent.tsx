import { CodeBlock } from './CodeBlock'

interface YamlContentProps {
  path: string
  content?: string
}

export function YamlContent({ path, content }: YamlContentProps) {
  if (!content) {
    return (
      <div className="my-6 p-6 bg-destructive/10 border border-destructive rounded-lg text-destructive">
        Error: YAML file content not found for {path}
      </div>
    )
  }

  return <CodeBlock code={content} language="yaml" />
}
