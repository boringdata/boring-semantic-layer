import { Card } from "@/components/ui/card";
import { Copy, Check } from "lucide-react";
import { useState } from "react";

const installOptions = [
  { label: "Basic", command: "pip install boring-semantic-layer" },
  { label: "With Examples", command: "pip install 'boring-semantic-layer[examples]'" },
  { label: "MCP Integration", command: "pip install 'boring-semantic-layer[fastmcp]'" },
  { label: "Altair Viz", command: "pip install 'boring-semantic-layer[viz-altair]'" },
  { label: "Plotly Viz", command: "pip install 'boring-semantic-layer[viz-plotly]'" }
];

export const Installation = () => {
  const [copiedIndex, setCopiedIndex] = useState<number | null>(null);

  const copyToClipboard = (command: string, index: number) => {
    navigator.clipboard.writeText(command);
    setCopiedIndex(index);
    setTimeout(() => setCopiedIndex(null), 2000);
  };

  return (
    <section id="installation" className="px-6 py-24 bg-muted/30">
      <div className="max-w-4xl mx-auto space-y-8">
        <div className="text-center space-y-4">
          <h2 id="installation" className="text-3xl md:text-4xl font-bold">Installation</h2>
          <p className="text-lg text-muted-foreground">
            Choose the installation that fits your needs
          </p>
        </div>

        <div className="grid gap-3">
          {installOptions.map((option, index) => (
            <Card 
              key={index}
              className="p-4 flex items-center justify-between gap-4 hover:border-accent transition-colors group"
            >
              <div className="flex items-center gap-4 flex-1 min-w-0">
                <span className="text-sm font-medium text-muted-foreground shrink-0 w-32">
                  {option.label}
                </span>
                <code className="text-sm font-mono bg-background px-3 py-1.5 rounded border flex-1 overflow-x-auto">
                  {option.command}
                </code>
              </div>
              <button
                onClick={() => copyToClipboard(option.command, index)}
                className="text-muted-foreground hover:text-accent transition-colors shrink-0"
                aria-label="Copy command"
              >
                {copiedIndex === index ? (
                  <Check className="h-4 w-4 text-accent" />
                ) : (
                  <Copy className="h-4 w-4" />
                )}
              </button>
            </Card>
          ))}
        </div>
      </div>
    </section>
  );
};
