import { Button } from "@/components/ui/button";
import { Copy, Check } from "lucide-react";
import { useState } from "react";
import { Link } from "react-router-dom";

const Home = () => {
  const [copied, setCopied] = useState(false);
  const installCommand = "pip install boring-semantic-layer";

  const copyToClipboard = () => {
    navigator.clipboard.writeText(installCommand);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative min-h-[calc(100vh-3.5rem)] flex items-center justify-center px-6 py-24">
      <div className="absolute inset-0 bg-gradient-to-b from-background via-muted/20 to-background" />
      
      <div className="relative max-w-5xl mx-auto text-center space-y-8 animate-fade-in">
        <div className="inline-block px-4 py-1.5 bg-secondary rounded-full text-sm text-muted-foreground mb-4">
          Lightweight • Ibis-powered • MCP-friendly
        </div>
        
        <h1 className="text-5xl md:text-7xl font-bold tracking-tighter">
          Boring Semantic Layer
        </h1>
        
        <p className="text-xl md:text-2xl text-muted-foreground max-w-2xl mx-auto leading-relaxed">
          A lightweight semantic layer built on Ibis. Define your data once, query it anywhere.
        </p>

        <div className="flex flex-col sm:flex-row items-center justify-center gap-4 pt-4">
          <div className="flex items-center gap-2 bg-card border rounded-lg px-4 py-3 text-sm font-mono group hover:border-accent transition-colors">
            <code className="text-foreground">{installCommand}</code>
            <button
              onClick={copyToClipboard}
              className="text-muted-foreground hover:text-accent transition-colors"
              aria-label="Copy install command"
            >
              {copied ? (
                <Check className="h-4 w-4 text-accent" />
              ) : (
                <Copy className="h-4 w-4" />
              )}
            </button>
          </div>

          <Button size="lg" asChild className="bg-accent hover:bg-accent/90 text-accent-foreground">
            <Link to="/about">
              Documentation
            </Link>
          </Button>

          <Button size="lg" variant="outline" asChild>
            <a href="https://github.com/boringdata/boring-semantic-layer" target="_blank" rel="noopener noreferrer">
              View on GitHub
            </a>
          </Button>
        </div>

        <div className="pt-12 space-y-6">
          <div>
            <p className="text-sm text-muted-foreground mb-4">A joint effort by</p>
            <div className="flex items-center justify-center gap-6 text-sm">
              <a
                href="https://github.com/xorq-labs/xorq"
                target="_blank"
                rel="noopener noreferrer"
                className="text-muted-foreground hover:text-accent transition-colors"
              >
                xorq-labs
              </a>
              <span className="text-muted-foreground">×</span>
              <a
                href="https://www.boringdata.io/"
                target="_blank"
                rel="noopener noreferrer"
                className="text-muted-foreground hover:text-accent transition-colors"
              >
                boringdata
              </a>
            </div>
          </div>

          <p className="text-xs text-muted-foreground/70">
            Freely inspired by the awesome{" "}
            <a
              href="https://github.com/malloydata/malloy"
              target="_blank"
              rel="noopener noreferrer"
              className="hover:text-accent transition-colors underline decoration-dotted"
            >
              Malloy
            </a>
            {" "}project. We loved the vision, just took the Python route
          </p>
        </div>
      </div>
    </div>
  );
};

export default Home;
