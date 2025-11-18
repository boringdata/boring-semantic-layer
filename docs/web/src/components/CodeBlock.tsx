import { useState, useEffect, useRef } from "react";
import { Copy, Check, Play } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useTheme } from "next-themes";
import hljs from "highlight.js/lib/core";
import python from "highlight.js/lib/languages/python";
import yaml from "highlight.js/lib/languages/yaml";
import sql from "highlight.js/lib/languages/sql";

hljs.registerLanguage("python", python);
hljs.registerLanguage("yaml", yaml);
hljs.registerLanguage("sql", sql);

// Track if stylesheet is already loaded to prevent multiple instances
let stylesheetLoaded = false;
let currentStylesheet: HTMLLinkElement | null = null;

interface CodeBlockProps {
  code: string;
  language?: string;
  runnable?: boolean;
  className?: string;
}

export const CodeBlock = ({ code, language = "python", runnable = false, className = "" }: CodeBlockProps) => {
  const [copied, setCopied] = useState(false);
  const { theme, resolvedTheme } = useTheme();

  // Map unknown languages to python (for custom block names like dimensions_demo, measures_demo, etc.)
  const highlightLanguage = ['python', 'yaml', 'sql'].includes(language) ? language : 'python';

  // Pre-highlight the code immediately (no useEffect needed)
  const highlightedCode = hljs.highlight(code, { language: highlightLanguage }).value;

  // Load appropriate highlight.js theme based on current theme (singleton pattern)
  useEffect(() => {
    const currentTheme = resolvedTheme || theme;
    const isDark = currentTheme === "dark";
    const newHref = isDark
      ? "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github-dark.min.css"
      : "https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github.min.css";

    // Only update if theme changed or not loaded
    if (!stylesheetLoaded || (currentStylesheet && currentStylesheet.href !== newHref)) {
      // Remove old stylesheet if exists
      if (currentStylesheet) {
        currentStylesheet.remove();
      }

      // Load new theme
      const link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = newHref;
      document.head.appendChild(link);

      currentStylesheet = link;
      stylesheetLoaded = true;
    }

    // Don't remove on cleanup - keep it loaded for all components
  }, [theme, resolvedTheme]);

  const copyToClipboard = () => {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className={`relative group ${className}`}>
      <div className="absolute right-2 top-2 flex gap-2 opacity-0 group-hover:opacity-100 transition-opacity z-10">
        {runnable && (
          <Button
            size="sm"
            variant="secondary"
            onClick={() => window.open(`https://marimo.app/l/sandbox?code=${encodeURIComponent(code)}`, '_blank')}
            className="h-8 px-2"
          >
            <Play className="h-3 w-3 mr-1" />
            <span className="text-xs">Try it</span>
          </Button>
        )}
        <Button
          size="sm"
          variant="secondary"
          onClick={copyToClipboard}
          className="h-8 px-2"
        >
          {copied ? (
            <>
              <Check className="h-3 w-3 mr-1" />
              <span className="text-xs">Copied!</span>
            </>
          ) : (
            <>
              <Copy className="h-3 w-3 mr-1" />
              <span className="text-xs">Copy</span>
            </>
          )}
        </Button>
      </div>
      <pre className="bg-muted/30 border border-border rounded-lg p-4 overflow-x-auto my-4">
        <code
          className={`language-${highlightLanguage} hljs !bg-transparent text-sm leading-relaxed`}
          dangerouslySetInnerHTML={{ __html: highlightedCode }}
        />
      </pre>
    </div>
  );
};
