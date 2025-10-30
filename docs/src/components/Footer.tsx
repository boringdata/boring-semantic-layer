import { Github, ExternalLink } from "lucide-react";

export const Footer = () => {
  return (
    <footer className="border-t">
      <div className="max-w-6xl mx-auto px-6 py-12">
        <div className="grid md:grid-cols-3 gap-8">
          <div className="space-y-4">
            <h3 className="font-semibold text-lg">Boring Semantic Layer</h3>
            <p className="text-sm text-muted-foreground leading-relaxed">
              A lightweight semantic layer built on Ibis. Define your data once, query it anywhere.
            </p>
          </div>

          <div className="space-y-4">
            <h4 className="font-semibold">Resources</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <a 
                  href="https://github.com/boringdata/boring-semantic-layer" 
                  target="_blank" 
                  rel="noopener noreferrer"
                  className="text-muted-foreground hover:text-accent transition-colors inline-flex items-center gap-1"
                >
                  GitHub Repository
                  <ExternalLink className="h-3 w-3" />
                </a>
              </li>
              <li>
                <a 
                  href="https://ibis-project.org/" 
                  target="_blank" 
                  rel="noopener noreferrer"
                  className="text-muted-foreground hover:text-accent transition-colors inline-flex items-center gap-1"
                >
                  Ibis Documentation
                  <ExternalLink className="h-3 w-3" />
                </a>
              </li>
              <li>
                <a 
                  href="https://pypi.org/project/boring-semantic-layer/" 
                  target="_blank" 
                  rel="noopener noreferrer"
                  className="text-muted-foreground hover:text-accent transition-colors inline-flex items-center gap-1"
                >
                  PyPI Package
                  <ExternalLink className="h-3 w-3" />
                </a>
              </li>
            </ul>
          </div>

          <div className="space-y-4">
            <h4 className="font-semibold">Community</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <a 
                  href="https://github.com/xorq-labs/xorq" 
                  target="_blank" 
                  rel="noopener noreferrer"
                  className="text-muted-foreground hover:text-accent transition-colors"
                >
                  xorq-labs
                </a>
              </li>
              <li>
                <a 
                  href="https://www.boringdata.io/" 
                  target="_blank" 
                  rel="noopener noreferrer"
                  className="text-muted-foreground hover:text-accent transition-colors"
                >
                  boringdata
                </a>
              </li>
            </ul>
          </div>
        </div>

        <div className="mt-12 pt-8 border-t flex flex-col md:flex-row items-center justify-between gap-4">
          <p className="text-sm text-muted-foreground">
            © 2025 Boring Semantic Layer. MIT License.
          </p>
          <a 
            href="https://github.com/boringdata/boring-semantic-layer" 
            target="_blank" 
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 text-sm text-muted-foreground hover:text-accent transition-colors"
          >
            <Github className="h-4 w-4" />
            Star on GitHub
          </a>
        </div>
      </div>
    </footer>
  );
};
