export const WhatIsBSL = () => {
  return (
    <section id="what-is-bsl" className="px-6 py-24">
      <div className="max-w-4xl mx-auto space-y-8">
        <div className="space-y-4">
          <h1 id="what-is-bsl" className="text-4xl font-bold">What is BSL?</h1>
          <p className="text-xl text-muted-foreground leading-relaxed">
            The Boring Semantic Layer (BSL) is a lightweight semantic layer based on{" "}
            <a href="https://ibis-project.org/" className="text-accent hover:underline" target="_blank" rel="noopener noreferrer">
              Ibis
            </a>
            .
          </p>
        </div>

        <div className="space-y-6">
          <div className="space-y-4">
            <h2 id="why-bsl" className="text-2xl font-semibold">Why BSL?</h2>
            <p className="text-muted-foreground leading-relaxed">
              BSL provides a simple, consistent way to define your data model once and query it anywhere.
              Built on top of Ibis, it inherits all the power and flexibility of Ibis expressions while
              adding a semantic layer that makes your data more accessible and reusable.
            </p>
          </div>

          <div className="space-y-4">
            <h3 id="design-philosophy" className="text-xl font-semibold">Design Philosophy</h3>
            <ul className="space-y-2 text-muted-foreground">
              <li className="flex items-start gap-2">
                <span className="text-accent mt-1">•</span>
                <span><strong className="text-foreground">Lightweight:</strong> No heavy dependencies, just pip install and go</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-accent mt-1">•</span>
                <span><strong className="text-foreground">Ibis-powered:</strong> Leverage the full ecosystem of Ibis backends</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-accent mt-1">•</span>
                <span><strong className="text-foreground">MCP-friendly:</strong> Perfect integration with Large Language Models</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-accent mt-1">•</span>
                <span><strong className="text-foreground">Composable:</strong> Build complex models from simple, reusable pieces</span>
              </li>
            </ul>
          </div>
        </div>

        <div className="bg-muted/30 rounded-lg p-6 space-y-2">
          <p className="text-sm font-medium">Joint Project</p>
          <p className="text-sm text-muted-foreground">
            This project is a collaborative effort between{" "}
            <a href="https://github.com/xorq-labs/xorq" className="text-accent hover:underline" target="_blank" rel="noopener noreferrer">
              xorq-labs
            </a>
            {" "}and{" "}
            <a href="https://www.boringdata.io/" className="text-accent hover:underline" target="_blank" rel="noopener noreferrer">
              boringdata
            </a>
            . We welcome feedback and contributions!
          </p>
        </div>
      </div>
    </section>
  );
};
