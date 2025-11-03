import { Card } from "@/components/ui/card";
import { CodeBlock } from "@/components/CodeBlock";

export const QuickExample = () => {
  const step1Code = `import ibis

flights_tbl = ibis.table(
    name="flights",
    schema={"origin": "string", "carrier": "string"}
)`;

  const step2Code = `from boring_semantic_layer import SemanticModel

flights_sm = SemanticModel(
    table=flights_tbl,
    dimensions={"origin": lambda t: t.origin},
    measures={"flight_count": lambda t: t.count()}
)`;

  const step3Code = `flights_sm.query(
    dimensions=["origin"],
    measures=["flight_count"]
).execute()`;

  return (
    <section id="quickstart" className="px-6 py-24 bg-muted/30">
      <div className="max-w-5xl mx-auto space-y-8">
        <div className="text-center space-y-4">
          <h2 id="quickstart" className="text-3xl md:text-4xl font-bold">Quickstart</h2>
          <p className="text-lg text-muted-foreground">
            Three simple steps to get started - Try the code live in Marimo!
          </p>
        </div>

        <div className="grid gap-6">
          <Card className="p-6 space-y-4 border-l-4 border-l-accent">
            <div className="flex items-start gap-4">
              <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-accent text-accent-foreground font-semibold">
                1
              </div>
              <div className="flex-1 space-y-3">
                <h3 className="text-lg font-semibold">Define your table</h3>
                <CodeBlock code={step1Code} runnable />
              </div>
            </div>
          </Card>

          <Card className="p-6 space-y-4 border-l-4 border-l-accent">
            <div className="flex items-start gap-4">
              <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-accent text-accent-foreground font-semibold">
                2
              </div>
              <div className="flex-1 space-y-3">
                <h3 className="text-lg font-semibold">Build a semantic model</h3>
                <CodeBlock code={step2Code} runnable />
              </div>
            </div>
          </Card>

          <Card className="p-6 space-y-4 border-l-4 border-l-accent">
            <div className="flex items-start gap-4">
              <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-accent text-accent-foreground font-semibold">
                3
              </div>
              <div className="flex-1 space-y-3">
                <h3 className="text-lg font-semibold">Query it</h3>
                <CodeBlock code={step3Code} runnable />
                <div className="pt-2">
                  <p className="text-sm text-muted-foreground mb-2">Expected output:</p>
                  <div className="bg-background border rounded-lg p-4 text-sm font-mono">
                    <table className="w-full">
                      <thead className="border-b">
                        <tr>
                          <th className="text-left py-2 px-4">origin</th>
                          <th className="text-left py-2 px-4">flight_count</th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr className="border-b">
                          <td className="py-2 px-4">JFK</td>
                          <td className="py-2 px-4">3689</td>
                        </tr>
                        <tr>
                          <td className="py-2 px-4">LGA</td>
                          <td className="py-2 px-4">2941</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
          </Card>
        </div>
      </div>
    </section>
  );
};
