import { Card } from "@/components/ui/card";
import { Footer } from "@/components/Footer";
import { CodeBlock } from "@/components/CodeBlock";

const MultiModelQuery = () => {
  return (
    <>
      <section className="px-6 py-24">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h1 id="multi-model" className="text-4xl font-bold">Multi Model Query</h1>
            <p className="text-xl text-muted-foreground">
              Query across multiple joined semantic tables
            </p>
          </div>

          <Card className="p-6 space-y-4">
            <h2 id="overview" className="text-2xl font-semibold">Overview</h2>
            <p className="text-muted-foreground leading-relaxed mb-4">
              When you have semantic tables joined together, you can query dimensions and 
              measures from multiple models in a single query. Use dot notation to reference 
              fields from joined models.
            </p>
            <CodeBlock code={`# Query dimensions and measures from joined models
result = flights_sm.query(
    dimensions=[
        'origin',                  # From flights
        'carriers.name',           # From joined carriers
        'carriers.nickname'        # Also from carriers
    ],
    measures=[
        'flight_count',            # From flights
        'carriers.carrier_count'   # From carriers
    ]
).execute()`} language="python" />
          </Card>
        </div>
      </section>
      <Footer />
    </>
  );
};

export default MultiModelQuery;