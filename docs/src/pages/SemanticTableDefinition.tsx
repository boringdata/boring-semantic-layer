import { Card } from "@/components/ui/card";
import { Footer } from "@/components/Footer";
import { CodeBlock } from "@/components/CodeBlock";

const SemanticTableDefinition = () => {
  return (
    <>
      <section className="px-6 py-24">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h1 id="semantic-table" className="text-4xl font-bold">Semantic Table Definition</h1>
            <p className="text-xl text-muted-foreground leading-relaxed">
              Define your data model with dimensions and measures using Ibis expressions
            </p>
          </div>

          <Card className="p-8">
            <h2 id="overview" className="text-2xl font-semibold mb-4">Overview</h2>
            <p className="text-muted-foreground leading-relaxed">
              A Semantic Table is the core building block of BSL. It transforms a raw Ibis table 
              into a reusable, self-documenting data model by defining dimensions (attributes to group by) 
              and measures (aggregations and calculations).
            </p>
          </Card>

          <Card className="p-6 space-y-4">
            <h2 id="basic-conversion" className="text-2xl font-semibold">From Ibis Table to Semantic Table</h2>
            <h3 className="text-lg font-semibold">Basic Conversion</h3>
            <CodeBlock code={`import ibis
from boring_semantic_layer import SemanticModel

# 1. Start with an Ibis table
flights_tbl = ibis.table(
    name="flights",
    schema={"origin": "string", "carrier": "string", "distance": "int64"}
)

# 2. Convert to a Semantic Table
flights_sm = SemanticModel(
    name="flights",
    table=flights_tbl,
    dimensions={
        'origin': lambda t: t.origin,
        'carrier': lambda t: t.carrier
    },
    measures={
        'flight_count': lambda t: t.count(),
        'total_distance': lambda t: t.distance.sum()
    }
)`} language="python" />
          </Card>
        </div>
      </section>
      <Footer />
    </>
  );
};

export default SemanticTableDefinition;