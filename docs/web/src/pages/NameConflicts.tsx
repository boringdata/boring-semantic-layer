import { Card } from "@/components/ui/card";
import { Footer } from "@/components/Footer";
import { CodeBlock } from "@/components/CodeBlock";

const NameConflicts = () => {
  return (
    <>
      <section className="px-6 py-24">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h1 id="name-conflicts" className="text-4xl font-bold">Name Conflicts</h1>
            <p className="text-xl text-muted-foreground">
              Handle naming conflicts when joining models
            </p>
          </div>

          <Card className="p-6 space-y-4">
            <h2 id="overview" className="text-2xl font-semibold">Overview</h2>
            <p className="text-sm text-muted-foreground mb-4">
              When multiple models have dimensions or measures with the same name, 
              use the model prefix to disambiguate.
            </p>
            <CodeBlock code={`# Both models have a 'name' dimension
result = flights_sm.query(
    dimensions=[
        'flights.name',    # Explicitly from flights
        'carriers.name'    # Explicitly from carriers
    ],
    measures=['flight_count']
).execute()`} language="python" />
          </Card>
        </div>
      </section>
      <Footer />
    </>
  );
};

export default NameConflicts;