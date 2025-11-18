import { Card } from "@/components/ui/card";
import { Footer } from "@/components/Footer";

const Measures = () => {
  return (
    <>
      <section className="px-6 py-24">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h1 id="with-measures" className="text-4xl font-bold">with_measures()</h1>
            <p className="text-xl text-muted-foreground">
              Define measures with lambda expressions, shortcuts, and descriptions
            </p>
          </div>

          <div className="space-y-6">
            <Card className="p-6 space-y-4">
              <h2 id="lambda-expressions" className="text-2xl font-semibold">Lambda Expressions</h2>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`measures={
    'total_flights': lambda t: t.count(),
    'total_distance': lambda t: t.distance.sum(),
    'avg_distance': lambda t: t.distance.mean(),
    'max_delay': lambda t: t.dep_delay.max()
}`}</code>
              </pre>
            </Card>

            <Card className="p-6 space-y-4">
              <h2 id="with-descriptions" className="text-2xl font-semibold">With Descriptions</h2>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`from boring_semantic_layer import MeasureSpec

measures={
    "flight_count": MeasureSpec(
        expr=lambda t: t.count(),
        description="Total number of flights"
    ),
    "avg_distance": MeasureSpec(
        expr=lambda t: t.distance.mean(),
        description="Average flight distance in miles"
    )
}`}</code>
              </pre>
            </Card>

            <Card className="p-6 space-y-4">
              <h2 id="reference-measures" className="text-2xl font-semibold">Reference Other Measures</h2>
              <p className="text-sm text-muted-foreground">
                Build complex measures by referencing other measures
              </p>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`measures={
    'total_distance': lambda t: t.distance.sum(),
    'flight_count': lambda t: t.count(),
    # Reference other measures
    'avg_distance_per_flight': lambda t: t.total_distance / t.flight_count
}`}</code>
              </pre>
            </Card>
          </div>
        </div>
      </section>
      <Footer />
    </>
  );
};

export default Measures;