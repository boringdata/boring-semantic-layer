import { Card } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Footer } from "@/components/Footer";
import { CodeBlock } from "@/components/CodeBlock";

const Dimensions = () => {
  return (
    <>
      <section className="px-6 py-24">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h1 id="with-dimensions" className="text-4xl font-bold">with_dimensions()</h1>
            <p className="text-xl text-muted-foreground">
              Define dimensions with optional descriptions for better documentation
            </p>
          </div>

          <Tabs defaultValue="basic" className="w-full">
            <TabsList>
              <TabsTrigger value="basic">Basic</TabsTrigger>
              <TabsTrigger value="descriptions">With Descriptions</TabsTrigger>
            </TabsList>

            <TabsContent value="basic" className="space-y-4 mt-6">
              <Card className="p-6 space-y-4">
                <h2 id="basic-dimensions" className="text-2xl font-semibold">Basic Dimensions</h2>
                <CodeBlock code={`from boring_semantic_layer import SemanticModel

flights_sm = SemanticModel(
    table=flights_tbl,
    dimensions={
        'origin': lambda t: t.origin,
        'destination': lambda t: t.dest,
        'year': lambda t: t.year
    }
)`} language="python" />
              </Card>
            </TabsContent>

            <TabsContent value="descriptions" className="space-y-4 mt-6">
              <Card className="p-6 space-y-4">
                <h2 id="dimensions-with-descriptions" className="text-2xl font-semibold">With Descriptions</h2>
                <p className="text-sm text-muted-foreground">
                  Add descriptions to make your models self-documenting and AI-friendly
                </p>
                <CodeBlock code={`from boring_semantic_layer import SemanticModel, DimensionSpec

flights_sm = SemanticModel(
    table=flights_tbl,
    dimensions={
        "origin": DimensionSpec(
            expr=lambda t: t.origin,
            description="Origin airport code where the flight departed"
        ),
        "destination": DimensionSpec(
            expr=lambda t: t.dest,
            description="Destination airport code where the flight arrived"
        ),
        "year": DimensionSpec(
            expr=lambda t: t.year,
            description="Year of the flight"
        )
    }
)`} language="python" />
              </Card>
            </TabsContent>
          </Tabs>
        </div>
      </section>
      <Footer />
    </>
  );
};

export default Dimensions;