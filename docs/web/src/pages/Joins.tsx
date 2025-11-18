import { Card } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Footer } from "@/components/Footer";

const Joins = () => {
  return (
    <>
      <section className="px-6 py-24">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h1 id="joins" className="text-4xl font-bold">Joins</h1>
            <p className="text-xl text-muted-foreground">
              Connect semantic tables with join, join_one, and join_cross
            </p>
          </div>

          <Tabs defaultValue="classic" className="w-full">
            <TabsList>
              <TabsTrigger value="classic">Classic Join</TabsTrigger>
              <TabsTrigger value="join_one">join_one</TabsTrigger>
              <TabsTrigger value="join_cross">join_cross</TabsTrigger>
            </TabsList>

            <TabsContent value="classic" className="space-y-4 mt-6">
              <Card className="p-6 space-y-4">
                <h2 id="classic-joins" className="text-2xl font-semibold">SQL-style Joins</h2>
                <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                  <code>{`from boring_semantic_layer import Join

# Define carriers model
carriers_sm = SemanticModel(
    name="carriers",
    table=carriers_tbl,
    dimensions={
        "code": lambda t: t.code,
        "name": lambda t: t.name
    }
)

# Join with flights
flights_sm = SemanticModel(
    name="flights",
    table=flights_tbl,
    dimensions={"carrier": lambda t: t.carrier},
    measures={"flight_count": lambda t: t.count()},
    joins={
        "carriers": Join(
            model=carriers_sm,
            on=lambda left, right: left.carrier == right.code,
            how="left"  # or "inner", "right", "outer"
        )
    }
)

# Query across joined models
flights_sm.query(
    dimensions=['carriers.name'],
    measures=['flight_count']
).execute()`}</code>
                </pre>
              </Card>
            </TabsContent>

            <TabsContent value="join_one" className="space-y-4 mt-6">
              <Card className="p-6 space-y-4">
                <h2 id="join-one" className="text-2xl font-semibold">One-to-One Joins</h2>
                <p className="text-sm text-muted-foreground mb-4">
                  Use join_one for relationships where each row matches exactly one row in the joined table
                </p>
                <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                  <code>{`from boring_semantic_layer import join_one

flights_sm = SemanticModel(
    table=flights_tbl,
    joins={
        "carrier_details": join_one(
            model=carriers_sm,
            on=lambda left, right: left.carrier == right.code
        )
    }
)`}</code>
                </pre>
              </Card>
            </TabsContent>

            <TabsContent value="join_cross" className="space-y-4 mt-6">
              <Card className="p-6 space-y-4">
                <h2 id="join-cross" className="text-2xl font-semibold">Cross Joins</h2>
                <p className="text-sm text-muted-foreground mb-4">
                  Create cartesian products for special analytical needs
                </p>
                <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                  <code>{`from boring_semantic_layer import join_cross

# Cross join for generating all combinations
flights_sm = SemanticModel(
    table=flights_tbl,
    joins={
        "all_carriers": join_cross(model=carriers_sm)
    }
)`}</code>
                </pre>
              </Card>
            </TabsContent>
          </Tabs>
        </div>
      </section>
      <Footer />
    </>
  );
};

export default Joins;