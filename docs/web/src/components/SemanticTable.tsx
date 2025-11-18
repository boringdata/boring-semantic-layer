import { Card } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

export const SemanticTable = () => {
  return (
    <>
      <section id="semantic-table" className="px-6 py-24 bg-muted/30 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h1 id="semantic-table" className="text-4xl font-bold">Semantic Table Definition</h1>
            <p className="text-xl text-muted-foreground leading-relaxed">
              Define your data model with dimensions and measures using Ibis expressions
            </p>
          </div>

          <Card className="p-8">
            <p className="text-muted-foreground leading-relaxed">
              A Semantic Table is the core building block of BSL. It transforms a raw Ibis table 
              into a reusable, self-documenting data model by defining dimensions (attributes to group by) 
              and measures (aggregations and calculations).
            </p>
          </Card>
        </div>
      </section>

      <section id="ibis-to-semantic" className="px-6 py-24 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="ibis-to-semantic" className="text-3xl font-bold">From Ibis Table to Semantic Table</h2>
            <p className="text-lg text-muted-foreground">
              Transform your Ibis tables into semantic models
            </p>
          </div>

          <Card className="p-6 space-y-4">
            <h3 className="text-lg font-semibold">Basic Conversion</h3>
            <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
              <code>{`import ibis
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
)`}</code>
            </pre>
          </Card>
        </div>
      </section>

      <section id="with-dimensions" className="px-6 py-24 bg-muted/30 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="with-dimensions" className="text-3xl font-bold">with_dimensions()</h2>
            <p className="text-lg text-muted-foreground">
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
                <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                  <code>{`from boring_semantic_layer import SemanticModel

flights_sm = SemanticModel(
    table=flights_tbl,
    dimensions={
        'origin': lambda t: t.origin,
        'destination': lambda t: t.dest,
        'year': lambda t: t.year
    }
)`}</code>
                </pre>
              </Card>
            </TabsContent>

            <TabsContent value="descriptions" className="space-y-4 mt-6">
              <Card className="p-6 space-y-4">
                <p className="text-sm text-muted-foreground">
                  Add descriptions to make your models self-documenting and AI-friendly
                </p>
                <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                  <code>{`from boring_semantic_layer import SemanticModel, DimensionSpec

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
)`}</code>
                </pre>
              </Card>
            </TabsContent>
          </Tabs>
        </div>
      </section>

      <section id="with-measures" className="px-6 py-24 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="with-measures" className="text-3xl font-bold">with_measures()</h2>
            <p className="text-lg text-muted-foreground">
              Define measures with lambda expressions, shortcuts, and descriptions
            </p>
          </div>

          <div className="space-y-6">
            <Card className="p-6 space-y-4">
              <h3 className="text-lg font-semibold">Lambda Expressions</h3>
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
              <h3 className="text-lg font-semibold">With Descriptions</h3>
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
              <h3 className="text-lg font-semibold">Reference Other Measures</h3>
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

      <section id="joins" className="px-6 py-24 bg-muted/30 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="joins" className="text-3xl font-bold">Joins</h2>
            <p className="text-lg text-muted-foreground">
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
                <h3 className="text-lg font-semibold">SQL-style Joins</h3>
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
                <h3 className="text-lg font-semibold">One-to-One Joins</h3>
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
                <h3 className="text-lg font-semibold">Cross Joins</h3>
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

      <section id="compose-models" className="px-6 py-24 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="compose-models" className="text-3xl font-bold">Compose Models Together</h2>
            <p className="text-lg text-muted-foreground">
              Build complex data models by combining multiple semantic tables
            </p>
          </div>

          <Card className="p-6 space-y-4">
            <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
              <code>{`# Build layered models
base_flights = SemanticModel(
    table=flights_tbl,
    dimensions={'origin': lambda t: t.origin},
    measures={'count': lambda t: t.count()}
)

enriched_flights = SemanticModel(
    table=base_flights.table,
    dimensions={
        **base_flights.dimensions,
        'destination': lambda t: t.dest
    },
    measures={
        **base_flights.measures,
        'avg_distance': lambda t: t.distance.mean()
    }
)`}</code>
            </pre>
          </Card>
        </div>
      </section>

      <section id="yaml" className="px-6 py-24 bg-muted/30 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="yaml" className="text-3xl font-bold">YAML Configuration</h2>
            <p className="text-lg text-muted-foreground">
              Define your semantic models using YAML for better organization
            </p>
          </div>

          <div className="space-y-6">
            <Card className="p-6 space-y-4">
              <h3 className="text-lg font-semibold">YAML Format</h3>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`# flights_model.yml
flights:
  table: flights_table
  description: "Flight data with departure and arrival information"
  
  dimensions:
    origin:
      expr: _.origin
      description: "Origin airport code"
    
    destination:
      expr: _.destination
      description: "Destination airport code"
  
  measures:
    flight_count:
      expr: _.count()
      description: "Total number of flights"
    
    avg_distance:
      expr: _.distance.mean()
      description: "Average flight distance in miles"`}</code>
              </pre>
            </Card>

            <Card className="p-6 space-y-4">
              <h3 className="text-lg font-semibold">Load YAML Models</h3>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`from boring_semantic_layer import SemanticModel

# Load models from YAML
models = SemanticModel.from_yaml(
    "flights_model.yml",
    tables={"flights_table": flights_tbl}
)

flights_sm = models["flights"]`}</code>
              </pre>
            </Card>
          </div>
        </div>
      </section>
    </>
  );
};
