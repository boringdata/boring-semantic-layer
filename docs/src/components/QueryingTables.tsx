import { Card } from "@/components/ui/card";

export const QueryingTables = () => {
  return (
    <>
      <section id="querying-overview" className="px-6 py-24 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h1 id="querying-overview" className="text-4xl font-bold">Querying Semantic Tables</h1>
            <p className="text-xl text-muted-foreground leading-relaxed">
              Use powerful query methods to analyze your data
            </p>
          </div>

          <Card className="p-8 space-y-4">
            <p className="text-muted-foreground leading-relaxed">
              Once you've defined your semantic tables, you can query them using intuitive methods 
              like group_by, aggregate, mutate, and more. All queries are composable and generate 
              optimized SQL.
            </p>
          </Card>
        </div>
      </section>

      <section id="query-methods" className="px-6 py-24 bg-muted/30 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="query-methods" className="text-3xl font-bold">group_by / aggregate / mutate / order_by</h2>
            <p className="text-lg text-muted-foreground">
              Core query methods for data transformation and analysis
            </p>
          </div>

          <div className="space-y-6">
            <Card className="p-6 space-y-4">
              <h3 className="text-lg font-semibold">group_by</h3>
              <p className="text-sm text-muted-foreground">Group data by dimensions</p>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`# Group by single dimension
result = flights_sm.query(
    dimensions=['origin'],
    measures=['flight_count']
).execute()

# Group by multiple dimensions
result = flights_sm.query(
    dimensions=['origin', 'carrier'],
    measures=['flight_count', 'avg_distance']
).execute()`}</code>
              </pre>
            </Card>

            <Card className="p-6 space-y-4">
              <h3 className="text-lg font-semibold">aggregate</h3>
              <p className="text-sm text-muted-foreground">Apply aggregation functions</p>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`# Aggregate measures
result = flights_sm.query(
    measures=['total_flights', 'total_distance', 'avg_distance']
).execute()`}</code>
              </pre>
            </Card>

            <Card className="p-6 space-y-4">
              <h3 className="text-lg font-semibold">order_by</h3>
              <p className="text-sm text-muted-foreground">Sort results by dimensions or measures</p>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`# Order by dimension
result = flights_sm.query(
    dimensions=['origin'],
    measures=['flight_count'],
    order_by=['origin']
).execute()

# Order by measure (descending)
result = flights_sm.query(
    dimensions=['origin'],
    measures=['flight_count'],
    order_by=[('flight_count', 'desc')]
).execute()`}</code>
              </pre>
            </Card>
          </div>
        </div>
      </section>

      <section id="multi-model" className="px-6 py-24 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="multi-model" className="text-3xl font-bold">Multi Model Query</h2>
            <p className="text-lg text-muted-foreground">
              Query across multiple joined semantic tables
            </p>
          </div>

          <Card className="p-6 space-y-4">
            <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
              <code>{`# Query dimensions and measures from joined models
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
).execute()`}</code>
            </pre>
          </Card>
        </div>
      </section>

      <section id="limit-filter" className="px-6 py-24 bg-muted/30 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="limit-filter" className="text-3xl font-bold">limit / filter</h2>
            <p className="text-lg text-muted-foreground">
              Control result size and filter data
            </p>
          </div>

          <div className="space-y-6">
            <Card className="p-6 space-y-4">
              <h3 className="text-lg font-semibold">Limit Results</h3>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`# Get top 10 results
result = flights_sm.query(
    dimensions=['origin'],
    measures=['flight_count'],
    limit=10
).execute()`}</code>
              </pre>
            </Card>

            <Card className="p-6 space-y-4">
              <h3 className="text-lg font-semibold">Filter with Ibis Expressions</h3>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`# Filter using lambda
result = flights_sm.query(
    dimensions=['origin'],
    measures=['flight_count'],
    filters=[lambda t: t.origin == 'JFK']
).execute()`}</code>
              </pre>
            </Card>

            <Card className="p-6 space-y-4">
              <h3 className="text-lg font-semibold">Filter with JSON (LLM-friendly)</h3>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`# Filter using JSON syntax
result = flights_sm.query(
    dimensions=['origin'],
    measures=['flight_count'],
    filters=[
        {
            'operator': 'AND',
            'conditions': [
                {'field': 'origin', 'operator': 'in', 'values': ['JFK', 'LGA']},
                {'field': 'year', 'operator': '=', 'values': [2013]}
            ]
        }
    ]
).execute()`}</code>
              </pre>
            </Card>
          </div>
        </div>
      </section>

      <section id="name-conflicts" className="px-6 py-24 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="name-conflicts" className="text-3xl font-bold">Name Conflicts</h2>
            <p className="text-lg text-muted-foreground">
              Handle naming conflicts when joining models
            </p>
          </div>

          <Card className="p-6 space-y-4">
            <p className="text-sm text-muted-foreground mb-4">
              When multiple models have dimensions or measures with the same name, 
              use the model prefix to disambiguate.
            </p>
            <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
              <code>{`# Both models have a 'name' dimension
result = flights_sm.query(
    dimensions=[
        'flights.name',    # Explicitly from flights
        'carriers.name'    # Explicitly from carriers
    ],
    measures=['flight_count']
).execute()`}</code>
            </pre>
          </Card>
        </div>
      </section>

      <section id="charting" className="px-6 py-24 bg-muted/30 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="charting" className="text-3xl font-bold">Charting</h2>
            <p className="text-lg text-muted-foreground">
              Visualize your query results with built-in charting
            </p>
          </div>

          <div className="space-y-6">
            <Card className="p-6 space-y-4">
              <h3 className="text-lg font-semibold">Auto-detected Charts</h3>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`# Install with viz support
pip install 'boring-semantic-layer[viz-altair]'

# Create a chart from your query
query = flights_sm.query(
    dimensions=['origin'],
    measures=['flight_count']
)

# Auto-detect chart type
chart = query.chart()
chart.save('flights.html')`}</code>
              </pre>
            </Card>

            <Card className="p-6 space-y-4">
              <h3 className="text-lg font-semibold">Custom Chart Types</h3>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`# Specify mark type
chart = query.chart(mark='bar')
chart = query.chart(mark='line')
chart = query.chart(mark='point')

# Time series
time_query = flights_sm.query(
    dimensions=['date'],
    measures=['flight_count'],
    time_range={'start': '2013-01-01', 'end': '2013-12-31'},
    time_grain='TIME_GRAIN_MONTH'
)
chart = time_query.chart()  # Auto-detects time series`}</code>
              </pre>
            </Card>
          </div>
        </div>
      </section>
    </>
  );
};
