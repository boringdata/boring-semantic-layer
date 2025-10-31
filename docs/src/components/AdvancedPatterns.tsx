import { Card } from "@/components/ui/card";

export const AdvancedPatterns = () => {
  return (
    <>
      <section id="percentage-total" className="px-6 py-24 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h1 id="advanced-patterns" className="text-4xl font-bold">Advanced Patterns</h1>
            <p className="text-xl text-muted-foreground">
              Powerful techniques for complex analytical queries
            </p>
          </div>

          <div className="space-y-4">
            <h2 id="percentage-total" className="text-2xl font-semibold">Percentage of Total</h2>
            <Card className="p-6 space-y-4">
              <p className="text-sm text-muted-foreground">
                Calculate percentage contributions using window functions
              </p>
              <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
                <code>{`# Define a measure that calculates percentage of total
measures = {
    'flight_count': lambda t: t.count(),
    'pct_of_total': lambda t: (
        t.count() / t.count().sum().over()
    ) * 100
}

result = flights_sm.query(
    dimensions=['origin'],
    measures=['flight_count', 'pct_of_total']
).execute()`}</code>
              </pre>
            </Card>
          </div>
        </div>
      </section>

      <section id="nested-subtotals" className="px-6 py-24 bg-muted/30 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="nested-subtotals" className="text-3xl font-bold">Nested Subtotals</h2>
            <p className="text-lg text-muted-foreground">
              Create hierarchical subtotals for grouped data
            </p>
          </div>

          <Card className="p-6 space-y-4">
            <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
              <code>{`# Query with multiple group levels
result = flights_sm.query(
    dimensions=['origin', 'carrier'],
    measures=['flight_count', 'total_distance']
).execute()

# Add subtotals using grouping sets
from ibis import _

subtotals = flights_sm.table.group_by([
    _.origin,
    _.carrier
]).agg(
    flight_count=_.count(),
    total_distance=_.distance.sum()
).union(
    # Subtotals by origin only
    flights_sm.table.group_by(_.origin).agg(
        carrier=ibis.literal('TOTAL'),
        flight_count=_.count(),
        total_distance=_.distance.sum()
    )
)`}</code>
            </pre>
          </Card>
        </div>
      </section>

      <section id="bucketing" className="px-6 py-24 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="bucketing" className="text-3xl font-bold">Bucketing with Others</h2>
            <p className="text-lg text-muted-foreground">
              Group low-frequency values into an "Others" category
            </p>
          </div>

          <Card className="p-6 space-y-4">
            <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
              <code>{`# Create bucketed dimension
dimensions = {
    'origin': lambda t: t.origin,
    'origin_bucketed': lambda t: t.origin.isin(['JFK', 'LGA', 'EWR']).ifelse(
        t.origin,
        'Others'
    )
}

result = flights_sm.query(
    dimensions=['origin_bucketed'],
    measures=['flight_count']
).execute()`}</code>
            </pre>
          </Card>
        </div>
      </section>

      <section id="sessionized" className="px-6 py-24 bg-muted/30 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="sessionized" className="text-3xl font-bold">Sessionized Data</h2>
            <p className="text-lg text-muted-foreground">
              Analyze user sessions and event sequences
            </p>
          </div>

          <Card className="p-6 space-y-4">
            <p className="text-sm text-muted-foreground mb-4">
              Create session IDs based on time gaps between events
            </p>
            <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
              <code>{`# Define session dimension using window functions
dimensions = {
    'user_id': lambda t: t.user_id,
    'session_id': lambda t: (
        (t.timestamp - t.timestamp.lag().over(
            ibis.window(group_by='user_id', order_by='timestamp')
        ) > ibis.interval(minutes=30))
        .cast('int32')
        .sum()
        .over(ibis.window(group_by='user_id', order_by='timestamp'))
    )
}

measures = {
    'session_count': lambda t: t.session_id.nunique(),
    'events_per_session': lambda t: t.count() / t.session_id.nunique()
}`}</code>
            </pre>
          </Card>
        </div>
      </section>

      <section id="indexing" className="px-6 py-24 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="indexing" className="text-3xl font-bold">Indexing</h2>
            <p className="text-lg text-muted-foreground">
              Create indexed values for time series comparisons
            </p>
          </div>

          <Card className="p-6 space-y-4">
            <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
              <code>{`# Index to base period (e.g., first month = 100)
measures = {
    'flight_count': lambda t: t.count(),
    'indexed_count': lambda t: (
        t.count() / t.count().first().over(
            ibis.window(order_by='date')
        ) * 100
    )
}

result = flights_sm.query(
    dimensions=['date'],
    measures=['flight_count', 'indexed_count'],
    time_grain='TIME_GRAIN_MONTH'
).execute()`}</code>
            </pre>
          </Card>
        </div>
      </section>

      <section id="nesting" className="px-6 py-24 bg-muted/30 scroll-mt-14">
        <div className="max-w-4xl mx-auto space-y-8">
          <div className="space-y-4">
            <h2 id="nesting" className="text-3xl font-bold">Nesting</h2>
            <p className="text-lg text-muted-foreground">
              Build measures on top of other measures for complex calculations
            </p>
          </div>

          <Card className="p-6 space-y-4">
            <pre className="bg-background border rounded-lg p-4 overflow-x-auto text-sm">
              <code>{`# Layer measures on top of each other
measures = {
    # Base measures
    'total_flights': lambda t: t.count(),
    'total_distance': lambda t: t.distance.sum(),
    
    # Nested measure using base measures
    'avg_distance_per_flight': lambda t: (
        t.total_distance / t.total_flights
    ),
    
    # Further nesting
    'distance_efficiency': lambda t: (
        t.avg_distance_per_flight / t.avg_distance_per_flight.max()
    ) * 100
}

result = flights_sm.query(
    dimensions=['carrier'],
    measures=[
        'total_flights',
        'avg_distance_per_flight',
        'distance_efficiency'
    ]
).execute()`}</code>
            </pre>
          </Card>
        </div>
      </section>
    </>
  );
};
