import { Card } from "@/components/ui/card";
import { Database, Filter, Link2, Brain, BarChart3, Clock } from "lucide-react";

const features = [
  {
    icon: Database,
    title: "Semantic Models",
    description: "Define dimensions and measures using Ibis expressions. Your data model becomes self-documenting and reusable across all queries."
  },
  {
    icon: Filter,
    title: "Flexible Filters",
    description: "Use Ibis expressions or JSON-based filters. Perfect for dynamic queries and LLM integration with operators like AND, OR, in, not in."
  },
  {
    icon: Link2,
    title: "Cross-Model Joins",
    description: "Join semantic models together with classic SQL joins, join_one, join_many, or join_cross. Enrich your data effortlessly."
  },
  {
    icon: Brain,
    title: "MCP Integration",
    description: "Native Model Context Protocol support. Connect LLMs directly to your structured data sources with zero friction."
  },
  {
    icon: BarChart3,
    title: "Smart Charting",
    description: "Built-in visualization with Altair and Plotly. Auto-detection of chart types based on your data structure."
  },
  {
    icon: Clock,
    title: "Time-Based Queries",
    description: "Define time dimensions and query with specific grains. Perfect for time-series analysis and temporal aggregations."
  }
];

export const Features = () => {
  return (
    <section id="features" className="px-6 py-24">
      <div className="max-w-6xl mx-auto space-y-12">
        <div className="text-center space-y-4">
          <h2 id="features" className="text-3xl md:text-4xl font-bold">Features</h2>
          <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
            Everything you need to build a powerful semantic layer
          </p>
        </div>

        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
          {features.map((feature, index) => (
            <Card 
              key={index} 
              className="p-6 space-y-4 hover:border-accent transition-all duration-300 hover:shadow-md"
            >
              <div className="flex items-center gap-3">
                <div className="p-2 bg-accent/10 rounded-lg">
                  <feature.icon className="h-5 w-5 text-accent" />
                </div>
                <h3 className="text-lg font-semibold">{feature.title}</h3>
              </div>
              <p className="text-muted-foreground leading-relaxed">
                {feature.description}
              </p>
            </Card>
          ))}
        </div>
      </div>
    </section>
  );
};
