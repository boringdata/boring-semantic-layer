// Search index for documentation pages
export interface SearchResult {
  title: string;
  path: string;
  section: string;
  type: 'page' | 'heading';
  keywords?: string[];
}

// Navigation structure for building search index
const navigationStructure = [
  {
    section: "Home",
    items: [
      { title: "Home", path: "/" },
    ],
  },
  {
    section: "About BSL",
    items: [
      { title: "What is BSL", path: "/about" },
      { title: "Getting Started", path: "/examples/getting-started" },
    ],
  },
  {
    section: "Building a Semantic Table",
    items: [
      { title: "Defining Semantic Tables", path: "/building/semantic-tables" },
      { title: "Joins & Relationships", path: "/building/joins" },
      { title: "Compose Models", path: "/building/compose" },
      { title: "YAML Config", path: "/building/yaml" },
    ],
  },
  {
    section: "Querying Semantic Tables",
    items: [
      { title: "Query Methods", path: "/querying/methods" },
      { title: "Filtering", path: "/querying/filtering" },
      { title: "Charting", path: "/querying/charting" },
    ],
  },
  {
    section: "Advanced Patterns",
    items: [
      { title: "Percentage of Total", path: "/advanced/percentage-total" },
      { title: "Nested Subtotals", path: "/advanced/nested-subtotals" },
      { title: "Bucketing", path: "/advanced/bucketing" },
      { title: "Sessionized Data", path: "/advanced/sessionized" },
      { title: "Indexing", path: "/advanced/indexing" },
      { title: "Nesting", path: "/advanced/nesting" },
    ],
  },
  {
    section: "Reference",
    items: [
      { title: "Reference", path: "/reference" },
    ],
  },
];

// Build search index from navigation structure
export function buildSearchIndex(): SearchResult[] {
  const searchIndex: SearchResult[] = [];

  navigationStructure.forEach(({ section, items }) => {
    items.forEach(({ title, path }) => {
      // Generate keywords from title and section
      const keywords = [
        ...title.toLowerCase().split(' '),
        ...section.toLowerCase().split(' '),
      ];

      searchIndex.push({
        title,
        path,
        section,
        type: 'page',
        keywords,
      });
    });
  });

  return searchIndex;
}

// Search function with fuzzy matching
export function searchPages(query: string, index: SearchResult[]): SearchResult[] {
  if (!query.trim()) {
    return [];
  }

  const normalizedQuery = query.toLowerCase().trim();
  const queryWords = normalizedQuery.split(/\s+/);

  return index
    .map(item => {
      let score = 0;

      // Exact title match gets highest score
      if (item.title.toLowerCase() === normalizedQuery) {
        score += 100;
      }

      // Title starts with query
      if (item.title.toLowerCase().startsWith(normalizedQuery)) {
        score += 50;
      }

      // Title contains query
      if (item.title.toLowerCase().includes(normalizedQuery)) {
        score += 30;
      }

      // Section matches
      if (item.section.toLowerCase().includes(normalizedQuery)) {
        score += 20;
      }

      // Keyword matches
      queryWords.forEach(word => {
        item.keywords?.forEach(keyword => {
          if (keyword === word) {
            score += 15;
          } else if (keyword.includes(word) || word.includes(keyword)) {
            score += 5;
          }
        });
      });

      return { item, score };
    })
    .filter(({ score }) => score > 0)
    .sort((a, b) => b.score - a.score)
    .map(({ item }) => item)
    .slice(0, 10); // Return top 10 results
}

// Popular pages to show when no query is entered
export const popularPages: SearchResult[] = [
  { title: "Getting Started", path: "/examples/getting-started", section: "About BSL", type: "page" },
  { title: "Defining Semantic Tables", path: "/building/semantic-tables", section: "Building a Semantic Table", type: "page" },
  { title: "Query Methods", path: "/querying/methods", section: "Querying Semantic Tables", type: "page" },
  { title: "Reference", path: "/reference", section: "Reference", type: "page" },
];
