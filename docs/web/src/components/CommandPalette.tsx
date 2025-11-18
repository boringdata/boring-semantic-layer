import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { buildSearchIndex, searchPages, SearchResult } from "@/lib/search-index";
import { FileText, ChevronRight } from "lucide-react";

interface CommandPaletteProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function CommandPalette({ open, onOpenChange }: CommandPaletteProps) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const navigate = useNavigate();

  // Build search index once on mount
  const [searchIndex] = useState(() => buildSearchIndex());

  // Popular pages to show when no query
  const popularPages = [
    { title: "Getting Started", path: "/examples/getting-started", section: "About BSL" },
    { title: "Semantic Table", path: "/examples/semantic-table", section: "Building a Semantic Table" },
    { title: "Query Methods", path: "/examples/query-methods", section: "Querying Semantic Tables" },
    { title: "Reference", path: "/reference", section: "Reference" },
  ];

  // Update results when query changes
  useEffect(() => {
    if (query.trim()) {
      const searchResults = searchPages(query, searchIndex);
      setResults(searchResults);
    } else {
      // Show popular pages when no query
      setResults(popularPages);
    }
  }, [query, searchIndex]);

  const handleSelect = (path: string) => {
    onOpenChange(false);
    navigate(path);
    setQuery(""); // Reset query after navigation
  };

  // Group results by section
  const groupedResults = results.reduce((acc, result) => {
    if (!acc[result.section]) {
      acc[result.section] = [];
    }
    acc[result.section].push(result);
    return acc;
  }, {} as Record<string, SearchResult[]>);

  return (
    <CommandDialog open={open} onOpenChange={onOpenChange}>
      <CommandInput
        placeholder="Search documentation..."
        value={query}
        onValueChange={setQuery}
      />
      <CommandList>
        <CommandEmpty>
          {query.trim() ? "No results found." : "Start typing to search..."}
        </CommandEmpty>
        {Object.entries(groupedResults).map(([section, items]) => (
          <CommandGroup
            key={section}
            heading={query.trim() ? section : `${section} (Popular)`}
          >
            {items.map((item) => (
              <CommandItem
                key={item.path}
                value={`${item.title}-${item.path}`}
                onSelect={() => handleSelect(item.path)}
                className="flex items-center gap-2 cursor-pointer"
              >
                <FileText className="h-4 w-4 text-muted-foreground" />
                <span className="flex-1">{item.title}</span>
                <ChevronRight className="h-4 w-4 text-muted-foreground" />
              </CommandItem>
            ))}
          </CommandGroup>
        ))}
      </CommandList>
    </CommandDialog>
  );
}
