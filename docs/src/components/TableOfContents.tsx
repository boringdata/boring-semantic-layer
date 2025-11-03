import { useEffect, useState } from "react";
import { cn } from "@/lib/utils";

interface Heading {
  id: string;
  text: string;
  level: number;
}

export const TableOfContents = () => {
  const [headings, setHeadings] = useState<Heading[]>([]);
  const [activeId, setActiveId] = useState<string>("");

  useEffect(() => {
    // Extract all headings from the page
    const elements = Array.from(
      document.querySelectorAll("main h1, main h2, main h3")
    );
    
    const headingData = elements.map((element) => ({
      id: element.id || "",
      text: element.textContent || "",
      level: parseInt(element.tagName.substring(1)),
    })).filter(h => h.id); // Only include headings with IDs

    setHeadings(headingData);

    // Set up intersection observer for scroll tracking
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            setActiveId(entry.target.id);
          }
        });
      },
      { rootMargin: "-80px 0px -80% 0px" }
    );

    elements.forEach((element) => {
      if (element.id) observer.observe(element);
    });

    return () => observer.disconnect();
  }, []);

  if (headings.length === 0) return null;

  const handleClick = (id: string) => {
    const element = document.getElementById(id);
    if (element) {
      const offset = 80; // Account for sticky header
      const elementPosition = element.getBoundingClientRect().top;
      const offsetPosition = elementPosition + window.pageYOffset - offset;

      window.scrollTo({
        top: offsetPosition,
        behavior: "smooth",
      });
    }
  };

  return (
    <div>
      <div className="max-h-[calc(100vh-8rem)] overflow-auto py-2 pr-4 pl-2">
        <h4 className="text-sm font-semibold mb-3 text-foreground">On This Page</h4>
        <nav className="space-y-1.5">
          {headings.map((heading) => (
            <button
              key={heading.id}
              onClick={() => handleClick(heading.id)}
              className={cn(
                "block w-full text-left text-sm transition-colors hover:text-foreground border-l-2",
                heading.level === 2 && "pl-3",
                heading.level === 3 && "pl-6",
                activeId === heading.id
                  ? "text-foreground font-medium border-foreground"
                  : "text-muted-foreground border-transparent hover:border-muted-foreground"
              )}
            >
              {heading.text}
            </button>
          ))}
        </nav>
      </div>
    </div>
  );
};