import { Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useEffect } from "react";

interface SearchButtonProps {
  onClick: () => void;
}

export function SearchButton({ onClick }: SearchButtonProps) {
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Cmd+K on Mac, Ctrl+K on Windows/Linux
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        onClick();
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [onClick]);

  return (
    <Button
      variant="outline"
      onClick={onClick}
      className="relative h-9 w-full justify-start text-sm text-muted-foreground sm:pr-12 md:w-40 lg:w-64"
    >
      <Search className="mr-2 h-4 w-4" />
      <span className="hidden lg:inline-flex">Search documentation...</span>
      <span className="inline-flex lg:hidden">Search...</span>
      <kbd className="pointer-events-none absolute right-1.5 top-1.5 hidden h-6 select-none items-center gap-1 rounded border bg-muted px-1.5 font-mono text-[10px] font-medium opacity-100 sm:flex">
        <span className="text-xs">{navigator.platform.includes("Mac") ? "⌘" : "Ctrl"}</span>K
      </kbd>
    </Button>
  );
}
