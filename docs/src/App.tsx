import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar";
import { AppSidebar } from "@/components/AppSidebar";
import { ThemeProvider } from "next-themes";
import { ThemeToggle } from "@/components/ThemeToggle";
import { TableOfContents } from "@/components/TableOfContents";
import { CommandPalette } from "@/components/CommandPalette";
import { SearchButton } from "@/components/SearchButton";
import { useState, useEffect } from "react";
import Home from "./pages/Home";
import About from "./pages/About";
import KeyFeatures from "./pages/KeyFeatures";
import Installation from "./pages/Installation";
import SemanticTableDefinition from "./pages/SemanticTableDefinition";
import Dimensions from "./pages/Dimensions";
import Measures from "./pages/Measures";
import Joins from "./pages/Joins";
import ComposeModels from "./pages/ComposeModels";
import YAMLConfig from "./pages/YAMLConfig";
import QueryMethods from "./pages/QueryMethods";
import MultiModelQuery from "./pages/MultiModelQuery";
import Filtering from "./pages/Filtering";
import NameConflicts from "./pages/NameConflicts";
import Charting from "./pages/Charting";
import PercentageTotal from "./pages/PercentageTotal";
import NestedSubtotals from "./pages/NestedSubtotals";
import Bucketing from "./pages/Bucketing";
import Sessionized from "./pages/Sessionized";
import Indexing from "./pages/Indexing";
import NotFound from "./pages/NotFound";
import BSLMarkdownPage from "./pages/BSLMarkdownPage";
import JoinsRelationships from "./pages/JoinsRelationships";
import SemanticTable from "./pages/SemanticTable";
import MCP from "./pages/MCP";
import Windowing from "./pages/Windowing";

const queryClient = new QueryClient();

const HomeLayout = ({ children }: { children: React.ReactNode }) => {
  const [searchOpen, setSearchOpen] = useState(false);

  return (
    <div className="min-h-screen flex flex-col">
      <div className="sticky top-0 z-10 flex h-14 items-center justify-between border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 px-4">
        <span className="font-semibold">Boring Semantic Layer</span>
        <div className="flex items-center gap-2">
          <SearchButton onClick={() => setSearchOpen(true)} />
          <ThemeToggle />
        </div>
      </div>
      <main className="flex-1">
        {children}
      </main>
      <CommandPalette open={searchOpen} onOpenChange={setSearchOpen} />
    </div>
  );
};

const Layout = ({ children }: { children: React.ReactNode }) => {
  const [searchOpen, setSearchOpen] = useState(false);

  return (
    <SidebarProvider>
      <div className="min-h-screen flex w-full">
        <AppSidebar />
        <div className="flex-1 flex">
          <main className="flex-1">
            <div className="sticky top-0 z-10 flex h-14 items-center justify-between border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 px-4">
              <div className="flex items-center gap-2">
                <SidebarTrigger />
                <span className="font-semibold">Boring Semantic Layer</span>
              </div>
              <div className="flex items-center gap-2">
                <SearchButton onClick={() => setSearchOpen(true)} />
                <ThemeToggle />
              </div>
            </div>
            {children}
          </main>
        </div>
      </div>
      <CommandPalette open={searchOpen} onOpenChange={setSearchOpen} />
    </SidebarProvider>
  );
};

const App = () => (
  <QueryClientProvider client={queryClient}>
    <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
      <TooltipProvider>
        <Toaster />
        <Sonner />
        <BrowserRouter basename={import.meta.env.BASE_URL}>
          <Routes>
            <Route path="/" element={<HomeLayout><Home /></HomeLayout>} />
            <Route path="/about" element={<Layout><About /></Layout>} />
            <Route path="/examples/getting-started" element={<Layout><BSLMarkdownPage pageSlug="getting-started" /></Layout>} />

            {/* Building Section - New Consolidated Pages */}
            <Route path="/building/semantic-tables" element={<Layout><SemanticTable /></Layout>} />
            <Route path="/building/compose" element={<Layout><ComposeModels /></Layout>} />
            <Route path="/building/yaml" element={<Layout><YAMLConfig /></Layout>} />

            {/* Legacy routes - redirect to new consolidated pages */}
            <Route path="/examples/semantic-table" element={<Navigate to="/building/semantic-tables" replace />} />
            <Route path="/building/semantic-table" element={<Navigate to="/building/semantic-tables" replace />} />
            <Route path="/building/dimensions" element={<Navigate to="/building/semantic-tables" replace />} />
            <Route path="/building/measures" element={<Navigate to="/building/semantic-tables" replace />} />
            <Route path="/examples/compose" element={<Navigate to="/building/compose" replace />} />
            <Route path="/examples/yaml-config" element={<Navigate to="/building/yaml" replace />} />
            {/* Querying Section */}
            <Route path="/querying/methods" element={<Layout><QueryMethods /></Layout>} />
            <Route path="/querying/filtering" element={<Layout><Filtering /></Layout>} />
            <Route path="/querying/charting" element={<Layout><Charting /></Layout>} />
            <Route path="/querying/indexing" element={<Layout><Indexing /></Layout>} />
            <Route path="/querying/mcp" element={<Layout><MCP /></Layout>} />

            {/* Legacy routes - redirect to new consolidated pages */}
            <Route path="/building/joins" element={<Navigate to="/building/semantic-tables" replace />} />
            <Route path="/querying/multi-model" element={<Navigate to="/building/semantic-tables" replace />} />
            <Route path="/querying/conflicts" element={<Navigate to="/building/semantic-tables" replace />} />
            <Route path="/examples/query-methods" element={<Navigate to="/querying/methods" replace />} />
            {/* Advanced Section */}
            <Route path="/advanced/percentage-total" element={<Layout><PercentageTotal /></Layout>} />
            <Route path="/advanced/nested-subtotals" element={<Layout><NestedSubtotals /></Layout>} />
            <Route path="/advanced/bucketing" element={<Layout><Bucketing /></Layout>} />
            <Route path="/advanced/sessionized" element={<Layout><Sessionized /></Layout>} />
            <Route path="/advanced/windowing" element={<Layout><Windowing /></Layout>} />

            {/* Legacy route - redirect indexing to new location */}
            <Route path="/advanced/indexing" element={<Navigate to="/querying/indexing" replace />} />
            <Route path="/reference" element={<Layout><BSLMarkdownPage pageSlug="reference" /></Layout>} />
            <Route path="/examples/:slug" element={<Layout><BSLMarkdownPage /></Layout>} />
            <Route path="*" element={<NotFound />} />
          </Routes>
        </BrowserRouter>
      </TooltipProvider>
    </ThemeProvider>
  </QueryClientProvider>
);

export default App;
