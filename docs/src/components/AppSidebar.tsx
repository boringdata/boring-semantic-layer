import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "@/components/ui/sidebar";
import { Home, BookOpen, Code2, Sparkles, FileText, Github, ChevronRight, Sparkle } from "lucide-react";
import { useState } from "react";
import { NavLink, useLocation } from "react-router-dom";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";

const navigationStructure = [
  {
    title: "Home",
    icon: Home,
    path: "/",
  },
  {
    title: "About BSL",
    icon: Sparkle,
    items: [
      { title: "What is BSL", path: "/about" },
      { title: "Getting Started", path: "/examples/getting-started" },
    ],
  },
  {
    title: "Building a Semantic Table",
    icon: Code2,
    items: [
      {
        title: "Semantic Tables",
        path: "/building/semantic-tables",
        subitems: [
          { title: "to_semantic_table()", hash: "#to-semantic-table" },
          { title: "with_dimensions()", hash: "#with-dimensions" },
          { title: "with_measures()", hash: "#with-measures" },
          { title: "join_one() (+many/cross)", hash: "#join-one-join-many-join-cross" },
        ],
      },
      { title: "Backend Profiles", path: "/building/profile" },
      { title: "Compose Models", path: "/building/compose" },
      { title: "Profiles", path: "/building/profiles" },
      { title: "YAML Config", path: "/building/yaml" },
    ],
  },
  {
    title: "Querying Semantic Tables",
    icon: BookOpen,
    items: [
      {
        title: "Query Methods",
        path: "/querying/methods",
        subitems: [
          { title: "group_by()", hash: "#group-by" },
          { title: "aggregate()", hash: "#aggregate" },
          { title: "filter() / order_by() / limit()", hash: "#filter-order-by-limit" },
          { title: "nest()", hash: "#nest" },
          { title: "mutate()", hash: "#mutate" },
          { title: "Window Functions (.over)", hash: "#window-functions-with-over" },
          { title: "as_table()", hash: "#as-table" },
        ],
      },
      { title: "Charting", path: "/querying/charting" },
      { title: "Dimensional Indexing", path: "/querying/indexing" },
      { title: "MCP Integration", path: "/querying/mcp" },
    ],
  },
  {
    title: "Advanced Patterns",
    icon: Sparkles,
    items: [
      { title: "Percentage of Total", path: "/advanced/percentage-total" },
      { title: "Nested Subtotals", path: "/advanced/nested-subtotals" },
      { title: "Window Functions", path: "/advanced/windowing" },
      { title: "Bucketing", path: "/advanced/bucketing" },
      { title: "Sessionized Data", path: "/advanced/sessionized" },
    ],
  },
  {
    title: "Reference",
    icon: FileText,
    path: "/reference",
  },
];

export function AppSidebar() {
  const { open } = useSidebar();
  const location = useLocation();
  const [openSections, setOpenSections] = useState<string[]>(["About BSL", "Building a Semantic Table", "Querying Semantic Tables", "Advanced Patterns", "Query Methods", "Defining Semantic Tables", "Semantic Tables"]);

  const toggleSection = (title: string) => {
    setOpenSections((prev) =>
      prev.includes(title) ? prev.filter((t) => t !== title) : [...prev, title]
    );
  };

  const isActivePath = (path: string) => {
    return location.pathname === path;
  };

  return (
    <Sidebar className="border-r" collapsible="icon">
      <SidebarContent className="py-4">
        {navigationStructure.map((section) => {
          // Section with subsections
          if ("items" in section) {
            return (
              <Collapsible
                key={section.title}
                open={open && openSections.includes(section.title)}
                onOpenChange={() => toggleSection(section.title)}
              >
                <SidebarGroup>
                  <CollapsibleTrigger asChild>
                    <SidebarGroupLabel className="group/label cursor-pointer hover:bg-muted/50 rounded-md px-2 py-1.5">
                      <div className="flex items-center gap-2">
                        <section.icon className="h-4 w-4" />
                        <span className="flex-1">{section.title}</span>
                        {open && <ChevronRight className="h-4 w-4 transition-transform group-data-[state=open]/label:rotate-90" />}
                      </div>
                    </SidebarGroupLabel>
                  </CollapsibleTrigger>
                  {open && (
                    <CollapsibleContent>
                      <SidebarGroupContent>
                        <SidebarMenu>
                          {section.items.map((item) => {
                            // Check if item has subitems
                            if ('subitems' in item) {
                              return (
                                <Collapsible
                                  key={item.path}
                                  open={open && openSections.includes(item.title)}
                                  onOpenChange={() => toggleSection(item.title)}
                                >
                                  <SidebarMenuItem>
                                    <CollapsibleTrigger asChild>
                                      <SidebarMenuButton
                                        asChild
                                        isActive={isActivePath(item.path)}
                                        tooltip={item.title}
                                        className="pl-8"
                                      >
                                        <div className="flex items-center gap-2 w-full cursor-pointer">
                                          <NavLink to={item.path} className="flex-1">
                                            <span className="text-sm">{item.title}</span>
                                          </NavLink>
                                          {open && <ChevronRight className="h-3 w-3 transition-transform data-[state=open]:rotate-90" />}
                                        </div>
                                      </SidebarMenuButton>
                                    </CollapsibleTrigger>
                                  </SidebarMenuItem>
                                  {open && (
                                    <CollapsibleContent>
                                      <SidebarMenu>
                                        {item.subitems.map((subitem) => (
                                          <SidebarMenuItem key={subitem.hash}>
                                            <SidebarMenuButton
                                              asChild
                                              tooltip={subitem.title}
                                              className="pl-12"
                                            >
                                              <NavLink to={`${item.path}${subitem.hash}`}>
                                                <span className="text-xs">{subitem.title}</span>
                                              </NavLink>
                                            </SidebarMenuButton>
                                          </SidebarMenuItem>
                                        ))}
                                      </SidebarMenu>
                                    </CollapsibleContent>
                                  )}
                                </Collapsible>
                              );
                            }

                            // Regular item without subitems
                            return (
                              <SidebarMenuItem key={item.path}>
                                <SidebarMenuButton
                                  asChild
                                  isActive={isActivePath(item.path)}
                                  tooltip={item.title}
                                  className="pl-8"
                                >
                                  <NavLink to={item.path}>
                                    <span className="text-sm">{item.title}</span>
                                  </NavLink>
                                </SidebarMenuButton>
                              </SidebarMenuItem>
                            );
                          })}
                        </SidebarMenu>
                      </SidebarGroupContent>
                    </CollapsibleContent>
                  )}
                </SidebarGroup>
              </Collapsible>
            );
          }

          // Section without subsections
          return (
            <SidebarGroup key={section.title}>
              <SidebarMenu>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={isActivePath(section.path!)}
                    tooltip={section.title}
                  >
                    <NavLink to={section.path!} className="flex items-center gap-2">
                      <section.icon className="h-4 w-4" />
                      <span>{section.title}</span>
                    </NavLink>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              </SidebarMenu>
            </SidebarGroup>
          );
        })}

        <SidebarGroup className="mt-auto">
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton asChild tooltip="GitHub">
                  <a
                    href="https://github.com/boringdata/boring-semantic-layer"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    <Github className="h-4 w-4" />
                    <span>GitHub</span>
                  </a>
                </SidebarMenuButton>
              </SidebarMenuItem>
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
    </Sidebar>
  );
}
