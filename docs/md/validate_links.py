#!/usr/bin/env python3
"""
Validate internal links in markdown documentation against React routes.

This script:
1. Extracts all internal links from markdown files
2. Parses routes from App.tsx
3. Checks that all links have corresponding routes (or redirects)
4. Reports any broken links
"""

import re
import sys
from pathlib import Path


def extract_markdown_links(docs_dir: Path) -> set[str]:
    """Extract all internal links from markdown files."""
    links = set()
    # Pattern matches [text](/path) or [text](/path#anchor)
    link_pattern = re.compile(r"\]\((/[a-zA-Z][^)]*)\)")

    for md_file in docs_dir.glob("**/*.md"):
        content = md_file.read_text()
        for match in link_pattern.finditer(content):
            link = match.group(1)
            # Remove anchor if present
            link_without_anchor = link.split("#")[0]
            if link_without_anchor:  # Only add non-empty links
                links.add(link_without_anchor)

    return links


def extract_routes_from_app_tsx(app_tsx_path: Path) -> tuple[set[str], dict[str, str]]:
    """Extract routes and redirects from App.tsx."""
    routes = set()
    redirects = {}

    content = app_tsx_path.read_text()

    # Extract regular routes: <Route path="/some/path" ...
    route_pattern = re.compile(r'<Route\s+path="([^"]+)"')
    for match in route_pattern.finditer(content):
        path = match.group(1)
        if path not in ["*", "/"]:  # Skip wildcard and root
            routes.add(path)

    # Extract redirects: <Route path="/old/path" element={<Navigate to="/new/path" replace />} />
    redirect_pattern = re.compile(r'<Route\s+path="([^"]+)"[^>]*element=\{<Navigate\s+to="([^"]+)"')
    for match in redirect_pattern.finditer(content):
        old_path = match.group(1)
        new_path = match.group(2)
        redirects[old_path] = new_path

    return routes, redirects


def validate_links(
    links: set[str], routes: set[str], redirects: dict[str, str]
) -> tuple[list[str], list[tuple[str, str]]]:
    """
    Validate that all links have corresponding routes.

    Returns:
        Tuple of (broken_links, redirect_links)
    """
    broken_links = []
    redirect_links = []

    for link in sorted(links):
        if link in routes:
            # Direct route exists
            continue
        elif link in redirects:
            # Link redirects to another route
            redirect_links.append((link, redirects[link]))
        else:
            # Check if it matches a dynamic route (e.g., /examples/:slug)
            matched = False
            for route in routes:
                if ":" in route:
                    # Convert route pattern to regex by processing each segment
                    parts = route.split("/")
                    regex_parts = []
                    for part in parts:
                        if part.startswith(":"):
                            # Dynamic segment - match any non-slash characters
                            regex_parts.append("[^/]+")
                        else:
                            # Static segment - escape special regex chars
                            regex_parts.append(re.escape(part))
                    pattern = "/".join(regex_parts)
                    if re.match(f"^{pattern}$", link):
                        matched = True
                        break

            if not matched:
                broken_links.append(link)

    return broken_links, redirect_links


def main():
    """Main validation function."""
    # Get script directory
    script_dir = Path(__file__).parent
    docs_dir = script_dir / "doc"
    app_tsx_path = script_dir.parent / "web" / "src" / "App.tsx"

    if not docs_dir.exists():
        print(f"❌ Documentation directory not found: {docs_dir}")
        sys.exit(1)

    if not app_tsx_path.exists():
        print(f"❌ App.tsx not found: {app_tsx_path}")
        sys.exit(1)

    # Extract links and routes
    print("🔍 Extracting links from markdown files...")
    links = extract_markdown_links(docs_dir)
    print(f"   Found {len(links)} unique internal links")

    print("🔍 Extracting routes from App.tsx...")
    routes, redirects = extract_routes_from_app_tsx(app_tsx_path)
    print(f"   Found {len(routes)} routes and {len(redirects)} redirects")

    # Validate
    print("\n📋 Validating links...")
    print("=" * 70)

    broken_links, redirect_links = validate_links(links, routes, redirects)

    # Report redirects (info only)
    if redirect_links:
        print(f"\nℹ️  {len(redirect_links)} link(s) use redirects:")
        for old, new in redirect_links:
            print(f"   {old} → {new}")

    # Report broken links
    if broken_links:
        print(f"\n❌ Found {len(broken_links)} broken link(s):")
        for link in broken_links:
            # Find which files contain this link
            files_with_link = []
            for md_file in docs_dir.glob("**/*.md"):
                if link in md_file.read_text():
                    files_with_link.append(md_file.relative_to(docs_dir))

            print(f"\n   {link}")
            print(f"      Used in: {', '.join(str(f) for f in files_with_link)}")

        print("\n" + "=" * 70)
        print("❌ Link validation FAILED")
        sys.exit(1)
    else:
        print("\n✅ All links are valid!")
        print("=" * 70)
        sys.exit(0)


if __name__ == "__main__":
    main()
