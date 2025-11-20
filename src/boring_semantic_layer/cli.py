"""CLI for Boring Semantic Layer tools."""

import http.server
import os
import socketserver
import sys
import threading
import webbrowser
from pathlib import Path

import click


@click.group()
def cli():
    """Boring Semantic Layer CLI tools."""
    pass


@cli.command()
@click.argument("md_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Output file path (default: same name with .html extension)",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["html", "markdown"]),
    default="html",
    help="Output format",
)
@click.option("--watch", "-w", is_flag=True, help="Watch for changes and auto-regenerate")
@click.option(
    "--serve",
    "-s",
    is_flag=True,
    help="Start local server and open in browser with live reload",
)
@click.option("--port", "-p", type=int, default=8000, help="Server port (default: 8000)")
def render(md_path: Path, output: Path, format: str, watch: bool, serve: bool, port: int):
    """Render markdown file with BSL queries to HTML with optional live reload.

    Examples:
        bsl render doc.md                    # Render to doc.html
        bsl render doc.md --watch            # Watch and regenerate on changes
        bsl render doc.md --serve            # Serve with live reload in browser
        bsl render doc.md -s -p 3000         # Serve on port 3000
    """
    from boring_semantic_layer.chart.md_renderer import cmd_render

    # Determine output path
    if output is None:
        output = md_path.with_suffix(".html")

    # Serve mode = watch + live server + browser
    if serve:
        click.echo(f"🚀 Starting development server on http://localhost:{port}")
        click.echo(f"📝 Rendering {md_path} → {output}")

        # Initial render
        success = cmd_render(md_path, output, format=format, watch=False)
        if not success:
            click.echo("❌ Initial render failed", err=True)
            sys.exit(1)

        # Start file watcher in background thread
        watcher_thread = threading.Thread(
            target=_watch_and_render,
            args=(md_path, output, format),
            daemon=True,
        )
        watcher_thread.start()

        # Start HTTP server with live reload
        _serve_with_livereload(output, port)

    # Regular watch mode
    elif watch:
        success = cmd_render(md_path, output, format=format, watch=True)
        sys.exit(0 if success else 1)

    # Single render
    else:
        success = cmd_render(md_path, output, format=format, watch=False)
        if success:
            click.echo(f"✅ Rendered to {output}")
        sys.exit(0 if success else 1)


def _watch_and_render(md_path: Path, output: Path, format: str):
    """Watch file and regenerate in background thread."""
    try:
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer
    except ImportError:
        click.echo("❌ Error: watchdog package required for --serve mode", err=True)
        click.echo("Install with: uv pip install watchdog", err=True)
        sys.exit(1)

    import time

    from boring_semantic_layer.chart.md_renderer import render_to_html

    class MarkdownChangeHandler(FileSystemEventHandler):
        def __init__(self, target_path: Path):
            self.target_path = target_path.resolve()
            self.last_modified = time.time()

        def on_modified(self, event):
            if event.is_directory:
                return

            event_path = Path(event.src_path).resolve()
            if event_path != self.target_path:
                return

            # Debounce
            current_time = time.time()
            if current_time - self.last_modified < 0.5:
                return

            self.last_modified = current_time
            click.echo("\n📝 File changed, regenerating...")
            try:
                render_to_html(md_path, output)
                click.echo(f"✅ Regenerated {output}")
            except Exception as e:
                click.echo(f"❌ Error: {e}", err=True)

    event_handler = MarkdownChangeHandler(md_path)
    observer = Observer()
    observer.schedule(event_handler, str(md_path.parent), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()


def _serve_with_livereload(html_path: Path, port: int):
    """Start HTTP server with live reload."""
    try:
        import livereload
    except ImportError:
        # Fallback to simple HTTP server
        click.echo("⚠️  livereload not installed, using simple HTTP server")
        click.echo("   For live reload: uv pip install livereload")
        _serve_simple(html_path, port)
        return

    server = livereload.Server()
    server.watch(str(html_path))

    # Open browser
    url = f"http://localhost:{port}"
    click.echo(f"🌐 Opening {url} in browser...")

    try:
        server.serve(port=port, host="localhost", root=str(html_path.parent), open_url_delay=1)
    except KeyboardInterrupt:
        click.echo("\n\n👋 Stopping server...")


def _serve_simple(html_path: Path, port: int):
    """Simple HTTP server without live reload."""
    os.chdir(html_path.parent)

    class Handler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, format, *args):
            pass  # Suppress logs

    with socketserver.TCPServer(("", port), Handler) as httpd:
        url = f"http://localhost:{port}/{html_path.name}"
        click.echo(f"🌐 Opening {url} in browser...")
        webbrowser.open(url)

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            click.echo("\n\n👋 Stopping server...")


def main():
    """Entry point for CLI."""
    cli()


if __name__ == "__main__":
    main()
