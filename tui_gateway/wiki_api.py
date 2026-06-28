"""Wiki scanning API for the TUI gateway.

Provides filesystem-level wiki introspection for native clients that
render graph views or page detail. Reads from $WIKI_PATH (default ~/wiki).
"""
import os
import re
from pathlib import Path


def _default_wiki_path() -> str:
    env = os.environ.get("WIKI_PATH", "")
    if env:
        return env
    return os.path.expanduser("~/wiki")


def _parse_frontmatter(content: str) -> tuple[dict, str]:
    """Simple frontmatter parser — returns (metadata, body)."""
    if not content.startswith("---"):
        return {}, content
    parts = content.split("---", 2)
    if len(parts) < 3:
        return {}, content
    metadata = {}
    for line in parts[1].strip().split("\n"):
        line = line.strip()
        if ":" in line:
            key, val = line.split(":", 1)
            key = key.strip()
            val = val.strip()
            # strip outer quotes
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
                val = val[1:-1]
            metadata[key] = val
    return metadata, parts[2]


def _extract_wikilinks(body: str) -> list[str]:
    """Extract [[wikilinks]] from markdown body."""
    pattern = r'\[\[([^\]|]+)(?:\|[^\]]+)?\]\]'
    matches = re.findall(pattern, body)
    return [m.strip().lower().replace(" ", "-") for m in matches]


def wiki_scan(wiki_path: str | None = None) -> dict:
    """Scan wiki directory and return graph structure."""
    wiki = Path(wiki_path or _default_wiki_path())
    if not wiki.exists():
        return {"pages": [], "links": []}

    pages: list[dict] = []
    page_ids: set[str] = set()
    links: list[dict] = []
    subdirs = ["entities", "concepts", "comparisons", "queries", "raw"]

    # First pass: collect all pages
    for subdir in subdirs:
        dir_path = wiki / subdir
        if not dir_path.exists():
            continue
        for file in dir_path.iterdir():
            if file.suffix != ".md":
                continue
            try:
                content = file.read_text(encoding="utf-8")
            except Exception:
                continue
            fm, _ = _parse_frontmatter(content)
            slug = file.stem
            rel_path = f"{subdir}/{file.name}"

            # Parse tags (handles "[tag1, tag2]" or "tag1, tag2")
            raw_tags = fm.get("tags", "")
            tags: list[str] = []
            if raw_tags:
                cleaned = raw_tags.strip().strip("[]").replace("'", "").replace('"', "")
                tags = [t.strip() for t in cleaned.split(",") if t.strip()]

            pages.append(
                {
                    "id": slug,
                    "title": fm.get("title", slug),
                    "type": fm.get("type", "concept"),
                    "tags": tags,
                    "path": rel_path,
                    "created": fm.get("created", ""),
                    "updated": fm.get("updated", ""),
                    "confidence": fm.get("confidence", ""),
                    "contested": fm.get("contested", "").lower() == "true",
                }
            )
            page_ids.add(slug)

    # Second pass: extract wikilinks (only link to existing pages)
    for subdir in subdirs:
        dir_path = wiki / subdir
        if not dir_path.exists():
            continue
        for file in dir_path.iterdir():
            if file.suffix != ".md":
                continue
            try:
                content = file.read_text(encoding="utf-8")
            except Exception:
                continue
            _, body = _parse_frontmatter(content)
            slug = file.stem
            for target in _extract_wikilinks(body):
                if target in page_ids:
                    links.append({"source": slug, "target": target, "type": "wikilink"})

    return {"pages": pages, "links": links}


def wiki_page(path: str, wiki_path: str | None = None) -> dict | None:
    """Read a single wiki page by relative path (e.g. 'entities/dflash-mlx.md')."""
    wiki = Path(wiki_path or _default_wiki_path())
    target = wiki / path
    # Security: refuse to escape the wiki directory
    try:
        target = target.resolve()
        wiki = wiki.resolve()
    except Exception:
        return None
    if not str(target).startswith(str(wiki)):
        return None
    if not target.exists() or target.suffix != ".md":
        return None
    try:
        content = target.read_text(encoding="utf-8")
    except Exception:
        return None
    fm, body = _parse_frontmatter(content)
    return {"frontmatter": fm, "body": body, "path": path}


def wiki_taxonomy(wiki_path: Optional[str] = None) -> Optional[dict]:
    """Load and return the hierarchical taxonomy tree from taxonomy.yaml.
    
    Returns the full taxonomy dict with categories and nested children,
    or None if taxonomy.yaml doesn't exist."""
    wiki = Path(wiki_path or _default_wiki_path())
    taxonomy_path = wiki / "taxonomy.yaml"
    if not taxonomy_path.exists():
        return None
    try:
        with open(taxonomy_path) as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return None


def wiki_flatten_taxonomy(wiki_path: Optional[str] = None) -> list[str]:
    """Return a flat list of all valid taxonomy paths from taxonomy.yaml."""
    tree = wiki_taxonomy(wiki_path)
    if not tree:
        return []
    
    def _flatten(categories, prefix=""):
        paths = []
        for name, node in categories.items():
            if not isinstance(node, dict):
                continue
            path = f"{prefix}{name}" if prefix else name
            paths.append(path)
            if "children" in node and isinstance(node["children"], dict):
                paths.extend(_flatten(node["children"], f"{path}/"))
        return paths
    
    return sorted(_flatten(tree.get("categories", {})))


def wiki_changesets(
    wiki_path: Optional[str] = None,
    page: Optional[str] = None,
    action: Optional[str] = None,
    trigger: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> dict:
    """Query wiki changesets (timeline view).

    Args:
        wiki_path: Wiki root path override
        page: Filter by page path
        action: Filter by action ('create', 'update', 'archive', 'delete')
        trigger: Filter by trigger ('ingest', 'query', 'lint', 'process-inbox')
        limit: Max results (default 50)
        offset: Pagination offset
        since: ISO timestamp filter (after)
        until: ISO timestamp filter (before)

    Returns:
        {"changesets": [...], "total": N, "limit": L, "offset": O}
    """
    import sys
    # Try repo-relative first (bundled with hermes-agent), fall back to user scripts
    import os as _os
    _repo_scripts = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "scripts")
    for _d in (_repo_scripts, _os.path.join(_os.path.expanduser("~"), ".hermes", "scripts")):
        if _d not in sys.path:
            sys.path.insert(0, _d)
    from wiki_changeset import wiki_query_changesets
    return wiki_query_changesets(
        wiki_path=wiki_path,
        page=page,
        action=action,
        trigger=trigger,
        limit=limit,
        offset=offset,
        since=since,
        until=until,
    )


def wiki_expand_links(page_slug: str, wiki_path: Optional[str] = None) -> dict:
    """Expand integration_links for a wiki page into live status.
    
    Currently resolves GitHub and Linear links. Returns a dict
    mapping each link to a status object. Other link types return
    a 'pending' status with the original value.
    
    Example return:
        {"github:hermes-agent#456": {"status": "merged", "title": "Fix wiki...", "url": "..."}}
    """
    wiki = Path(wiki_path or _default_wiki_path())
    
    # Find the page by slug
    for subdir in ["entities", "concepts", "comparisons", "queries",
                       "projects", "goals", "life", "issues"]:
        file_path = wiki / subdir / f"{page_slug}.md"
        if file_path.exists():
            break
    else:
        return {"error": f"Page '{page_slug}' not found"}
    
    try:
        content = file_path.read_text(encoding="utf-8")
    except Exception:
        return {"error": f"Could not read '{page_slug}'"}
    
    fm, _ = _parse_frontmatter(content)
    links = fm.get("integration_links", [])
    if not isinstance(links, list):
        return {}
    
    result = {}
    for link in links:
        if not isinstance(link, str) or ":" not in link:
            continue
        prefix, rest = link.split(":", 1)
        prefix = prefix.lower()
        
        if prefix == "github":
            # Parse org/repo#num
            if "#" in rest:
                repo_path, num = rest.rsplit("#", 1)
                result[link] = {
                    "type": "github",
                    "repo": repo_path,
                    "number": num,
                    "url": f"https://github.com/{repo_path}/pull/{num}",
                    "status": "unknown",
                    "title": f"{repo_path}#{num}",
                }
            else:
                result[link] = {"type": "github", "repo": rest, "status": "unknown", "title": rest}
        elif prefix == "linear":
            result[link] = {
                "type": "linear",
                "issue_id": rest,
                "url": f"https://linear.app/issue/{rest}",
                "status": "unknown",
                "title": rest,
            }
        elif prefix == "notion":
            result[link] = {"type": "notion", "url": rest, "status": "unknown", "title": "Notion page"}
        elif prefix == "obsidian":
            result[link] = {"type": "obsidian", "note": rest, "status": "unknown", "title": rest}
        elif prefix == "slack":
            result[link] = {"type": "slack", "channel_msg": rest, "status": "unknown", "title": rest}
        else:
            result[link] = {"type": prefix, "raw": rest, "status": "unknown", "title": f"{prefix}:{rest}"}
    
    return result
