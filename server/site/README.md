# Documentation site

The published documentation at <https://hupe1980.github.io/rustcdc-server>, built with
[Zola](https://www.getzola.org).

```bash
zola --root site serve    # http://127.0.0.1:1111, live reload
zola --root site build    # output in site/public/
zola --root site check    # also validates external links
```

## Layout

| Path | What it is |
|---|---|
| `zola.toml` | Site config, including `base_url` and the sidebar navigation |
| `content/_index.md` | The landing page's code sample; the rest of that page is `templates/index.html` |
| `content/docs/` | Every documentation page, one Markdown file each |
| `templates/` | Tera templates |
| `sass/main.scss` | The whole stylesheet — no framework, no webfont |
| `static/` | Scripts, icons, the Open Graph image, `robots.txt` |

## Writing a page

Front matter drives navigation and search, so all three fields matter:

```toml
+++
title = "Page title"          # <h1>, sidebar label, <title>, breadcrumb
description = "One sentence."  # meta description, the lede under the h1, search result
weight = 30                    # sidebar order and the prev/next pager
+++
```

Do **not** start the body with an `# H1` or a table of contents. The template renders the
title, the description and a generated TOC; adding them by hand produces a duplicate
heading and a second TOC that goes stale.

Link between pages with Zola's internal syntax, never a bare relative path:

```markdown
See [delivery contracts](@/docs/concepts.md#3-delivery-contracts).
```

`@/…md` links are resolved at build time, so a renamed page or a heading that no longer
exists **fails the build** instead of shipping a dead link. That check is the reason the
build is the CI gate.

Anchors come from Zola's slugifier, which is not identical to GitHub's — `[sink.codec]`
becomes `sink-codec`, not `sinkcodec`. If you are unsure, build once and grep the
generated HTML for the `id`.

## Adding a page to the sidebar

Add it to `[[extra.nav]]` in `zola.toml`. The order there is editorial rather than
alphabetical, which is why it is not derived from the filesystem.

## Changing where the site is served

Set `base_url` in `zola.toml` and the `Sitemap:` line in `static/robots.txt`. Both are
absolute, and canonical URLs, Open Graph tags and the sitemap are all built from the
first. For a custom domain, also add `static/CNAME`.
