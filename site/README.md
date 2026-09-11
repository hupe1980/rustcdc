# Documentation site

The published documentation at <https://hupe1980.github.io/rustcdc>, built with
[Zola](https://www.getzola.org).

One site covers both halves of the repository: `/docs/` is the **server**, `/library/` is
the **`rustcdc` crate** you embed. They used to be two sites in two repositories, which
meant two search indexes, two navigation trees, and a cross-reference between them that
could only ever be a bare URL.

> [!IMPORTANT]
> **Zola 0.23 removed shortcodes and made every content file a Tera template.**
> `config.toml` therefore sets `skip_content_templating = ["**/*.md"]`, which turns that
> pass off for all content. Do not remove it: without it, any code sample containing
> `{{` — a Rust `format!` escape, a Prometheus annotation, a Helm value, a Vue snippet —
> fails the build with a template error in a file that contains no template. Upstream
> considers this intended (getzola/zola#3263, closed "not going to happen by default").
>
> The practical consequence for writers: **Tera syntax does nothing in these pages.**
> There are no shortcodes, `{% raw %}` is not needed and would render literally, and a
> value that must stay in step with the code (the MSRV, say) is written as a literal and
> checked by `server/tests/architecture.rs` rather than interpolated.

```bash
zola --root site serve    # http://127.0.0.1:1111, live reload
zola --root site build    # output in site/public/
zola --root site check    # also validates external links
```

## Layout

| Path | What it is |
|---|---|
| `config.toml` | Site config, including `base_url`, the release version and the sidebar navigation |
| `content/_index.md` | The landing page's code sample; the rest of that page is `templates/index.html` |
| `content/docs/` | Server documentation — the operator-facing pages |
| `content/library/` | `rustcdc` crate documentation, also embedded into rustdoc by `src/lib.rs` |
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
See [the API guide](@/library/api.md).
```

`@/…md` links are resolved at build time, so a renamed page or a heading that no longer
exists **fails the build** instead of shipping a dead link. That check is the reason the
build is the CI gate.

Anchors come from Zola's slugifier, which is not identical to GitHub's — `[sink.codec]`
becomes `sink-codec`, not `sinkcodec`. If you are unsure, build once and grep the
generated HTML for the `id`.

## Adding a page to the sidebar

Add it to `[[extra.nav]]` in `config.toml`. The order there is editorial rather than
alphabetical, which is why it is not derived from the filesystem.

## Changing where the site is served

Set `base_url` in `config.toml` and the `Sitemap:` line in `static/robots.txt`. Both are
absolute, and canonical URLs, Open Graph tags and the sitemap are all built from the
first. For a custom domain, also add `static/CNAME`.

## The library pages are compiled, not just published

`src/lib.rs` embeds `content/library/*.md` with `include_str!`, so every Rust code block
on those pages is compiled by `cargo test --doc`. A sample that stops compiling fails the
build rather than rotting quietly on the site.

Two consequences when editing under `content/library/`:

- A Rust block that cannot compile here needs ` ```rust,ignore ` and a comment saying why.
- The files cannot move without updating `src/lib.rs`, and they must stay inside the
  package root — `cargo package` collects nothing above it, so docs.rs would lose the book.
