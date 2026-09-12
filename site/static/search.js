// Client-side documentation search over Zola's elasticlunr index.
//
// No server, no third-party service, no request that leaves the reader's browser — which
// also means no analytics on what people search for. That trade is deliberate: a docs
// search box is a place readers type things like internal hostnames.
(function () {
  "use strict";

  var input = document.getElementById("search-input");
  var results = document.getElementById("search-results");
  if (!input || !results) return;

  var index = null;

  function ensureIndex() {
    if (index) return index;
    // Both are loaded with `defer` ahead of this file; if either is missing the box
    // simply stays inert rather than throwing on every keystroke.
    if (typeof window.elasticlunr === "undefined" || typeof window.searchIndex === "undefined") {
      return null;
    }
    index = window.elasticlunr.Index.load(window.searchIndex);
    return index;
  }

  function clear() {
    results.innerHTML = "";
    results.hidden = true;
  }

  function render(matches, term) {
    if (!matches.length) {
      results.innerHTML = "<p>No matches for “" + escapeHtml(term) + "”.</p>";
      results.hidden = false;
      return;
    }

    var html = matches
      .slice(0, 8)
      .map(function (match) {
        var doc = match.doc;
        var body = (doc.body || "").replace(/\s+/g, " ").slice(0, 110);
        return (
          '<a href="' + doc.id + '">' +
          "<strong>" + escapeHtml(doc.title || "Untitled") + "</strong>" +
          "<span>" + escapeHtml(body) + "…</span>" +
          "</a>"
        );
      })
      .join("");

    results.innerHTML = html;
    results.hidden = false;
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  var debounce;
  input.addEventListener("input", function () {
    clearTimeout(debounce);
    debounce = setTimeout(function () {
      var term = input.value.trim();
      if (term.length < 2) return clear();

      var idx = ensureIndex();
      if (!idx) return clear();

      // Prefix-expanded and fuzzy, weighted towards the title: readers search for the
      // name of a setting far more often than for prose around it.
      var matches = idx.search(term, {
        fields: { title: { boost: 3 }, body: { boost: 1 } },
        bool: "AND",
        expand: true
      });
      render(matches, term);
    }, 120);
  });

  // Dismissal: click outside, or Escape.
  document.addEventListener("click", function (event) {
    if (!results.contains(event.target) && event.target !== input) clear();
  });
  input.addEventListener("keydown", function (event) {
    if (event.key === "Escape") { clear(); input.blur(); }
  });

  // `/` focuses the box, the convention every developer docs site now shares — but not
  // while the reader is already typing into something.
  document.addEventListener("keydown", function (event) {
    if (event.key !== "/" || event.metaKey || event.ctrlKey || event.altKey) return;
    var tag = (event.target.tagName || "").toLowerCase();
    if (tag === "input" || tag === "textarea" || event.target.isContentEditable) return;
    event.preventDefault();
    input.focus();
  });
})();
