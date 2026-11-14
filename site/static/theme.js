// Theme toggle, copy buttons, TOC scroll-spy, and horizontal scroll containers for wide
// tables. Deliberately one small file with no dependencies: everything here is progressive
// enhancement, and the page is fully readable if it never loads.
(function () {
  "use strict";

  // ── Theme ──────────────────────────────────────────────────────────────────
  // The initial theme is applied by an inline script in <head> to avoid a flash of the
  // wrong theme. This only handles the toggle itself.
  var toggle = document.querySelector("[data-theme-toggle]");
  if (toggle) {
    toggle.addEventListener("click", function () {
      var root = document.documentElement;
      var current = root.dataset.theme;
      if (!current) {
        // No explicit choice yet: flip away from whatever the OS is currently asking for.
        current = window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
      }
      var next = current === "dark" ? "light" : "dark";
      root.dataset.theme = next;
      try { localStorage.setItem("theme", next); } catch (e) {}
      followTheme();
    });
  }

  // ── Screenshots follow the chosen theme ────────────────────────────────────
  // A <picture>'s dark source asks the OS, not the toggle. Pin it to the explicit choice, so a
  // reader who switched themes sees the screenshots they switched to.
  function followTheme() {
    var chosen = document.documentElement.dataset.theme;
    document.querySelectorAll("picture source[data-theme-media], picture source[media*='prefers-color-scheme']").forEach(function (source) {
      if (!source.dataset.themeMedia) source.dataset.themeMedia = source.media;
      source.media = !chosen ? source.dataset.themeMedia : chosen === "dark" ? "all" : "not all";
    });
  }
  followTheme();

  // ── Screenshots open full size ─────────────────────────────────────────────
  // The image as shown — the theme's variant — in a <dialog>: Escape, a click or the button
  // closes it, and focus returns where it was.
  var shots = document.querySelectorAll(".prose picture img, .prose img.zoomable");
  if (shots.length && typeof HTMLDialogElement === "function") {
    var dialog = document.createElement("dialog");
    dialog.className = "lightbox";
    dialog.innerHTML = '<button type="button" class="lightbox-close" aria-label="Close">×</button><img alt="">';
    document.body.appendChild(dialog);
    var big = dialog.querySelector("img");
    dialog.addEventListener("click", function () { dialog.close(); });
    shots.forEach(function (img) {
      img.classList.add("is-zoomable");
      img.tabIndex = 0;
      img.setAttribute("role", "button");
      img.setAttribute("aria-label", (img.alt || "Screenshot") + " — open full size");
      var open = function () {
        big.src = img.currentSrc || img.src;
        big.alt = img.alt;
        dialog.showModal();
      };
      img.addEventListener("click", open);
      img.addEventListener("keydown", function (event) {
        if (event.key === "Enter" || event.key === " ") { event.preventDefault(); open(); }
      });
    });
  }

  // ── Copy buttons ───────────────────────────────────────────────────────────
  function attachCopy(button, getText) {
    button.addEventListener("click", function () {
      var text = getText();
      var done = function () {
        var original = button.textContent;
        button.textContent = "Copied";
        button.setAttribute("data-copied", "");
        setTimeout(function () {
          button.textContent = original;
          button.removeAttribute("data-copied");
        }, 1600);
      };
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(done, function () {});
      }
    });
  }

  document.querySelectorAll(".copy-btn[data-copy]").forEach(function (button) {
    attachCopy(button, function () { return button.getAttribute("data-copy"); });
  });

  // Wrap every code block so a copy button can be positioned over it.
  document.querySelectorAll(".prose pre").forEach(function (pre) {
    var wrapper = document.createElement("div");
    wrapper.className = "code-wrap";
    pre.parentNode.insertBefore(wrapper, pre);
    wrapper.appendChild(pre);

    var button = document.createElement("button");
    button.className = "copy-btn";
    button.type = "button";
    button.textContent = "Copy";
    button.setAttribute("aria-label", "Copy code to clipboard");
    wrapper.appendChild(button);
    attachCopy(button, function () { return pre.innerText; });
  });

  // ── Wide tables ────────────────────────────────────────────────────────────
  // The configuration reference has tables far wider than a phone. Each gets its own
  // scroll container so the page body itself never scrolls sideways.
  document.querySelectorAll(".prose table").forEach(function (table) {
    var scroller = document.createElement("div");
    scroller.className = "table-scroll";
    table.parentNode.insertBefore(scroller, table);
    scroller.appendChild(table);
  });

  // ── TOC scroll-spy ─────────────────────────────────────────────────────────
  var tocLinks = Array.prototype.slice.call(
    document.querySelectorAll(".toc-rail .toc a")
  );
  if (tocLinks.length && "IntersectionObserver" in window) {
    var byId = {};
    var headings = [];
    tocLinks.forEach(function (link) {
      var id = decodeURIComponent((link.getAttribute("href") || "").split("#")[1] || "");
      if (!id) return;
      var heading = document.getElementById(id);
      if (!heading) return;
      byId[id] = link;
      headings.push(heading);
    });

    var visible = new Set();
    var observer = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) visible.add(entry.target.id);
          else visible.delete(entry.target.id);
        });

        // Highlight the first heading currently on screen, in document order, so the
        // rail tracks reading position rather than whichever entry fired last.
        var activeId = null;
        for (var i = 0; i < headings.length; i++) {
          if (visible.has(headings[i].id)) { activeId = headings[i].id; break; }
        }
        tocLinks.forEach(function (link) { link.classList.remove("is-active"); });
        if (activeId && byId[activeId]) byId[activeId].classList.add("is-active");
      },
      { rootMargin: "-80px 0px -70% 0px", threshold: 0 }
    );

    headings.forEach(function (heading) { observer.observe(heading); });
  }
})();
