/* cordis-pic-ror docs — shared interactivity. */

(function () {
  "use strict";

  // ---- Number formatting helpers ----
  function fmt(n) {
    if (n == null || isNaN(n)) return "—";
    return Number(n).toLocaleString("en-US");
  }
  function pct(p, d) {
    if (p == null || isNaN(p)) return "—";
    return (p * 100).toFixed(d == null ? 1 : d) + "%";
  }
  window.fmt = fmt;
  window.pct = pct;

  // ---- Render quick stat cards on Overview page ----
  function renderOverviewStats() {
    const root = document.querySelector("[data-stats]");
    if (!root) return;
    const D = window.DATA || {};
    const tc = (D.summary && D.summary.table_counts) || {};
    const conf = (D.summary && D.summary.resolution_confidence) || {};
    const rq = D.review_queue || {};
    const ai = D.ai_review || {};
    const cards = [
      {
        label: "CORDIS participants",
        value: fmt(tc.cordis_participants),
        sub: fmt(tc.cordis_projects) + " projects across HE + H2020 + FP7"
      },
      {
        label: "Pipeline resolutions",
        value: fmt(tc.pic_ror_resolutions),
        sub:
          fmt(conf.high) + " high · " + fmt(conf.medium) +
          " medium · " + fmt(conf.review) + " review"
      },
      {
        label: "Unresolved (queue)",
        value: fmt(rq.total_pics),
        sub: fmt(rq.total_appearances) + " total appearances waiting on review"
      },
      {
        label: "AI-assisted curation",
        value: fmt(ai.accepted) + " / " + fmt(ai.total),
        sub:
          ai.generalized != null
            ? fmt(ai.generalized) + " generalized to parent"
            : "—"
      }
    ];
    root.innerHTML = cards
      .map(
        c =>
          '<div class="stat"><div class="label">' +
          c.label +
          '</div><div class="value">' +
          c.value +
          '</div><div class="sub">' +
          c.sub +
          "</div></div>"
      )
      .join("");
  }

  // ---- Pipeline diagram: populate inline counts ----
  function populateDiagram() {
    const D = window.DATA || {};
    const tc = (D.summary && D.summary.table_counts) || {};
    const conf = (D.summary && D.summary.resolution_confidence) || {};
    document.querySelectorAll(".diagram [data-count]").forEach(el => {
      const k = el.dataset.count;
      el.textContent = tc[k] != null ? fmt(tc[k]) : "—";
    });
    document.querySelectorAll(".diagram [data-conf]").forEach(el => {
      const k = el.dataset.conf;
      el.textContent = conf[k] != null ? fmt(conf[k]) : "—";
    });
  }

  // ---- Confidence bar (Overview page) ----
  function renderConfidenceBar() {
    const root = document.querySelector("[data-confidence-bar]");
    if (!root) return;
    const conf = (window.DATA && window.DATA.summary && window.DATA.summary.resolution_confidence) || {};
    const total = (conf.high || 0) + (conf.medium || 0) + (conf.review || 0);
    if (!total) return;
    const segs = [
      { k: "high", n: conf.high, label: "high (auto)" },
      { k: "medium", n: conf.medium, label: "medium (auto)" },
      { k: "review", n: conf.review, label: "review (human)" }
    ];
    root.innerHTML =
      '<div class="bar">' +
      segs
        .map(s => {
          const pctVal = ((s.n || 0) / total) * 100;
          return (
            '<span class="bar-segment ' +
            s.k +
            '" style="width:' +
            pctVal.toFixed(2) +
            '%">' +
            (pctVal > 6 ? fmt(s.n) : "") +
            "</span>"
          );
        })
        .join("") +
      "</div>" +
      '<div class="bar-legend">' +
      segs
        .map(
          s =>
            '<span><span class="swatch" style="background:var(--' +
            (s.k === "high" ? "good" : s.k === "medium" ? "warn" : "bad") +
            ');"></span>' +
            s.label +
            " — " +
            fmt(s.n) +
            "</span>"
        )
        .join("") +
      "</div>";
  }

  // ---- Top-N table (Numbers page) ----
  function renderTopTable() {
    const root = document.querySelector("[data-top-table]");
    if (!root) return;
    const top = (window.DATA && window.DATA.review_queue && window.DATA.review_queue.top) || [];
    if (!top.length) {
      root.innerHTML =
        '<p class="muted small">Top-N preview not available — run <code>just review-queue</code> to populate it.</p>';
      return;
    }
    const rows = top.map((r, i) => {
      const rorCell = r.pipeline_ror
        ? '<a href="https://ror.org/' + r.pipeline_ror + '" target="_blank" rel="noopener">' + r.pipeline_ror + "</a>"
        : '<span class="muted">—</span>';
      const confPill = r.pipeline_confidence
        ? '<span class="pill ' + r.pipeline_confidence + '">' + r.pipeline_confidence + "</span>"
        : "";
      return (
        "<tr>" +
        "<td class=num>" + (i + 1) + "</td>" +
        "<td>" + r.pic + "</td>" +
        "<td class=num>" + fmt(r.count) + "</td>" +
        "<td>" + (r.country || "") + "</td>" +
        "<td class=name>" + (r.legal_name || "").replace(/&/g, "&amp;").replace(/</g, "&lt;") + "</td>" +
        "<td>" + rorCell + "</td>" +
        "<td>" + confPill + "</td>" +
        "</tr>"
      );
    });
    root.innerHTML =
      "<table class=data><thead><tr>" +
      "<th>#</th><th>PIC</th><th class=num>count</th><th>cc</th>" +
      "<th>legal_name</th><th>pipeline ROR</th><th>conf.</th>" +
      "</tr></thead><tbody>" +
      rows.join("") +
      "</tbody></table>";
  }

  // ---- Coverage curve chart (Numbers page) ----
  function renderCoverageChart() {
    const svg = document.querySelector("svg[data-coverage-chart]");
    if (!svg) return;
    const points =
      (window.DATA && window.DATA.review_queue && window.DATA.review_queue.cumulative) || [];
    if (!points.length) {
      svg.innerHTML =
        '<text x="50%" y="50%" text-anchor="middle" fill="#888" font-family="var(--font-mono)" font-size="12">no data — run `just review-queue`</text>';
      return;
    }
    // log-scale x-axis (top_n) so the long tail compresses
    const W = 720, H = 240, ML = 50, MR = 20, MT = 16, MB = 30;
    const innerW = W - ML - MR, innerH = H - MT - MB;
    const minN = 1;
    const maxN = points[points.length - 1].top_n;
    function xs(n) { return ML + (Math.log10(n) - Math.log10(minN)) / (Math.log10(maxN) - Math.log10(minN)) * innerW; }
    function ys(p) { return MT + (1 - p) * innerH; }
    const path = points.map((pt, i) => (i === 0 ? "M" : "L") + xs(pt.top_n).toFixed(1) + "," + ys(pt.pct).toFixed(1)).join(" ");
    const area = path + " L" + xs(maxN).toFixed(1) + "," + (MT + innerH) + " L" + xs(minN).toFixed(1) + "," + (MT + innerH) + " Z";
    // X ticks at 10, 100, 1000, 10000
    const xTicks = [10, 100, 1000, 10000].filter(t => t <= maxN);
    const yTicks = [0, 0.25, 0.5, 0.75, 1.0];
    svg.setAttribute("viewBox", "0 0 " + W + " " + H);
    svg.setAttribute("preserveAspectRatio", "xMidYMid meet");
    let html = '<g class="axis">';
    yTicks.forEach(t => {
      const y = ys(t);
      html += '<line x1="' + ML + '" x2="' + (W - MR) + '" y1="' + y + '" y2="' + y + '" stroke-dasharray="2 4" />';
      html += '<text x="' + (ML - 8) + '" y="' + (y + 3) + '" text-anchor="end">' + (t * 100).toFixed(0) + "%</text>";
    });
    xTicks.forEach(t => {
      const x = xs(t);
      html += '<line x1="' + x + '" x2="' + x + '" y1="' + (MT + innerH) + '" y2="' + (MT + innerH + 4) + '" />';
      html += '<text x="' + x + '" y="' + (MT + innerH + 18) + '" text-anchor="middle">' + (t >= 1000 ? (t / 1000) + "k" : t) + "</text>";
    });
    html += "</g>";
    html += '<path class="area" d="' + area + '" />';
    html += '<path class="line" d="' + path + '" />';
    // Marker for current slider position (added by slider handler)
    html += '<circle class="marker" data-marker cx="-99" cy="-99" r="4" />';
    svg.innerHTML = html;
  }

  // ---- Coverage slider (Numbers page) ----
  function wireCoverageSlider() {
    const slider = document.querySelector("input[data-coverage-slider]");
    const readout = document.querySelector("[data-coverage-readout]");
    const summary = document.querySelector("[data-coverage-summary]");
    const marker = document.querySelector("svg[data-coverage-chart] [data-marker]");
    if (!slider) return;
    const points =
      (window.DATA && window.DATA.review_queue && window.DATA.review_queue.cumulative) || [];
    const total =
      (window.DATA && window.DATA.review_queue && window.DATA.review_queue.total_pics) || 0;
    const totalApp =
      (window.DATA && window.DATA.review_queue && window.DATA.review_queue.total_appearances) || 0;
    const totalParticipants =
      (window.DATA && window.DATA.summary && window.DATA.summary.table_counts &&
        window.DATA.summary.table_counts.cordis_participants) || 0;

    if (!total || !points.length) {
      slider.disabled = true;
      if (readout) readout.textContent = "—";
      return;
    }
    slider.min = 1;
    slider.max = total;
    slider.step = 1;
    slider.value = 100;

    function lookupPct(n) {
      // Linear search over the bucketed cumulative; small enough
      let prev = points[0];
      for (const p of points) {
        if (p.top_n >= n) return p.pct;
        prev = p;
      }
      return prev.pct;
    }
    function update() {
      const n = parseInt(slider.value, 10) || 0;
      const p = lookupPct(n);
      if (readout) readout.textContent = "top " + fmt(n);
      if (summary) {
        const covered = Math.round(p * totalApp);
        const boost = totalParticipants ? (covered / totalParticipants) * 100 : 0;
        summary.innerHTML =
          'Curating the top <span class="num">' + fmt(n) + '</span> entries covers ' +
          '<span class="num">' + fmt(covered) + '</span> of ' +
          '<span class="num">' + fmt(totalApp) + '</span> unresolved appearances ' +
          '(<span class="num">' + (p * 100).toFixed(1) + "%</span>) — " +
          "lifts the resolution rate on full participants by " +
          '<span class="num">' + boost.toFixed(2) + "pp</span>.";
      }
      if (marker) {
        const W = 720, MR = 20, ML = 50, MT = 16, H = 240, MB = 30;
        const innerW = W - ML - MR, innerH = H - MT - MB;
        const maxN = points[points.length - 1].top_n;
        const x = ML + (Math.log10(Math.max(1, n)) - 0) / (Math.log10(maxN) - 0) * innerW;
        const y = MT + (1 - p) * innerH;
        marker.setAttribute("cx", x.toFixed(1));
        marker.setAttribute("cy", y.toFixed(1));
      }
    }
    slider.addEventListener("input", update);
    update();
  }

  // ---- AI review summary (Curation page) ----
  function renderAiReview() {
    const root = document.querySelector("[data-ai-review]");
    if (!root) return;
    const ai = (window.DATA && window.DATA.ai_review) || {};
    if (!ai.total) {
      root.innerHTML = '<p class="muted small">No AI-review data found.</p>';
      return;
    }
    const acceptPct = ai.total ? (ai.accepted / ai.total) * 100 : 0;
    const genPct = ai.accepted ? (ai.generalized / ai.accepted) * 100 : 0;
    let coverageLine = "";
    if (ai.appearances_covered != null && ai.total_unresolved_appearances) {
      const c = ai.appearances_covered / ai.total_unresolved_appearances;
      coverageLine =
        '<p>If all accepted entries land in <code>pic_ror_overrides.yaml</code>, they would cover <span class="num mono">' +
        fmt(ai.appearances_covered) + "</span> of <span class=\"num mono\">" +
        fmt(ai.total_unresolved_appearances) +
        '</span> unresolved CORDIS appearances (<span class="num mono">' +
        (c * 100).toFixed(1) + "%</span>).</p>";
    }
    root.innerHTML =
      '<div class="grid-3">' +
      '<div class="stat"><div class="label">Reviewed</div><div class="value">' + fmt(ai.total) + '</div><div class="sub">PICs in <code>top_3000.yaml</code></div></div>' +
      '<div class="stat"><div class="label">Accepted</div><div class="value">' + fmt(ai.accepted) + '</div><div class="sub">' + acceptPct.toFixed(0) + '% of reviewed</div></div>' +
      '<div class="stat"><div class="label">Generalized to parent</div><div class="value">' + fmt(ai.generalized) + '</div><div class="sub">' + genPct.toFixed(0) + '% of accepts</div></div>' +
      "</div>" + coverageLine;
  }

  // ---- Active nav link ----
  function highlightActiveNav() {
    const path = location.pathname.split("/").pop() || "index.html";
    document.querySelectorAll("header.site nav a").forEach(a => {
      const href = a.getAttribute("href") || "";
      if (href === path || (path === "" && href === "index.html")) {
        a.classList.add("active");
      }
    });
  }

  // ---- Boot ----
  document.addEventListener("DOMContentLoaded", () => {
    highlightActiveNav();
    renderOverviewStats();
    populateDiagram();
    renderConfidenceBar();
    renderTopTable();
    renderCoverageChart();
    wireCoverageSlider();
    renderAiReview();
  });
})();
