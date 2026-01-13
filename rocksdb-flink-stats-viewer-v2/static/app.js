const COUNTER_GROUPS = [
  {
    title: "Block Cache",
    metrics: [
      "rocksdb.block.cache.miss",
      "rocksdb.block.cache.hit",
      "rocksdb.block.cache.add",
      "rocksdb.block.cache.add.failures",
      "rocksdb.block.cache.index.miss",
      "rocksdb.block.cache.index.hit",
      "rocksdb.block.cache.index.add",
      "rocksdb.block.cache.filter.miss",
      "rocksdb.block.cache.filter.hit",
      "rocksdb.block.cache.filter.add",
      "rocksdb.block.cache.data.miss",
      "rocksdb.block.cache.data.hit",
      "rocksdb.block.cache.data.add",
    ],
  },
  {
    title: "Bloom Filter",
    metrics: [
      "rocksdb.bloom.filter.useful",
      "rocksdb.bloom.filter.full.positive",
      "rocksdb.bloom.filter.full.true.positive",
      "rocksdb.bloom.filter.micros",
      "rocksdb.bloom.filter.prefix.checked",
      "rocksdb.bloom.filter.prefix.useful",
    ],
  },
  {
    title: "Memtable + Levels",
    metrics: [
      "rocksdb.memtable.hit",
      "rocksdb.memtable.miss",
      "rocksdb.l0.hit",
      "rocksdb.l1.hit",
      "rocksdb.l2andup.hit",
    ],
  },
  {
    title: "Key Counters",
    metrics: [
      "rocksdb.number.keys.written",
      "rocksdb.number.keys.read",
      "rocksdb.number.keys.updated",
    ],
  },
  {
    title: "Iterator Counters",
    metrics: [
      "rocksdb.number.db.seek",
      "rocksdb.number.db.seek.found",
      "rocksdb.number.db.next",
      "rocksdb.number.db.next.found",
      "rocksdb.number.db.prev",
      "rocksdb.number.db.prev.found",
    ],
  },
];

const HIST_METRICS = [
  "rocksdb.db.get.micros",
  "rocksdb.db.write.micros",
  "rocksdb.compaction.times.micros",
  "rocksdb.compaction.times.cpu_micros",
  "rocksdb.read.block.get.micros",
  "rocksdb.db.seek.micros",
  "rocksdb.sst.read.micros",
];

const MARKER_STYLES = {
  flush_started: { label: "Flush start", color: "#0f7c7e" },
  flush_finished: { label: "Flush end", color: "#1aa3a5" },
  compaction_started: { label: "Compaction start", color: "#f39b2f" },
  compaction_finished: { label: "Compaction end", color: "#d9790d" },
  stats_dump: { label: "Stats dump", color: "#6e6a63" },
};

const state = {
  experiments: [],
  currentExperiment: null,
  markers: [],
  series: {},
  markerOffset: 0,
  markerFilters: {
    flush_finished: true,
    compaction_finished: true,
    stats_dump: true,
  },
  customPlots: [],
  lsm: {
    frames: [],
    index: 0,
    playing: false,
    timer: null,
    maxLevels: 0,
    maxCounts: [],
    leftName: "left",
    rightName: "right",
  },
  stats: {
    dumps: [],
    startIndex: 0,
    endIndex: 0,
    playing: false,
    timer: null,
    fromStart: true,
  },
};

const STORAGE_KEY = "rocksdb-lsm-custom-plots-v2";
const TAB_KEY = "rocksdb-lsm-active-tab-v2";

const elements = {
  experimentSelect: document.getElementById("experiment-select"),
  operatorSelect: document.getElementById("operator-select"),
  metaSummary: document.getElementById("meta-summary"),
  tabButtons: Array.from(document.querySelectorAll(".tab-button")),
  tabPanels: Array.from(document.querySelectorAll(".tab-panel")),
  throughputChart: document.getElementById("throughput-chart"),
  throughputMeta: document.getElementById("throughput-meta"),
  throughputRange: document.getElementById("throughput-range"),
  hitRatioChart: document.getElementById("hitratio-chart"),
  hitRatioMeta: document.getElementById("hitratio-meta"),
  hitRatioRange: document.getElementById("hitratio-range"),
  toggleFlushEnd: document.getElementById("toggle-flush-end"),
  toggleCompactionEnd: document.getElementById("toggle-compaction-end"),
  toggleStatsDump: document.getElementById("toggle-stats-dump"),
  customPlotName: document.getElementById("custom-plot-name"),
  customPlotQuery: document.getElementById("custom-plot-query"),
  customPlotStep: document.getElementById("custom-plot-step"),
  customPlotAdd: document.getElementById("custom-plot-add"),
  customPlotSave: document.getElementById("custom-plot-save"),
  customCharts: document.getElementById("custom-charts"),
  lsmSection: document.getElementById("lsm-section"),
  lsmPrevBtn: document.getElementById("lsm-prev-btn"),
  lsmPlayBtn: document.getElementById("lsm-play-btn"),
  lsmNextBtn: document.getElementById("lsm-next-btn"),
  lsmFrameSlider: document.getElementById("lsm-frame-slider"),
  lsmSpeedSelect: document.getElementById("lsm-speed-select"),
  leftPanel: document.getElementById("left-panel"),
  rightPanel: document.getElementById("right-panel"),
  leftLevels: document.getElementById("left-levels"),
  rightLevels: document.getElementById("right-levels"),
  lsmDetails: document.getElementById("lsm-details"),
  statsPrevBtn: document.getElementById("stats-prev-btn"),
  statsPlayBtn: document.getElementById("stats-play-btn"),
  statsNextBtn: document.getElementById("stats-next-btn"),
  statsEndSlider: document.getElementById("stats-end-slider"),
  statsStartSlider: document.getElementById("stats-start-slider"),
  statsSpeedSelect: document.getElementById("stats-speed-select"),
  statsFromStart: document.getElementById("stats-from-start"),
  statsSummary: document.getElementById("stats-summary"),
  statsCounters: document.getElementById("stats-counters"),
  statsHistograms: document.getElementById("stats-histograms"),
};

function formatMs(ms) {
  if (ms == null) {
    return "-";
  }
  const seconds = ms / 1000;
  return `${seconds.toFixed(3)}s`;
}

function formatNumber(value) {
  if (value == null || Number.isNaN(value)) {
    return "-";
  }
  if (Number.isInteger(value)) {
    return value.toLocaleString("en-US");
  }
  return value.toLocaleString("en-US", { maximumFractionDigits: 3 });
}

function formatTimeLabel(timeMicros) {
  if (timeMicros == null) {
    return "-";
  }
  const date = new Date(timeMicros / 1000);
  return date.toLocaleTimeString("en-US", { hour12: false });
}

function formatDateTimeLabel(timeMicros) {
  if (timeMicros == null) {
    return "-";
  }
  const date = new Date(timeMicros / 1000);
  return date.toLocaleString("en-CA", { hour12: false });
}

function formatDumpLabel(dump, index) {
  if (!dump) {
    return "-";
  }
  const rel = formatMs(dump.t_rel_ms);
  return `#${index + 1} (${rel})`;
}

function setActiveTab(tabName) {
  if (!tabName) {
    return;
  }
  elements.tabButtons.forEach((button) => {
    const isActive = button.dataset.tab === tabName;
    button.classList.toggle("is-active", isActive);
  });
  elements.tabPanels.forEach((panel) => {
    const isActive = panel.dataset.tab === tabName;
    panel.classList.toggle("is-active", isActive);
  });
  try {
    localStorage.setItem(TAB_KEY, tabName);
  } catch (error) {
    // Ignore storage failures.
  }
}

function gotoLsmIndex(index) {
  if (index == null) {
    return;
  }
  toggleLsm(false);
  setLsmIndex(index);
  if (elements.lsmSection) {
    elements.lsmSection.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

function createSvg(width, height) {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("width", width);
  svg.setAttribute("height", height);
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("role", "img");
  return svg;
}

function renderChartLegend(container, usedTypes) {
  const legend = document.createElement("div");
  legend.className = "chart-legend";
  usedTypes.forEach((type) => {
    const style = MARKER_STYLES[type];
    if (!style) {
      return;
    }
    const item = document.createElement("div");
    item.className = "legend-item";
    const dot = document.createElement("div");
    dot.className = "legend-dot";
    dot.style.background = style.color;
    const label = document.createElement("span");
    label.textContent = style.label;
    item.appendChild(dot);
    item.appendChild(label);
    legend.appendChild(item);
  });
  container.appendChild(legend);
}

function renderLineChart(container, metaEl, rangeEl, series, markers, options) {
  const prevScroll = container.scrollLeft || 0;
  container.innerHTML = "";
  if (!series || !series.points || series.points.length === 0) {
    container.innerHTML = '<div class="chart-empty">No data available.</div>';
    if (metaEl) {
      metaEl.textContent = "";
    }
    if (rangeEl) {
      rangeEl.textContent = "";
    }
    return;
  }

  const points = series.points;
  const times = points.map((point) => point.time_micros);
  const width = Math.max(container.clientWidth || 600, 900);
  const height = 240;
  const margin = { top: 18, right: 20, bottom: 28, left: 56 };
  const chartWidth = width - margin.left - margin.right;
  const chartHeight = height - margin.top - margin.bottom;

  const minX = points[0].time_micros;
  const maxX = points[points.length - 1].time_micros;
  let minY = Math.min(...points.map((p) => p.value));
  let maxY = Math.max(...points.map((p) => p.value));
  if (minY === maxY) {
    maxY += 1;
    minY -= 1;
  }

  const scaleX = (time) =>
    margin.left + ((time - minX) / (maxX - minX || 1)) * chartWidth;
  const scaleY = (value) =>
    margin.top + (1 - (value - minY) / (maxY - minY || 1)) * chartHeight;

  const svg = createSvg(width, height);
  svg.style.minWidth = `${width}px`;

  const axis = document.createElementNS("http://www.w3.org/2000/svg", "line");
  axis.setAttribute("x1", margin.left);
  axis.setAttribute("y1", height - margin.bottom);
  axis.setAttribute("x2", width - margin.right);
  axis.setAttribute("y2", height - margin.bottom);
  axis.setAttribute("stroke", "#d8c9b6");
  svg.appendChild(axis);

  const yAxis = document.createElementNS("http://www.w3.org/2000/svg", "line");
  yAxis.setAttribute("x1", margin.left);
  yAxis.setAttribute("y1", margin.top);
  yAxis.setAttribute("x2", margin.left);
  yAxis.setAttribute("y2", height - margin.bottom);
  yAxis.setAttribute("stroke", "#d8c9b6");
  svg.appendChild(yAxis);

  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  const pathData = points
    .map((point, index) => {
      const command = index === 0 ? "M" : "L";
      return `${command}${scaleX(point.time_micros)},${scaleY(point.value)}`;
    })
    .join(" ");
  path.setAttribute("d", pathData);
  path.setAttribute("fill", "none");
  path.setAttribute("stroke", options?.lineColor || "#0f7c7e");
  path.setAttribute("stroke-width", "2");
  svg.appendChild(path);

  const hoverGroup = document.createElementNS("http://www.w3.org/2000/svg", "g");
  const hoverLine = document.createElementNS("http://www.w3.org/2000/svg", "line");
  hoverLine.setAttribute("stroke", "#272522");
  hoverLine.setAttribute("stroke-width", "1");
  hoverLine.setAttribute("opacity", "0");
  const hoverDot = document.createElementNS("http://www.w3.org/2000/svg", "circle");
  hoverDot.setAttribute("r", "4");
  hoverDot.setAttribute("fill", options?.lineColor || "#0f7c7e");
  hoverDot.setAttribute("stroke", "#fff7ee");
  hoverDot.setAttribute("stroke-width", "2");
  hoverDot.setAttribute("opacity", "0");
  hoverGroup.appendChild(hoverLine);
  hoverGroup.appendChild(hoverDot);
  svg.appendChild(hoverGroup);

  const overlay = document.createElementNS("http://www.w3.org/2000/svg", "rect");
  overlay.setAttribute("x", margin.left);
  overlay.setAttribute("y", margin.top);
  overlay.setAttribute("width", chartWidth);
  overlay.setAttribute("height", chartHeight);
  overlay.setAttribute("fill", "transparent");
  svg.appendChild(overlay);

  const tooltip = document.createElement("div");
  tooltip.className = "chart-tooltip";
  container.appendChild(tooltip);

  const yMaxText = document.createElementNS("http://www.w3.org/2000/svg", "text");
  yMaxText.setAttribute("x", 8);
  yMaxText.setAttribute("y", margin.top + 4);
  yMaxText.setAttribute("fill", "#6e6a63");
  yMaxText.setAttribute("font-size", "11");
  yMaxText.textContent = formatNumber(maxY);
  svg.appendChild(yMaxText);

  const yMinText = document.createElementNS("http://www.w3.org/2000/svg", "text");
  yMinText.setAttribute("x", 8);
  yMinText.setAttribute("y", height - margin.bottom);
  yMinText.setAttribute("fill", "#6e6a63");
  yMinText.setAttribute("font-size", "11");
  yMinText.textContent = formatNumber(minY);
  svg.appendChild(yMinText);

  const filterTypes = options?.filterTypes || {};
  const filteredMarkers = (markers || []).filter(
    (marker) => filterTypes[marker.event]
  );
  const visibleMarkers = filteredMarkers.filter(
    (marker) => marker.time_micros >= minX && marker.time_micros <= maxX
  );
  const markerTypes = new Set();

  visibleMarkers.forEach((marker) => {
    const style = MARKER_STYLES[marker.event] || MARKER_STYLES.stats_dump;
    markerTypes.add(marker.event);
    const x = scaleX(marker.time_micros);

    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", x);
    line.setAttribute("y1", margin.top);
    line.setAttribute("x2", x);
    line.setAttribute("y2", height - margin.bottom);
    line.setAttribute("stroke", style.color);
    line.setAttribute("stroke-width", "1");
    line.setAttribute("opacity", "0.45");
    svg.appendChild(line);

    const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("cx", x);
    circle.setAttribute("cy", margin.top + 4);
    circle.setAttribute("r", "4");
    circle.setAttribute("fill", style.color);
    circle.setAttribute("data-lsm-index", marker.lsm_index ?? "");
    const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
    title.textContent = `${style.label} @ ${formatTimeLabel(marker.time_micros)}`;
    circle.appendChild(title);
    circle.style.cursor = marker.lsm_index != null ? "pointer" : "default";
    circle.addEventListener("click", () => gotoLsmIndex(marker.lsm_index));
    svg.appendChild(circle);
  });

  const activeTime = options?.activeTime;
  if (activeTime != null && activeTime >= minX && activeTime <= maxX) {
    const x = scaleX(activeTime);
    const focusLine = document.createElementNS("http://www.w3.org/2000/svg", "line");
    focusLine.setAttribute("x1", x);
    focusLine.setAttribute("y1", margin.top);
    focusLine.setAttribute("x2", x);
    focusLine.setAttribute("y2", height - margin.bottom);
    focusLine.setAttribute("stroke", "#272522");
    focusLine.setAttribute("stroke-width", "2");
    focusLine.setAttribute("opacity", "0.6");
    svg.appendChild(focusLine);

    const focusCircle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    focusCircle.setAttribute("cx", x);
    focusCircle.setAttribute("cy", margin.top + 6);
    focusCircle.setAttribute("r", "7");
    focusCircle.setAttribute("fill", "#272522");
    focusCircle.setAttribute("stroke", "#fff7ee");
    focusCircle.setAttribute("stroke-width", "2");
    svg.appendChild(focusCircle);
  }

  container.appendChild(svg);
  renderChartLegend(container, Array.from(markerTypes));
  container.scrollLeft = prevScroll;

  if (metaEl) {
    const label = series.label || "";
    metaEl.textContent = `${label} (${points.length} pts)`;
  }
  if (rangeEl) {
    rangeEl.textContent = `${formatDateTimeLabel(minX)} → ${formatDateTimeLabel(maxX)}`;
  }

  const findNearestIndex = (target) => {
    let low = 0;
    let high = times.length - 1;
    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      const value = times[mid];
      if (value === target) {
        return mid;
      }
      if (value < target) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    if (low >= times.length) {
      return times.length - 1;
    }
    if (low <= 0) {
      return 0;
    }
    const before = times[low - 1];
    const after = times[low];
    return target - before <= after - target ? low - 1 : low;
  };

  const updateHover = (event) => {
    const rect = container.getBoundingClientRect();
    const x = event.clientX - rect.left + container.scrollLeft;
    const y = event.clientY - rect.top;
    if (x < margin.left || x > width - margin.right) {
      return;
    }
    const time = minX + ((x - margin.left) / chartWidth) * (maxX - minX || 1);
    const idx = findNearestIndex(time);
    const point = points[idx];
    if (!point) {
      return;
    }
    const cx = scaleX(point.time_micros);
    const cy = scaleY(point.value);

    hoverLine.setAttribute("x1", cx);
    hoverLine.setAttribute("x2", cx);
    hoverLine.setAttribute("y1", margin.top);
    hoverLine.setAttribute("y2", height - margin.bottom);
    hoverLine.setAttribute("opacity", "0.5");
    hoverDot.setAttribute("cx", cx);
    hoverDot.setAttribute("cy", cy);
    hoverDot.setAttribute("opacity", "1");

    tooltip.textContent = `${formatNumber(point.value)}`;
    const tooltipLeft = Math.min(x + 12, width - 160);
    const tooltipTop = Math.max(12, y - 12);
    tooltip.style.left = `${tooltipLeft}px`;
    tooltip.style.top = `${tooltipTop}px`;
    tooltip.classList.add("is-visible");
  };

  const clearHover = () => {
    hoverLine.setAttribute("opacity", "0");
    hoverDot.setAttribute("opacity", "0");
    tooltip.classList.remove("is-visible");
  };

  overlay.addEventListener("mousemove", updateHover);
  overlay.addEventListener("mouseleave", clearHover);
}

function buildLevels(container, maxLevels) {
  container.innerHTML = "";
  for (let i = 0; i < maxLevels; i += 1) {
    const row = document.createElement("div");
    row.className = "level-row";

    const label = document.createElement("div");
    label.className = "level-label";
    label.textContent = `L${i}`;

    const barWrap = document.createElement("div");
    barWrap.className = "level-bar-wrap";

    const bar = document.createElement("div");
    bar.className = "level-bar";
    bar.style.width = "0%";

    barWrap.appendChild(bar);

    const count = document.createElement("div");
    count.className = "level-count";
    count.textContent = "0";

    row.appendChild(label);
    row.appendChild(barWrap);
    row.appendChild(count);
    container.appendChild(row);
  }
}

function updatePanel(panel, container, levels, maxCounts, active) {
  panel.classList.toggle("is-active", active);
  const rows = container.querySelectorAll(".level-row");
  rows.forEach((row, index) => {
    const count = levels[index] || 0;
    const maxCount = maxCounts[index] || 1;
    const ratio = maxCount === 0 ? 0 : count / maxCount;
    const bar = row.querySelector(".level-bar");
    const countEl = row.querySelector(".level-count");
    bar.style.width = `${Math.min(1, ratio) * 100}%`;
    countEl.textContent = count;
  });
}

function setLsmDetails(frame) {
  const items = [];
  items.push(["Event", frame.event || "-"]);
  items.push(["Column Family", frame.cf_name || "-"]);
  items.push(["Job", frame.job != null ? frame.job : "-"]);
  items.push(["Relative Time", formatMs(frame.t_rel_ms)]);

  if (frame.meta) {
    Object.keys(frame.meta).forEach((key) => {
      items.push([key, String(frame.meta[key])]);
    });
  }

  elements.lsmDetails.innerHTML = "";
  items.forEach(([label, value]) => {
    const item = document.createElement("div");
    item.className = "detail-item";

    const keyEl = document.createElement("div");
    keyEl.className = "detail-key";
    keyEl.textContent = label;

    const valueEl = document.createElement("div");
    valueEl.className = "detail-value";
    valueEl.textContent = value;

    item.appendChild(keyEl);
    item.appendChild(valueEl);
    elements.lsmDetails.appendChild(item);
  });
}

function renderLsmFrame(index) {
  if (!state.lsm.frames.length) {
    elements.lsmDetails.innerHTML = "No LSM events were found.";
    return;
  }
  const frame = state.lsm.frames[index];
  const leftActive = frame.cf_name === state.lsm.leftName;
  const rightActive = frame.cf_name === state.lsm.rightName;

  updatePanel(elements.leftPanel, elements.leftLevels, frame.left_state, state.lsm.maxCounts, leftActive);
  updatePanel(elements.rightPanel, elements.rightLevels, frame.right_state, state.lsm.maxCounts, rightActive);

  setLsmDetails(frame);
  elements.lsmFrameSlider.value = index;
}

function setLsmIndex(nextIndex) {
  const safeIndex = Math.max(0, Math.min(nextIndex, state.lsm.frames.length - 1));
  state.lsm.index = safeIndex;
  renderLsmFrame(state.lsm.index);
  renderCharts();
}

function tickLsm() {
  if (!state.lsm.playing) {
    return;
  }
  if (state.lsm.index >= state.lsm.frames.length - 1) {
    toggleLsm(false);
    return;
  }
  setLsmIndex(state.lsm.index + 1);
}

function toggleLsm(forceState) {
  if (typeof forceState === "boolean") {
    state.lsm.playing = forceState;
  } else {
    state.lsm.playing = !state.lsm.playing;
  }

  if (state.lsm.playing) {
    const speed = parseFloat(elements.lsmSpeedSelect.value || "1");
    const interval = 600 / Math.max(0.25, speed);
    state.lsm.timer = setInterval(tickLsm, interval);
    elements.lsmPlayBtn.textContent = "Pause";
  } else {
    clearInterval(state.lsm.timer);
    state.lsm.timer = null;
    elements.lsmPlayBtn.textContent = "Play";
  }
}

function computeMaxCounts(frames, maxLevels) {
  const maxCounts = new Array(maxLevels).fill(0);
  frames.forEach((frame) => {
    for (let i = 0; i < maxLevels; i += 1) {
      const leftCount = frame.left_state[i] || 0;
      const rightCount = frame.right_state[i] || 0;
      maxCounts[i] = Math.max(maxCounts[i], leftCount, rightCount);
    }
  });
  return maxCounts.map((count) => (count === 0 ? 1 : count));
}

function getCounterDelta(metric, startDump, endDump) {
  const endValue = endDump?.counters?.[metric];
  if (endValue == null) {
    return null;
  }
  const startValue = startDump?.counters?.[metric];
  const startBase = startValue == null ? 0 : startValue;
  return endValue - startBase;
}

function renderCounters(startDump, endDump) {
  elements.statsCounters.innerHTML = "";
  COUNTER_GROUPS.forEach((group) => {
    const groupEl = document.createElement("div");
    groupEl.className = "stat-group";

    const title = document.createElement("div");
    title.className = "stat-group-title";
    title.textContent = group.title;

    const grid = document.createElement("div");
    grid.className = "stat-group-grid";

    group.metrics.forEach((metric) => {
      const value = getCounterDelta(metric, startDump, endDump);
      const item = document.createElement("div");
      item.className = "stat-item";

      const keyEl = document.createElement("div");
      keyEl.className = "stat-key";
      keyEl.textContent = metric;

      const valueEl = document.createElement("div");
      valueEl.className = "stat-value";
      valueEl.textContent = formatNumber(value);

      item.appendChild(keyEl);
      item.appendChild(valueEl);
      grid.appendChild(item);
    });

    groupEl.appendChild(title);
    groupEl.appendChild(grid);
    elements.statsCounters.appendChild(groupEl);
  });
}

function renderHistograms(endDump) {
  elements.statsHistograms.innerHTML = "";

  const header = document.createElement("div");
  header.className = "hist-row hist-head";
  header.innerHTML =
    "<div>Metric</div><div>P50</div><div>P95</div><div>P99</div><div>P100</div><div>COUNT</div><div>SUM</div>";
  elements.statsHistograms.appendChild(header);

  HIST_METRICS.forEach((metric) => {
    const hist = endDump?.histograms?.[metric];
    const row = document.createElement("div");
    row.className = "hist-row";

    const name = document.createElement("div");
    name.className = "hist-name";
    name.textContent = metric;

    const p50 = document.createElement("div");
    p50.textContent = formatNumber(hist?.p50);
    const p95 = document.createElement("div");
    p95.textContent = formatNumber(hist?.p95);
    const p99 = document.createElement("div");
    p99.textContent = formatNumber(hist?.p99);
    const p100 = document.createElement("div");
    p100.textContent = formatNumber(hist?.p100);
    const count = document.createElement("div");
    count.textContent = formatNumber(hist?.count);
    const sum = document.createElement("div");
    sum.textContent = formatNumber(hist?.sum);

    row.appendChild(name);
    row.appendChild(p50);
    row.appendChild(p95);
    row.appendChild(p99);
    row.appendChild(p100);
    row.appendChild(count);
    row.appendChild(sum);
    elements.statsHistograms.appendChild(row);
  });
}

function renderStats() {
  if (!state.stats.dumps.length) {
    elements.statsSummary.textContent = "No stats dumps found.";
    elements.statsCounters.innerHTML = "";
    elements.statsHistograms.innerHTML = "";
    return;
  }

  const startDump = state.stats.dumps[state.stats.startIndex];
  const endDump = state.stats.dumps[state.stats.endIndex];

  const startLabel = formatDumpLabel(startDump, state.stats.startIndex);
  const endLabel = formatDumpLabel(endDump, state.stats.endIndex);
  elements.statsSummary.textContent = `Range: ${startLabel} → ${endLabel}`;

  renderCounters(startDump, endDump);
  renderHistograms(endDump);
}

function setStatsStart(index) {
  const safeIndex = Math.max(0, Math.min(index, state.stats.dumps.length - 1));
  state.stats.startIndex = safeIndex;
  if (state.stats.endIndex < safeIndex) {
    state.stats.endIndex = safeIndex;
    elements.statsEndSlider.value = safeIndex;
  }
  elements.statsStartSlider.value = safeIndex;
  renderStats();
}

function setStatsEnd(index) {
  const safeIndex = Math.max(0, Math.min(index, state.stats.dumps.length - 1));
  state.stats.endIndex = safeIndex;
  if (state.stats.startIndex > safeIndex) {
    state.stats.startIndex = safeIndex;
    elements.statsStartSlider.value = safeIndex;
  }
  elements.statsEndSlider.value = safeIndex;
  renderStats();
}

function tickStats() {
  if (!state.stats.playing) {
    return;
  }
  if (state.stats.endIndex >= state.stats.dumps.length - 1) {
    toggleStats(false);
    return;
  }
  setStatsEnd(state.stats.endIndex + 1);
}

function toggleStats(forceState) {
  if (typeof forceState === "boolean") {
    state.stats.playing = forceState;
  } else {
    state.stats.playing = !state.stats.playing;
  }

  if (state.stats.playing) {
    const speed = parseFloat(elements.statsSpeedSelect.value || "1");
    const interval = 900 / Math.max(0.25, speed);
    state.stats.timer = setInterval(tickStats, interval);
    elements.statsPlayBtn.textContent = "Pause";
  } else {
    clearInterval(state.stats.timer);
    state.stats.timer = null;
    elements.statsPlayBtn.textContent = "Play";
  }
}

function setMeta(expName, lsmFrames, statsDumps) {
  const frameCount = lsmFrames?.length || 0;
  const dumpCount = statsDumps?.length || 0;
  const label = expName ? `${expName} | ` : "";
  elements.metaSummary.textContent = `${label}${frameCount} LSM frames | ${dumpCount} stats dumps`;
}

function renderCharts() {
  const series = state.series || {};
  const currentFrame = state.lsm.frames?.[state.lsm.index];
  const activeTime =
    currentFrame && currentFrame.time_micros != null
      ? currentFrame.time_micros + (state.markerOffset || 0)
      : null;
  renderLineChart(
    elements.throughputChart,
    elements.throughputMeta,
    elements.throughputRange,
    series.throughput,
    state.markers,
    {
      lineColor: "#0f7c7e",
      activeTime,
      filterTypes: state.markerFilters,
    }
  );
  renderLineChart(
    elements.hitRatioChart,
    elements.hitRatioMeta,
    elements.hitRatioRange,
    series.block_cache_hit_ratio,
    state.markers,
    {
      lineColor: "#f39b2f",
      activeTime,
      filterTypes: state.markerFilters,
    }
  );
  renderCustomCharts(activeTime);
}

function renderCustomCharts(activeTime) {
  if (!elements.customCharts) {
    return;
  }
  elements.customCharts.innerHTML = "";
  if (!state.customPlots.length) {
    return;
  }

  state.customPlots.forEach((plot) => {
    const panel = document.createElement("div");
    panel.className = "chart-panel";

    const header = document.createElement("div");
    header.className = "chart-header";

    const title = document.createElement("div");
    title.className = "chart-title";
    title.textContent = plot.name || "Custom Plot";

    const metaWrap = document.createElement("div");
    metaWrap.className = "chart-meta";
    const meta = document.createElement("div");
    const range = document.createElement("div");
    range.className = "chart-range";
    metaWrap.appendChild(meta);
    metaWrap.appendChild(range);

    const remove = document.createElement("button");
    remove.className = "btn";
    remove.type = "button";
    remove.textContent = "X";
    remove.title = "Remove plot";
    remove.addEventListener("click", () => {
      state.customPlots = state.customPlots.filter((item) => item.id !== plot.id);
      saveCustomPlots();
      renderCustomCharts(activeTime);
    });

    header.appendChild(title);
    header.appendChild(metaWrap);
    header.appendChild(remove);

    const body = document.createElement("div");
    body.className = "chart-body";
    const canvas = document.createElement("div");
    canvas.className = "chart-canvas";
    body.appendChild(canvas);

    panel.appendChild(header);
    panel.appendChild(body);
    elements.customCharts.appendChild(panel);

    renderLineChart(canvas, meta, range, plot.series, state.markers, {
      lineColor: "#4f6f5f",
      activeTime,
      filterTypes: state.markerFilters,
    });
  });
}

function saveCustomPlots() {
  const payload = state.customPlots.map((plot) => ({
    id: plot.id,
    name: plot.name,
    query: plot.query,
    step: plot.step,
  }));
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
  } catch (error) {
    // Ignore storage failures.
  }
}

function loadCustomPlots() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.filter((item) => item.query);
  } catch (error) {
    return [];
  }
}

async function refreshCustomPlots() {
  if (!state.customPlots.length) {
    renderCustomCharts(state.lsm.frames?.[state.lsm.index]?.time_micros || null);
    return;
  }
  const exp = state.currentExperiment?.experiment || {};
  const start = exp.stats_time_start_micros;
  const end = exp.stats_time_end_micros;
  if (!start || !end) {
    return;
  }
  const offset = state.markerOffset || 0;
  const rangeStart = start + offset;
  const rangeEnd = end + offset;

  for (const plot of state.customPlots) {
    try {
      const params = new URLSearchParams({
        query: plot.query,
        start: rangeStart,
        end: rangeEnd,
        step: String(Number.isFinite(plot.step) ? plot.step : 10),
      });
      const response = await fetch(`/api/query_range?${params.toString()}`);
      if (!response.ok) {
        continue;
      }
      const payload = await response.json();
      plot.series = {
        label: plot.name || payload.label || plot.query,
        points: payload.points || [],
      };
    } catch (error) {
      // Ignore per-plot failures.
    }
  }
  renderCustomCharts(state.lsm.frames?.[state.lsm.index]?.time_micros || null);
}

async function addCustomPlot() {
  const name = elements.customPlotName?.value?.trim();
  const query = elements.customPlotQuery?.value?.trim();
  const step = Number(elements.customPlotStep?.value || 10);
  if (!query) {
    return;
  }
  const exp = state.currentExperiment?.experiment || {};
  const start = exp.stats_time_start_micros;
  const end = exp.stats_time_end_micros;
  if (!start || !end) {
    return;
  }
  const offset = state.markerOffset || 0;
  const rangeStart = start + offset;
  const rangeEnd = end + offset;

  if (elements.customPlotAdd) {
    elements.customPlotAdd.disabled = true;
    elements.customPlotAdd.textContent = "Loading...";
  }

  try {
    const params = new URLSearchParams({
      query,
      start: rangeStart,
      end: rangeEnd,
      step: String(Number.isFinite(step) ? step : 10),
    });
    const response = await fetch(`/api/query_range?${params.toString()}`);
    if (!response.ok) {
      throw new Error("Query failed");
    }
    const payload = await response.json();
    const series = {
      label: name || payload.label || query,
      points: payload.points || [],
    };
    state.customPlots.push({
      id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
      name: name || query,
      query,
      step,
      series,
    });
    saveCustomPlots();
    elements.customPlotQuery.value = "";
    if (elements.customPlotName) {
      elements.customPlotName.value = "";
    }
    renderCharts();
  } catch (error) {
    if (elements.customPlotAdd) {
      elements.customPlotAdd.textContent = "Query failed";
    }
  } finally {
    if (elements.customPlotAdd) {
      elements.customPlotAdd.disabled = false;
      setTimeout(() => {
        if (elements.customPlotAdd) {
          elements.customPlotAdd.textContent = "Add plot";
        }
      }, 1200);
    }
  }
}

function applyExperiment(data) {
  state.currentExperiment = data;
  state.series = data.series || {};
  state.markers = data.markers?.items || [];
  state.markerOffset = data.markers?.time_offset_micros || 0;
  const stored = loadCustomPlots();
  state.customPlots = stored.map((plot) => ({
    ...plot,
    series: { label: plot.name || plot.query, points: [] },
  }));

  const lsm = data.lsm || { meta: {}, frames: [] };
  const stats = data.stats || { dumps: [] };

  state.lsm.frames = lsm.frames || [];
  state.lsm.index = 0;
  state.lsm.maxLevels = lsm.meta?.max_levels || 7;
  state.lsm.leftName = lsm.meta?.left_name || "left";
  state.lsm.rightName = lsm.meta?.right_name || "right";
  state.lsm.maxCounts = computeMaxCounts(state.lsm.frames, state.lsm.maxLevels);
  toggleLsm(false);

  buildLevels(elements.leftLevels, state.lsm.maxLevels);
  buildLevels(elements.rightLevels, state.lsm.maxLevels);
  elements.lsmFrameSlider.max = Math.max(0, state.lsm.frames.length - 1);

  const leftTitle = elements.leftPanel.querySelector(".panel-title");
  const rightTitle = elements.rightPanel.querySelector(".panel-title");
  if (leftTitle) {
    leftTitle.textContent = state.lsm.leftName;
  }
  if (rightTitle) {
    rightTitle.textContent = state.lsm.rightName;
  }

  if (state.lsm.frames.length) {
    renderLsmFrame(0);
  } else {
    elements.lsmDetails.textContent = "No LSM events were found.";
  }

  state.stats.dumps = stats.dumps || [];
  state.stats.startIndex = 0;
  state.stats.endIndex = Math.max(0, state.stats.dumps.length - 1);
  state.stats.fromStart = true;
  toggleStats(false);

  elements.statsFromStart.checked = true;
  elements.statsStartSlider.disabled = true;
  elements.statsStartSlider.max = Math.max(0, state.stats.dumps.length - 1);
  elements.statsEndSlider.max = Math.max(0, state.stats.dumps.length - 1);
  elements.statsStartSlider.value = state.stats.startIndex;
  elements.statsEndSlider.value = state.stats.endIndex;

  renderStats();
  renderCharts();
  refreshCustomPlots();
  setMeta(data.experiment?.name || "", state.lsm.frames, state.stats.dumps);
}

function wireControls() {
  if (elements.experimentSelect) {
    elements.experimentSelect.addEventListener("change", () => {
      const index = Number(elements.experimentSelect.value);
      const exp = state.experiments[index];
      if (exp) {
        loadExperimentEntry(exp);
      }
    });
  }
  if (elements.operatorSelect) {
    elements.operatorSelect.addEventListener("change", () => {
      const expIndex = Number(elements.experimentSelect.value);
      const exp = state.experiments[expIndex];
      if (!exp || !exp.logs) {
        return;
      }
      const logIndex = Number(elements.operatorSelect.value);
      const log = exp.logs[logIndex];
      if (log) {
        fetchJson(log.file).then((data) => data && applyExperiment(data));
      }
    });
  }
  elements.tabButtons.forEach((button) => {
    button.addEventListener("click", () => {
      setActiveTab(button.dataset.tab);
    });
  });
  elements.lsmPlayBtn.addEventListener("click", () => toggleLsm());
  elements.lsmPrevBtn.addEventListener("click", () => {
    toggleLsm(false);
    setLsmIndex(state.lsm.index - 1);
  });
  elements.lsmNextBtn.addEventListener("click", () => {
    toggleLsm(false);
    setLsmIndex(state.lsm.index + 1);
  });
  elements.lsmFrameSlider.addEventListener("input", (event) => {
    toggleLsm(false);
    setLsmIndex(Number(event.target.value));
  });
  elements.lsmSpeedSelect.addEventListener("change", () => {
    if (state.lsm.playing) {
      toggleLsm(false);
      toggleLsm(true);
    }
  });

  elements.statsPlayBtn.addEventListener("click", () => toggleStats());
  elements.statsPrevBtn.addEventListener("click", () => {
    toggleStats(false);
    setStatsEnd(state.stats.endIndex - 1);
  });
  elements.statsNextBtn.addEventListener("click", () => {
    toggleStats(false);
    setStatsEnd(state.stats.endIndex + 1);
  });
  elements.statsEndSlider.addEventListener("input", (event) => {
    toggleStats(false);
    setStatsEnd(Number(event.target.value));
  });
  elements.statsStartSlider.addEventListener("input", (event) => {
    toggleStats(false);
    setStatsStart(Number(event.target.value));
  });
  elements.statsFromStart.addEventListener("change", (event) => {
    state.stats.fromStart = event.target.checked;
    elements.statsStartSlider.disabled = state.stats.fromStart;
    if (state.stats.fromStart) {
      setStatsStart(0);
    }
  });
  elements.statsSpeedSelect.addEventListener("change", () => {
    if (state.stats.playing) {
      toggleStats(false);
      toggleStats(true);
    }
  });

  const updateFilters = () => {
    state.markerFilters.flush_finished = !!elements.toggleFlushEnd?.checked;
    state.markerFilters.compaction_finished = !!elements.toggleCompactionEnd?.checked;
    state.markerFilters.stats_dump = !!elements.toggleStatsDump?.checked;
    renderCharts();
  };

  if (elements.toggleFlushEnd) {
    elements.toggleFlushEnd.addEventListener("change", updateFilters);
  }
  if (elements.toggleCompactionEnd) {
    elements.toggleCompactionEnd.addEventListener("change", updateFilters);
  }
  if (elements.toggleStatsDump) {
    elements.toggleStatsDump.addEventListener("change", updateFilters);
  }

  if (elements.customPlotAdd) {
    elements.customPlotAdd.addEventListener("click", addCustomPlot);
  }

  if (elements.customPlotSave) {
    elements.customPlotSave.addEventListener("click", () => {
      saveCustomPlots();
      elements.customPlotSave.textContent = "Saved";
      setTimeout(() => {
        if (elements.customPlotSave) {
          elements.customPlotSave.textContent = "Save plots";
        }
      }, 1000);
    });
  }

  if (elements.customPlotQuery) {
    elements.customPlotQuery.addEventListener("keydown", (event) => {
      if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
        addCustomPlot();
      }
    });
  }
}

async function fetchJson(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) {
    return null;
  }
  return response.json();
}

function loadExperimentEntry(exp) {
  if (!exp) {
    return;
  }
  if (Array.isArray(exp.logs) && exp.logs.length) {
    const targetIndex = exp.logs.findIndex((log) =>
      String(log.operator || "").toLowerCase().includes("streamingjoinoperator")
    );
    const defaultIndex = targetIndex >= 0 ? targetIndex : 0;

    if (elements.operatorSelect) {
      elements.operatorSelect.innerHTML = "";
      exp.logs.forEach((log, idx) => {
        const option = document.createElement("option");
        option.value = String(idx);
        option.textContent = log.operator || `operator-${idx + 1}`;
        elements.operatorSelect.appendChild(option);
      });
      elements.operatorSelect.disabled = false;
      elements.operatorSelect.value = String(defaultIndex);
    }

    const log = exp.logs[defaultIndex];
    if (log?.file) {
      fetchJson(log.file).then((data) => data && applyExperiment(data));
    }
    return;
  }

  if (elements.operatorSelect) {
    elements.operatorSelect.innerHTML = "<option>default</option>";
    elements.operatorSelect.disabled = true;
  }
  if (exp.file) {
    fetchJson(exp.file).then((data) => data && applyExperiment(data));
  }
}

async function init() {
  wireControls();
  let savedTab = null;
  try {
    savedTab = localStorage.getItem(TAB_KEY);
  } catch (error) {
    savedTab = null;
  }
  setActiveTab(savedTab || "overview");
  if (elements.toggleFlushEnd) {
    elements.toggleFlushEnd.checked = state.markerFilters.flush_finished;
  }
  if (elements.toggleCompactionEnd) {
    elements.toggleCompactionEnd.checked = state.markerFilters.compaction_finished;
  }
  if (elements.toggleStatsDump) {
    elements.toggleStatsDump.checked = state.markerFilters.stats_dump;
  }
  window.addEventListener("resize", () => {
    if (state.currentExperiment) {
      renderCharts();
    }
  });

  const index = await fetchJson("index.json");
  if (index && Array.isArray(index.experiments) && index.experiments.length) {
    state.experiments = index.experiments;
    elements.experimentSelect.innerHTML = "";
    index.experiments.forEach((exp, idx) => {
      const option = document.createElement("option");
      option.value = String(idx);
      option.textContent = exp.name || `experiment-${idx + 1}`;
      elements.experimentSelect.appendChild(option);
    });
    elements.experimentSelect.value = "0";
    loadExperimentEntry(index.experiments[0]);
    return;
  }

  elements.experimentSelect.innerHTML = "<option>single-log</option>";
  elements.experimentSelect.disabled = true;
  if (elements.operatorSelect) {
    elements.operatorSelect.innerHTML = "<option>default</option>";
    elements.operatorSelect.disabled = true;
  }

  const data = await fetchJson("data.json");
  if (data) {
    applyExperiment(data);
    return;
  }

  elements.metaSummary.textContent = "Failed to load timeline data.";
}

init();
