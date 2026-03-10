const eventColors = {
  takeoff: "#38bdf8",
  cruising: "#22c55e",
  descent: "#f59e0b",
  diverting: "#ef4444",
  landed: "#a855f7",
  gate_arrival: "#64748b",
  gate_departure: "#0ea5e9",
};

const map = L.map("map", {
  zoomControl: true,
  worldCopyJump: true,
}).setView([20, 0], 2);

L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: "&copy; OpenStreetMap contributors",
  maxZoom: 7,
  minZoom: 2,
}).addTo(map);

const markerLayer = L.layerGroup().addTo(map);
const snapshotCache = new Map();

const hourSlider = document.getElementById("hour-slider");
const hourSliderLabel = document.getElementById("hour-slider-label");
const hourLabel = document.getElementById("hour-label");
const tonnesNow = document.getElementById("tonnes-now");
const operatorsChart = document.getElementById("operators-chart");
const equipmentChart = document.getElementById("equipment-chart");
const eventLegend = document.getElementById("event-legend");
const statActiveAircraft = document.getElementById("stat-active-aircraft");
const statTakeoffs = document.getElementById("stat-takeoffs");
const statLandings = document.getElementById("stat-landings");

let hours = [];
let initialBoundsApplied = false;

function formatHourLabel(isoDateTime) {
  const d = new Date(isoDateTime);
  return d.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function formatTonnes(value) {
  return `${Math.round(value).toLocaleString()} tonnes`;
}

function formatInteger(value) {
  return Number(value || 0).toLocaleString();
}

function markerRadius(weight) {
  if (weight == null || Number.isNaN(weight)) return 4;
  return Math.max(3, Math.min(14, Math.sqrt(weight / 1200)));
}

function renderLegend() {
  const entries = Object.entries(eventColors)
    .map(
      ([event, color]) =>
        `<span class="legend-item"><span class="swatch" style="background:${color}"></span>${event}</span>`
    )
    .join("");
  eventLegend.innerHTML = entries;
}

function renderBarChart(container, rows, labelKey) {
  if (!rows || rows.length === 0) {
    container.innerHTML = "<p class='empty'>No data for this hour</p>";
    return;
  }

  const maxValue = Math.max(...rows.map((row) => Number(row.capacity_weight) || 0), 1);

  container.innerHTML = rows
    .slice(0, 10)
    .map((row) => {
      const label = row[labelKey] ?? "UNKNOWN";
      const value = Number(row.capacity_weight) || 0;
      const pct = (value / maxValue) * 100;
      return `
        <div class="bar-row">
          <span class="bar-label">${label}</span>
          <div class="bar-track">
            <div class="bar-fill" style="width:${pct.toFixed(2)}%"></div>
          </div>
          <span class="bar-value">${Math.round(value / 1000).toLocaleString()} t</span>
        </div>
      `;
    })
    .join("");
}

async function getSnapshot(snapshotPath) {
  if (snapshotCache.has(snapshotPath)) {
    return snapshotCache.get(snapshotPath);
  }

  const response = await fetch(`data/${snapshotPath}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${snapshotPath}`);
  }

  const geojson = await response.json();
  snapshotCache.set(snapshotPath, geojson);
  return geojson;
}

function renderMap(geojson) {
  markerLayer.clearLayers();

  const featuresLayer = L.geoJSON(geojson, {
    pointToLayer: (feature, latlng) => {
      const event = feature.properties.event;
      const color = eventColors[event] || "#94a3b8";
      const radius = markerRadius(feature.properties.available_capacity_weight);
      return L.circleMarker(latlng, {
        radius,
        color,
        fillColor: color,
        fillOpacity: 0.7,
        weight: 1,
      });
    },
    onEachFeature: (feature, layer) => {
      const p = feature.properties;
      layer.bindPopup(`
        <strong>${p.callsign ?? p.flight ?? "Unknown Flight"}</strong><br/>
        Event: ${p.event ?? "unknown"}<br/>
        Operator: ${p.operator ?? "UNKNOWN"}<br/>
        Equipment: ${p.equipment ?? "UNKNOWN"}<br/>
        Capacity (weight): ${p.available_capacity_weight ? Math.round(p.available_capacity_weight).toLocaleString() : "n/a"}
      `);
    },
  });

  markerLayer.addLayer(featuresLayer);

  if (!initialBoundsApplied && featuresLayer.getBounds().isValid()) {
    map.fitBounds(featuresLayer.getBounds(), { padding: [30, 30] });
    initialBoundsApplied = true;
  }
}

async function renderHour(index) {
  const row = hours[index];
  if (!row) return;

  const label = formatHourLabel(row.hour_start);
  hourLabel.textContent = label;
  hourSliderLabel.textContent = label;
  tonnesNow.textContent = formatTonnes(row.airborne_capacity_tonnes || 0);
  statActiveAircraft.textContent = formatInteger(row.active_aircraft);
  statTakeoffs.textContent = formatInteger(row.takeoffs_this_hour);
  statLandings.textContent = formatInteger(row.landings_this_hour);

  renderBarChart(operatorsChart, row.top_operators, "operator");
  renderBarChart(equipmentChart, row.top_equipment, "equipment");

  const geojson = await getSnapshot(row.snapshot);
  renderMap(geojson);
}

async function initialize() {
  renderLegend();

  const response = await fetch("data/hourly_stats.json");
  if (!response.ok) {
    throw new Error("Unable to load data/hourly_stats.json");
  }

  const payload = await response.json();
  hours = payload.hours || [];

  if (hours.length === 0) {
    throw new Error("hourly_stats.json contains no hourly rows");
  }

  hourSlider.min = "0";
  hourSlider.max = String(hours.length - 1);
  hourSlider.value = "0";

  hourSlider.addEventListener("input", async (event) => {
    const idx = Number(event.target.value);
    await renderHour(idx);
  });

  await renderHour(0);
}

initialize().catch((error) => {
  console.error(error);
  tonnesNow.textContent = "Failed to load data";
  hourLabel.textContent = "Error";
  hourSliderLabel.textContent = "Error";
  statActiveAircraft.textContent = "--";
  statTakeoffs.textContent = "--";
  statLandings.textContent = "--";
  operatorsChart.innerHTML = `<p class='empty'>${error.message}</p>`;
  equipmentChart.innerHTML = `<p class='empty'>${error.message}</p>`;
});
