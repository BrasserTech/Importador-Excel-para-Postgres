const storageKeys = {
  connection: "importador.pg.connection",
  schema: "importador.pg.schema",
  selectedTable: "importador.pg.table",
  mappingByTable: "importador.pg.mappingByTable",
  excel: "importador.excel",
};

function $(id) {
  return document.getElementById(id);
}

function setStatus(el, text) {
  el.textContent = text || "";
}

function safeJsonParse(raw, fallback) {
  try {
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function readCache(key, fallback) {
  const raw = localStorage.getItem(key);
  if (raw == null) return fallback;
  return safeJsonParse(raw, fallback);
}

function writeCache(key, value) {
  localStorage.setItem(key, JSON.stringify(value));
}

function clearCache() {
  Object.values(storageKeys).forEach((k) => localStorage.removeItem(k));
}

function normalizeKey(input) {
  return String(input || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[_-]+/g, "");
}

function getConnectionFromForm() {
  return {
    host: $("pgHost").value.trim(),
    port: Number($("pgPort").value || 5432),
    user: $("pgUser").value.trim(),
    password: $("pgPassword").value,
    database: $("pgDatabase").value.trim(),
    ssl: $("pgSsl").checked,
  };
}

function setConnectionToForm(connection) {
  $("pgHost").value = connection?.host ?? "";
  $("pgPort").value = String(connection?.port ?? 5432);
  $("pgUser").value = connection?.user ?? "";
  $("pgPassword").value = connection?.password ?? "";
  $("pgDatabase").value = connection?.database ?? "";
  $("pgSsl").checked = Boolean(connection?.ssl);
}

function getSchemaFromForm() {
  const schema = $("pgSchema").value.trim();
  return schema || "public";
}

function setSchemaToForm(schema) {
  $("pgSchema").value = schema || "public";
}

function setTabs() {
  const tabs = Array.from(document.querySelectorAll(".tab"));
  const panels = Array.from(document.querySelectorAll(".panel"));

  function activate(name) {
    tabs.forEach((t) => {
      const isActive = t.dataset.tab === name;
      t.classList.toggle("is-active", isActive);
      t.setAttribute("aria-selected", isActive ? "true" : "false");
    });
    panels.forEach((p) => {
      p.classList.toggle("is-active", p.id === `tab-${name}`);
    });
  }

  tabs.forEach((t) => {
    t.addEventListener("click", () => activate(t.dataset.tab));
  });

  activate("conexao");
}

async function apiJson(url, payload) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await res.json().catch(() => null);
  return { ok: res.ok, data };
}

async function apiForm(url, formData) {
  const res = await fetch(url, { method: "POST", body: formData });
  const data = await res.json().catch(() => null);
  return { ok: res.ok, data };
}

let schemaState = null;
let excelState = {
  sheetNames: [],
  sheetName: "",
  headerRow: 1,
  headers: [],
  sampleRows: [],
};

function renderSchemaPreview() {
  const el = $("schemaPreview");
  if (!schemaState) {
    el.textContent = "";
    return;
  }
  el.textContent = JSON.stringify(schemaState, null, 2);
}

function fillTablesSelect() {
  const select = $("dbTable");
  const current = readCache(storageKeys.selectedTable, "");
  select.innerHTML = "";

  const empty = document.createElement("option");
  empty.value = "";
  empty.textContent = "Selecione...";
  select.appendChild(empty);

  if (!schemaState?.tables?.length) return;
  for (const t of schemaState.tables) {
    const opt = document.createElement("option");
    opt.value = t.table;
    opt.textContent = t.table;
    select.appendChild(opt);
  }
  if (current) select.value = current;
}

function fillSheetsSelect() {
  const select = $("excelSheet");
  select.innerHTML = "";
  const empty = document.createElement("option");
  empty.value = "";
  empty.textContent = "Selecione...";
  select.appendChild(empty);

  for (const s of excelState.sheetNames || []) {
    const opt = document.createElement("option");
    opt.value = s;
    opt.textContent = s;
    select.appendChild(opt);
  }

  const cached = readCache(storageKeys.excel, {});
  if (cached.sheetName) select.value = cached.sheetName;
}

function getCurrentColumnsForSelectedTable() {
  const table = $("dbTable").value;
  const found = schemaState?.tables?.find((t) => t.table === table);
  return found?.columns || [];
}

function getMappingCache() {
  return readCache(storageKeys.mappingByTable, {});
}

function getCurrentMappingKey() {
  const table = $("dbTable").value;
  const headersSig = (excelState.headers || []).map((h) => normalizeKey(h)).filter(Boolean).join("|");
  return `${table}::${headersSig}`;
}

function loadCachedMappingIntoUI() {
  const mappingByTable = getMappingCache();
  const key = getCurrentMappingKey();
  const cached = mappingByTable[key];
  if (!cached) return;

  const selects = Array.from(document.querySelectorAll("[data-excel-col]"));
  for (const s of selects) {
    const excelCol = s.getAttribute("data-excel-col");
    const mapped = cached[excelCol];
    if (mapped) s.value = mapped;
  }
}

function readMappingFromUI() {
  const selects = Array.from(document.querySelectorAll("[data-excel-col]"));
  const mapping = {};
  for (const s of selects) {
    const excelCol = s.getAttribute("data-excel-col");
    const dbCol = s.value;
    if (excelCol && dbCol) mapping[excelCol] = dbCol;
  }
  return mapping;
}

function renderExcelPreview() {
  $("excelPreview").textContent = JSON.stringify(
    { sheetName: excelState.sheetName, headerRow: excelState.headerRow, headers: excelState.headers, sampleRows: excelState.sampleRows },
    null,
    2,
  );
}

function renderMappingGrid() {
  const grid = $("mappingGrid");
  grid.innerHTML = "";

  const dbColumns = getCurrentColumnsForSelectedTable().map((c) => c.name);

  for (const header of excelState.headers || []) {
    const row = document.createElement("div");
    row.className = "mapping__row";

    const left = document.createElement("div");
    left.className = "pill";
    left.textContent = header;

    const select = document.createElement("select");
    select.className = "select";
    select.setAttribute("data-excel-col", header);

    const empty = document.createElement("option");
    empty.value = "";
    empty.textContent = "Não mapear";
    select.appendChild(empty);

    for (const col of dbColumns) {
      const opt = document.createElement("option");
      opt.value = col;
      opt.textContent = col;
      select.appendChild(opt);
    }

    row.appendChild(left);
    row.appendChild(select);
    grid.appendChild(row);
  }

  loadCachedMappingIntoUI();
}

function autoMap() {
  const dbCols = getCurrentColumnsForSelectedTable().map((c) => c.name);
  const dbByKey = new Map();
  for (const c of dbCols) {
    const k = normalizeKey(c);
    if (k && !dbByKey.has(k)) dbByKey.set(k, c);
  }

  const selects = Array.from(document.querySelectorAll("[data-excel-col]"));
  for (const s of selects) {
    const excelCol = s.getAttribute("data-excel-col");
    const match = dbByKey.get(normalizeKey(excelCol));
    if (match) s.value = match;
  }
}

async function previewExcel() {
  const file = $("excelFile").files?.[0];
  if (!file) return;

  const sheetName = $("excelSheet").value;
  const headerRow = Number($("excelHeaderRow").value || 1);

  const fd = new FormData();
  fd.append("file", file);
  if (sheetName) fd.append("sheetName", sheetName);
  fd.append("headerRow", String(headerRow));

  setStatus($("mappingStatus"), "Lendo Excel...");
  const res = await apiForm("/api/excel/preview", fd);
  if (!res.ok || !res.data?.ok) {
    setStatus($("mappingStatus"), res.data?.error || "Erro ao ler Excel.");
    return;
  }

  excelState = res.data;
  $("excelHeaderRow").value = String(excelState.headerRow || 1);
  $("excelSheet").value = excelState.sheetName || "";

  writeCache(storageKeys.excel, { sheetName: excelState.sheetName, headerRow: excelState.headerRow });

  renderExcelPreview();
  renderMappingGrid();
  setStatus($("mappingStatus"), `Colunas lidas: ${excelState.headers?.length || 0}`);
}

async function testConnection() {
  setStatus($("testConnStatus"), "Testando...");
  const res = await apiJson("/api/test-connection", { connection: getConnectionFromForm() });
  if (!res.ok || !res.data?.ok) {
    setStatus($("testConnStatus"), res.data?.error || "Falha.");
    return;
  }
  setStatus($("testConnStatus"), "OK");
}

async function loadSchema() {
  setStatus($("schemaStatus"), "Carregando...");
  const res = await apiJson("/api/schema", { connection: getConnectionFromForm(), schema: getSchemaFromForm() });
  if (!res.ok || !res.data?.ok) {
    setStatus($("schemaStatus"), res.data?.error || "Falha.");
    return;
  }
  schemaState = res.data;
  renderSchemaPreview();
  fillTablesSelect();
  setStatus($("schemaStatus"), `OK (${schemaState.tables?.length || 0} tabelas)`);
  if (excelState.headers?.length) renderMappingGrid();
}

async function doImport() {
  const file = $("excelFile").files?.[0];
  if (!file) {
    setStatus($("importStatus"), "Selecione um arquivo Excel na aba Mapeamento.");
    return;
  }
  if (!schemaState?.tables?.length) {
    setStatus($("importStatus"), "Carregue o schema antes de importar (aba Testes).");
    return;
  }
  if (!excelState.headers?.length) {
    setStatus($("importStatus"), "Leia as colunas do Excel antes de importar (aba Mapeamento).");
    return;
  }

  const table = $("dbTable").value;
  if (!table) {
    setStatus($("importStatus"), "Selecione a tabela destino.");
    return;
  }

  const mapping = readMappingFromUI();
  if (!Object.keys(mapping).length) {
    setStatus($("importStatus"), "Mapeie ao menos uma coluna.");
    return;
  }

  const fd = new FormData();
  fd.append("file", file);
  fd.append("connection", JSON.stringify(getConnectionFromForm()));
  fd.append("schema", getSchemaFromForm());
  fd.append("table", table);
  fd.append("sheetName", $("excelSheet").value || excelState.sheetName || "");
  fd.append("headerRow", String(Number($("excelHeaderRow").value || excelState.headerRow || 1)));
  fd.append("mapping", JSON.stringify(mapping));

  setStatus($("importStatus"), "Importando...");
  $("importResult").textContent = "";
  const res = await apiForm("/api/import", fd);
  if (!res.ok || !res.data?.ok) {
    setStatus($("importStatus"), res.data?.error || "Erro ao importar.");
    $("importResult").textContent = JSON.stringify(res.data, null, 2);
    return;
  }
  setStatus($("importStatus"), `OK (inseridas: ${res.data.inserted})`);
  $("importResult").textContent = JSON.stringify(res.data, null, 2);
}

function bindEvents() {
  $("saveConnBtn").addEventListener("click", () => {
    writeCache(storageKeys.connection, getConnectionFromForm());
    writeCache(storageKeys.schema, getSchemaFromForm());
    setStatus($("connStatus"), "Salvo.");
  });

  $("testConnBtn").addEventListener("click", testConnection);
  $("loadSchemaBtn").addEventListener("click", loadSchema);

  $("excelFile").addEventListener("change", () => {
    setStatus($("mappingStatus"), "");
    $("excelPreview").textContent = "";
    $("mappingGrid").innerHTML = "";
    excelState = { sheetNames: [], sheetName: "", headerRow: 1, headers: [], sampleRows: [] };
  });

  $("previewExcelBtn").addEventListener("click", previewExcel);

  $("dbTable").addEventListener("change", () => {
    writeCache(storageKeys.selectedTable, $("dbTable").value);
    if (excelState.headers?.length) renderMappingGrid();
  });

  $("autoMapBtn").addEventListener("click", () => {
    autoMap();
    setStatus($("mappingStatus"), "Auto-mapeamento aplicado.");
  });

  $("saveMappingBtn").addEventListener("click", () => {
    const mappingByTable = getMappingCache();
    const key = getCurrentMappingKey();
    mappingByTable[key] = readMappingFromUI();
    writeCache(storageKeys.mappingByTable, mappingByTable);
    setStatus($("mappingStatus"), "Mapeamento salvo.");
  });

  $("importBtn").addEventListener("click", doImport);

  $("clearAllBtn").addEventListener("click", () => {
    clearCache();
    setStatus($("connStatus"), "");
    setStatus($("testConnStatus"), "");
    setStatus($("schemaStatus"), "");
    setStatus($("mappingStatus"), "");
    setStatus($("importStatus"), "");
    $("importResult").textContent = "";
    $("schemaPreview").textContent = "";
    $("excelPreview").textContent = "";
    $("mappingGrid").innerHTML = "";
    schemaState = null;
    excelState = { sheetNames: [], sheetName: "", headerRow: 1, headers: [], sampleRows: [] };
    setConnectionToForm({});
    setSchemaToForm("public");
    fillTablesSelect();
    fillSheetsSelect();
  });

  $("excelSheet").addEventListener("change", () => {
    writeCache(storageKeys.excel, { sheetName: $("excelSheet").value, headerRow: Number($("excelHeaderRow").value || 1) });
  });

  $("excelHeaderRow").addEventListener("change", () => {
    writeCache(storageKeys.excel, { sheetName: $("excelSheet").value, headerRow: Number($("excelHeaderRow").value || 1) });
  });
}

function initFromCache() {
  const conn = readCache(storageKeys.connection, null);
  if (conn) setConnectionToForm(conn);

  const schema = readCache(storageKeys.schema, "public");
  setSchemaToForm(schema);

  const excel = readCache(storageKeys.excel, { sheetName: "", headerRow: 1 });
  $("excelHeaderRow").value = String(excel.headerRow || 1);
}

function main() {
  setTabs();
  initFromCache();
  bindEvents();
  fillTablesSelect();
  fillSheetsSelect();
}

main();

