const storageKeys = {
  connection: "importador.pg.connection",
  schema: "importador.pg.schema",
  selectedTable: "importador.pg.table",
  mappingByTable: "importador.pg.mappingByTable",
  excel: "importador.excel",
  useServerEnv: "importador.pg.useServerEnv",
  usersTableName: "importador.pg.usersTableName",
};

function $(id) {
  return document.getElementById(id);
}

function setStatus(el, text) {
  el.textContent = text || "";
}

function logClient(event, payload) {
  const el = $("clientLogs");
  if (!el) return;
  const line = JSON.stringify(
    { ts: new Date().toISOString(), event, ...(payload && typeof payload === "object" ? payload : { message: String(payload ?? "") }) },
    null,
    0,
  );
  const next = `${el.textContent}${el.textContent ? "\n" : ""}${line}`;
  const lines = next.split("\n");
  el.textContent = lines.slice(Math.max(0, lines.length - 200)).join("\n");
}

let clientLogQueue = [];
let clientLogFlushTimer = null;

async function flushClientLogs() {
  const batch = clientLogQueue.slice(0, 50);
  if (!batch.length) return;
  clientLogQueue = clientLogQueue.slice(batch.length);

  try {
    await fetch("/api/client-log", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ level: "info", event: "client.batch", payload: { logs: batch } }),
    });
  } catch {}
}

function enqueueTerminalLog(event, payload, level = "info") {
  clientLogQueue.push({ ts: new Date().toISOString(), level, event, payload });
  if (clientLogQueue.length > 200) clientLogQueue = clientLogQueue.slice(clientLogQueue.length - 200);
  if (!clientLogFlushTimer) {
    clientLogFlushTimer = setTimeout(async () => {
      clientLogFlushTimer = null;
      await flushClientLogs();
    }, 300);
  }
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
  if ($("useServerEnv")?.checked) return null;
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

function setConnectionInputsDisabled(disabled) {
  $("pgHost").disabled = disabled;
  $("pgPort").disabled = disabled;
  $("pgUser").disabled = disabled;
  $("pgPassword").disabled = disabled;
  $("pgDatabase").disabled = disabled;
  $("pgSsl").disabled = disabled;
}

function setUseServerEnv(value) {
  $("useServerEnv").checked = Boolean(value);
  setConnectionInputsDisabled(Boolean(value));
  writeCache(storageKeys.useServerEnv, Boolean(value));
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
  logClient("http.request", { url, type: "json" });
  enqueueTerminalLog("http.request", { url, type: "json" }, "info");
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const text = await res.text().catch(() => "");
  const data = text ? safeJsonParse(text, null) : null;
  logClient("http.response", { url, status: res.status, ok: res.ok, bytes: text.length });
  enqueueTerminalLog("http.response", { url, status: res.status, ok: res.ok, bytes: text.length }, res.ok ? "info" : "error");
  return { ok: res.ok, status: res.status, data, text };
}

async function apiForm(url, formData) {
  logClient("http.request", { url, type: "form" });
  enqueueTerminalLog("http.request", { url, type: "form" }, "info");
  const res = await fetch(url, { method: "POST", body: formData });
  const text = await res.text().catch(() => "");
  const data = text ? safeJsonParse(text, null) : null;
  logClient("http.response", { url, status: res.status, ok: res.ok, bytes: text.length });
  enqueueTerminalLog("http.response", { url, status: res.status, ok: res.ok, bytes: text.length }, res.ok ? "info" : "error");
  return { ok: res.ok, status: res.status, data, text };
}

let schemaState = null;
let fileState = {
  fileType: "",
  delimiter: ",",
  sheetNames: [],
  sheetName: "",
  headerRow: 1,
  columns: [],
  sampleRows: [],
};

function detectClientFileType(file) {
  const name = String(file?.name ?? "").toLowerCase();
  if (name.endsWith(".csv") || name.endsWith(".txt") || name.endsWith(".tsv")) return "csv";
  return "excel";
}

function setFileControlsForType(fileType) {
  const isCsv = fileType === "csv";
  $("csvDelimiter").disabled = !isCsv;
  $("excelSheet").disabled = isCsv;
  $("excelHeaderRow").disabled = isCsv;
  if (isCsv) $("excelHeaderRow").value = "1";
}

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

  for (const s of fileState.sheetNames || []) {
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
  const fileType = fileState.fileType || "";
  const delimiter = fileState.fileType === "csv" ? String(fileState.delimiter || ",") : "";
  const columnsSig = (fileState.columns || [])
    .map((c) => `${c.index}:${normalizeKey(c.name) || ""}`)
    .join("|");
  return `${fileType}::${delimiter}::${columnsSig}`;
}

function loadCachedMappingIntoUI() {
  const mappingByTable = getMappingCache();
  const key = getCurrentMappingKey();
  const cached = mappingByTable[key];
  if (!cached) return;

  for (const [sourceIndexRaw, value] of Object.entries(cached)) {
    const sourceIndex = String(sourceIndexRaw);
    const tableSelect = document.querySelector(`[data-source-index="${sourceIndex}"][data-role="table"]`);
    const colSelect = document.querySelector(`[data-source-index="${sourceIndex}"][data-role="column"]`);
    if (!tableSelect || !colSelect) continue;
    tableSelect.value = value?.table ?? "";
    fillDbColumnsSelect(colSelect, tableSelect.value);
    colSelect.value = value?.column ?? "";
  }
}

function readMappingFromUI() {
  const tableSelects = Array.from(document.querySelectorAll('[data-role="table"][data-source-index]'));
  const entries = [];
  for (const t of tableSelects) {
    const sourceIndex = Number(t.getAttribute("data-source-index"));
    const table = t.value;
    const colSelect = document.querySelector(`[data-role="column"][data-source-index="${String(sourceIndex)}"]`);
    const column = colSelect?.value ?? "";
    if (Number.isFinite(sourceIndex) && sourceIndex > 0 && table && column) entries.push({ sourceIndex, table, column });
  }
  return entries;
}

function renderExcelPreview() {
  $("excelPreview").textContent = JSON.stringify(
    {
      fileType: fileState.fileType,
      delimiter: fileState.delimiter,
      sheetName: fileState.sheetName,
      headerRow: fileState.headerRow,
      columns: fileState.columns,
      sampleRows: fileState.sampleRows,
    },
    null,
    2,
  );
}

function fillDbColumnsSelect(select, tableName) {
  select.innerHTML = "";
  const empty = document.createElement("option");
  empty.value = "";
  empty.textContent = "Selecione...";
  select.appendChild(empty);

  if (!tableName) return;
  const t = schemaState?.tables?.find((x) => x.table === tableName);
  const columns = t?.columns?.map((c) => c.name) ?? [];
  for (const col of columns) {
    const opt = document.createElement("option");
    opt.value = col;
    opt.textContent = col;
    select.appendChild(opt);
  }
}

function renderMappingGrid() {
  const grid = $("mappingGrid");
  grid.innerHTML = "";

  const defaultTable = $("dbTable").value;

  for (const col of fileState.columns || []) {
    const row = document.createElement("div");
    row.className = "mapping__row";

    const idx = document.createElement("div");
    idx.className = "pill pill--small";
    idx.textContent = `#${col.index}`;

    const left = document.createElement("div");
    left.className = "pill";
    left.textContent = col.displayName || col.name || `Coluna ${col.index}`;

    const tableSelect = document.createElement("select");
    tableSelect.className = "select";
    tableSelect.setAttribute("data-role", "table");
    tableSelect.setAttribute("data-source-index", String(col.index));

    const tableEmpty = document.createElement("option");
    tableEmpty.value = "";
    tableEmpty.textContent = "Não importar";
    tableSelect.appendChild(tableEmpty);

    for (const t of schemaState?.tables ?? []) {
      const opt = document.createElement("option");
      opt.value = t.table;
      opt.textContent = t.table;
      tableSelect.appendChild(opt);
    }

    const columnSelect = document.createElement("select");
    columnSelect.className = "select";
    columnSelect.setAttribute("data-role", "column");
    columnSelect.setAttribute("data-source-index", String(col.index));
    fillDbColumnsSelect(columnSelect, defaultTable);

    tableSelect.value = defaultTable || "";
    tableSelect.addEventListener("change", () => {
      fillDbColumnsSelect(columnSelect, tableSelect.value);
      columnSelect.value = "";
    });

    row.appendChild(idx);
    row.appendChild(left);
    row.appendChild(tableSelect);
    row.appendChild(columnSelect);
    grid.appendChild(row);
  }

  loadCachedMappingIntoUI();
}

function autoMap() {
  const table = $("dbTable").value;
  if (!table) return;

  const dbCols = (schemaState?.tables?.find((t) => t.table === table)?.columns ?? []).map((c) => c.name);
  const dbByKey = new Map();
  for (const c of dbCols) {
    const k = normalizeKey(c);
    if (k && !dbByKey.has(k)) dbByKey.set(k, c);
  }

  for (const col of fileState.columns || []) {
    const match = dbByKey.get(normalizeKey(col.name));
    if (!match) continue;
    const tableSelect = document.querySelector(`[data-role="table"][data-source-index="${String(col.index)}"]`);
    const colSelect = document.querySelector(`[data-role="column"][data-source-index="${String(col.index)}"]`);
    if (!tableSelect || !colSelect) continue;
    tableSelect.value = table;
    fillDbColumnsSelect(colSelect, table);
    colSelect.value = match;
  }
}

async function previewExcel() {
  const file = $("excelFile").files?.[0];
  if (!file) return;

  const sheetName = $("excelSheet").value;
  const headerRow = Number($("excelHeaderRow").value || 1);
  const delimiter = $("csvDelimiter").value || ",";
  const fileType = detectClientFileType(file);
  setFileControlsForType(fileType);
  logClient("preview.start", { name: file.name, size: file.size, type: file.type, fileType, sheetName, headerRow, delimiter });
  enqueueTerminalLog("preview.start", { name: file.name, size: file.size, type: file.type, fileType, sheetName, headerRow, delimiter }, "info");

  const fd = new FormData();
  fd.append("file", file);
  if (sheetName) fd.append("sheetName", sheetName);
  fd.append("headerRow", String(headerRow));
  fd.append("delimiter", delimiter);

  setStatus($("mappingStatus"), "Lendo arquivo...");
  let res = await apiForm("/api/file/preview", fd);
  if ((!res.ok || !res.data?.ok) && res.status === 404) {
    res = await apiForm("/api/excel/preview", fd);
  }
  if (!res.ok || !res.data?.ok) {
    const serverMsg =
      typeof res.data?.error === "string" && res.data.error.trim()
        ? res.data.error.trim()
        : (res.text || "").trim()
          ? String(res.text).trim()
          : "Erro ao ler arquivo.";
    const extra = res.status === 404 ? " (API não encontrada; reinicie o servidor)" : "";
    logClient("preview.error", { status: res.status, message: serverMsg });
    enqueueTerminalLog("preview.error", { status: res.status, message: serverMsg }, "error");
    setStatus($("mappingStatus"), `${serverMsg}${extra}`);
    return;
  }

  fileState = res.data;
  $("excelHeaderRow").value = String(fileState.headerRow || 1);
  fillSheetsSelect();
  $("excelSheet").value = fileState.sheetName || "";
  $("csvDelimiter").value = fileState.delimiter || delimiter;
  setFileControlsForType(fileState.fileType);
  logClient("preview.ok", { fileType: fileState.fileType, columns: fileState.columns?.length ?? 0, sheetName: fileState.sheetName || "" });
  enqueueTerminalLog("preview.ok", { fileType: fileState.fileType, columns: fileState.columns?.length ?? 0, sheetName: fileState.sheetName || "" }, "info");

  writeCache(storageKeys.excel, {
    sheetName: fileState.sheetName,
    headerRow: fileState.headerRow,
    delimiter: $("csvDelimiter").value || ",",
  });

  renderExcelPreview();
  renderMappingGrid();
  setStatus($("mappingStatus"), `Colunas lidas: ${fileState.columns?.length || 0}`);
}

async function testConnection() {
  setStatus($("testConnStatus"), "Testando...");
  const res = await apiJson("/api/test-connection", {
    connection: getConnectionFromForm(),
    schema: getSchemaFromForm(),
    userTable: $("usersTableName")?.value?.trim() || "",
  });
  if (!res.ok || !res.data?.ok) {
    setStatus($("testConnStatus"), res.data?.error || "Falha.");
    $("connPreview").textContent = JSON.stringify(res.data, null, 2);
    return;
  }
  const info = res.data?.info ?? null;
  const users = res.data?.users ?? [];
  const usersTable = res.data?.usersTable ?? null;
  $("connPreview").textContent = JSON.stringify({ info, users, usersTable }, null, 2);
  setStatus($("testConnStatus"), info?.database ? `OK (${info.database})` : "OK");
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
  if (fileState.columns?.length) renderMappingGrid();
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
  if (!fileState.columns?.length) {
    setStatus($("importStatus"), "Leia as colunas do Excel antes de importar (aba Mapeamento).");
    return;
  }

  const mappingEntries = readMappingFromUI();
  if (!mappingEntries.length) {
    setStatus($("importStatus"), "Mapeie ao menos uma coluna (defina tabela e coluna).");
    return;
  }

  const fd = new FormData();
  fd.append("file", file);
  fd.append("connection", JSON.stringify(getConnectionFromForm()));
  fd.append("schema", getSchemaFromForm());
  fd.append("sheetName", $("excelSheet").value || fileState.sheetName || "");
  fd.append("headerRow", String(Number($("excelHeaderRow").value || fileState.headerRow || 1)));
  fd.append("delimiter", $("csvDelimiter").value || ",");
  fd.append("mappingEntries", JSON.stringify(mappingEntries));

  setStatus($("importStatus"), "Importando...");
  $("importResult").textContent = "";
  logClient("import.start", {
    name: file.name,
    size: file.size,
    type: file.type,
    schema: getSchemaFromForm(),
    sheetName: $("excelSheet").value || fileState.sheetName || "",
    headerRow: String(Number($("excelHeaderRow").value || fileState.headerRow || 1)),
    delimiter: $("csvDelimiter").value || ",",
    mappingEntriesCount: mappingEntries.length,
  });
  const res = await apiForm("/api/import-multi", fd);
  if (!res.ok || !res.data?.ok) {
    logClient("import.error", { status: res.status, message: res.data?.error || res.text || "Erro ao importar." });
    setStatus($("importStatus"), res.data?.error || "Erro ao importar.");
    $("importResult").textContent = JSON.stringify(res.data, null, 2);
    return;
  }
  const insertedTotal = Array.isArray(res.data?.result) ? res.data.result.reduce((acc, x) => acc + (Number(x.inserted) || 0), 0) : 0;
  logClient("import.ok", { insertedTotal, result: res.data?.result });
  setStatus($("importStatus"), `OK (inseridas: ${insertedTotal})`);
  $("importResult").textContent = JSON.stringify(res.data, null, 2);
}

function bindEvents() {
  $("saveConnBtn").addEventListener("click", () => {
    const conn = getConnectionFromForm();
    if (conn) writeCache(storageKeys.connection, conn);
    writeCache(storageKeys.schema, getSchemaFromForm());
    setStatus($("connStatus"), "Salvo.");
  });

  $("testConnBtn").addEventListener("click", testConnection);
  $("loadSchemaBtn").addEventListener("click", loadSchema);

  $("useServerEnv").addEventListener("change", () => {
    setUseServerEnv($("useServerEnv").checked);
  });

  $("excelFile").addEventListener("change", () => {
    setStatus($("mappingStatus"), "");
    $("excelPreview").textContent = "";
    $("mappingGrid").innerHTML = "";
    $("connPreview").textContent = "";
    fileState = { fileType: "", delimiter: ",", sheetNames: [], sheetName: "", headerRow: 1, columns: [], sampleRows: [] };
    $("csvDelimiter").value = readCache(storageKeys.excel, {}).delimiter || ",";
    setFileControlsForType(detectClientFileType($("excelFile").files?.[0]));
  });

  $("previewExcelBtn").addEventListener("click", previewExcel);

  $("dbTable").addEventListener("change", () => {
    writeCache(storageKeys.selectedTable, $("dbTable").value);
    if (fileState.columns?.length) renderMappingGrid();
  });

  $("autoMapBtn").addEventListener("click", () => {
    autoMap();
    setStatus($("mappingStatus"), "Auto-mapeamento aplicado.");
  });

  $("saveMappingBtn").addEventListener("click", () => {
    const mappingByTable = getMappingCache();
    const key = getCurrentMappingKey();
    const entries = readMappingFromUI();
    const mapped = {};
    for (const e of entries) {
      mapped[String(e.sourceIndex)] = { table: e.table, column: e.column };
    }
    mappingByTable[key] = mapped;
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
    $("connPreview").textContent = "";
    fileState = { fileType: "", delimiter: ",", sheetNames: [], sheetName: "", headerRow: 1, columns: [], sampleRows: [] };
    setConnectionToForm({});
    setSchemaToForm("public");
    setUseServerEnv(false);
    $("usersTableName").value = "public.usuariostec";
    $("csvDelimiter").value = ",";
    fillTablesSelect();
    fillSheetsSelect();
  });

  $("excelSheet").addEventListener("change", () => {
    writeCache(storageKeys.excel, {
      sheetName: $("excelSheet").value,
      headerRow: Number($("excelHeaderRow").value || 1),
      delimiter: $("csvDelimiter").value || ",",
    });
  });

  $("excelHeaderRow").addEventListener("change", () => {
    writeCache(storageKeys.excel, {
      sheetName: $("excelSheet").value,
      headerRow: Number($("excelHeaderRow").value || 1),
      delimiter: $("csvDelimiter").value || ",",
    });
  });

  $("csvDelimiter").addEventListener("change", () => {
    writeCache(storageKeys.excel, {
      sheetName: $("excelSheet").value,
      headerRow: Number($("excelHeaderRow").value || 1),
      delimiter: $("csvDelimiter").value || ",",
    });
  });

  $("usersTableName").addEventListener("change", () => {
    writeCache(storageKeys.usersTableName, $("usersTableName").value.trim());
  });
}

function initFromCache() {
  const useServerEnv = readCache(storageKeys.useServerEnv, false);
  $("useServerEnv").checked = Boolean(useServerEnv);
  setConnectionInputsDisabled(Boolean(useServerEnv));

  const conn = readCache(storageKeys.connection, null);
  if (conn) setConnectionToForm(conn);

  const schema = readCache(storageKeys.schema, "public");
  setSchemaToForm(schema);

  const excel = readCache(storageKeys.excel, { sheetName: "", headerRow: 1, delimiter: "," });
  $("excelHeaderRow").value = String(excel.headerRow || 1);
  $("csvDelimiter").value = String(excel.delimiter || ",");

  const usersTableName = readCache(storageKeys.usersTableName, "public.usuariostec");
  $("usersTableName").value = String(usersTableName || "public.usuariostec");
}

function main() {
  setTabs();
  initFromCache();
  bindEvents();
  fillTablesSelect();
  fillSheetsSelect();
  setFileControlsForType(detectClientFileType($("excelFile").files?.[0]));
}

main();
