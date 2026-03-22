const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const express = require("express");
const multer = require("multer");
const ExcelJS = require("exceljs");
const AdmZip = require("adm-zip");
const { Client } = require("pg");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(express.json({ limit: "2mb" }));
app.use(express.static(path.join(__dirname, "..", "public")));

function nowIso() {
  return new Date().toISOString();
}

function logInfo(event, payload) {
  try {
    console.log(JSON.stringify({ level: "info", ts: nowIso(), event, ...payload }));
  } catch {
    console.log(`[info] ${event}`);
  }
}

function logError(event, payload) {
  try {
    console.error(JSON.stringify({ level: "error", ts: nowIso(), event, ...payload }));
  } catch {
    console.error(`[error] ${event}`);
  }
}

function requestId() {
  return `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;
}

app.use((req, res, next) => {
  const rid = requestId();
  req._rid = rid;
  const start = Date.now();
  logInfo("http.request", { rid, method: req.method, path: req.path });
  res.on("finish", () => {
    logInfo("http.response", { rid, method: req.method, path: req.path, status: res.statusCode, ms: Date.now() - start });
  });
  next();
});

app.post("/api/client-log", express.json({ limit: "256kb" }), (req, res) => {
  const body = req.body && typeof req.body === "object" ? req.body : {};
  const level = String(body.level ?? "info").toLowerCase();
  const event = String(body.event ?? "client.log");
  const payload = body.payload && typeof body.payload === "object" ? body.payload : { message: String(body.payload ?? "") };
  const line = { level, ts: nowIso(), event, client: true, payload };

  if (level === "error") console.error(JSON.stringify(line));
  else console.log(JSON.stringify(line));

  res.json({ ok: true });
});

function getEnvValue(name) {
  const raw = String(process.env[name] ?? "").trim();
  if (!raw) return "";

  const unquoted =
    (raw.startsWith("`") && raw.endsWith("`")) || (raw.startsWith('"') && raw.endsWith('"')) || (raw.startsWith("'") && raw.endsWith("'"))
      ? raw.slice(1, -1).trim()
      : raw;

  return unquoted;
}

function getEnvNumber(name) {
  const raw = getEnvValue(name);
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

function getEnvBoolean(name) {
  const raw = getEnvValue(name).toLowerCase();
  if (!raw) return null;
  if (raw === "true" || raw === "1" || raw === "yes") return true;
  if (raw === "false" || raw === "0" || raw === "no") return false;
  return null;
}

function normalizeConnection(connection) {
  const incoming = connection && typeof connection === "object" ? connection : {};

  const envHost = getEnvValue("DB_HOST");
  const envUser = getEnvValue("DB_USER");
  const envDatabase = getEnvValue("DB_NAME") || getEnvValue("DB_DATABASE");
  const envPassword = getEnvValue("DB_PASSWORD");
  const envPort = getEnvNumber("DB_PORT");
  const envSsl = getEnvBoolean("DB_SSL");

  const host = String(incoming.host ?? "").trim() || envHost;
  const user = String(incoming.user ?? "").trim() || envUser;
  const database = String(incoming.database ?? "").trim() || envDatabase;
  const password = String(incoming.password ?? "") || envPassword;

  const portRaw = incoming.port ?? envPort ?? 5432;
  const port = Number(portRaw);

  const ssl = typeof incoming.ssl === "boolean" ? incoming.ssl : Boolean(envSsl);

  if (!host || !user || !database || !Number.isFinite(port)) return null;

  return { host, user, database, password, port, ssl };
}

function pgConfigFromConnection(connection) {
  const normalized = normalizeConnection(connection);
  if (!normalized) return null;

  const config = {
    host: normalized.host,
    user: normalized.user,
    database: normalized.database,
    password: normalized.password,
    port: normalized.port,
  };

  if (normalized.ssl) {
    config.ssl = { rejectUnauthorized: false };
  }

  return config;
}

function quoteIdent(identifier) {
  return `"${String(identifier).replaceAll('"', '""')}"`;
}

async function withPgClient(connection, fn) {
  const config = pgConfigFromConnection(connection);
  if (!config) {
    const error = new Error("Configuração de conexão inválida.");
    error.statusCode = 400;
    throw error;
  }

  const client = new Client(config);
  try {
    await client.connect();
    return await fn(client);
  } finally {
    await client.end().catch(() => undefined);
  }
}

async function parseWorkbookFromBuffer(buffer) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.load(buffer);
  return workbook;
}

function normalizeCellValue(value) {
  if (value == null) return null;
  if (value instanceof Date) return value;
  if (typeof value === "number") return value;
  if (typeof value === "boolean") return value;
  if (typeof value === "string") return value;

  if (typeof value === "object") {
    if ("result" in value) return normalizeCellValue(value.result);
    if ("text" in value) return normalizeCellValue(value.text);
    if ("richText" in value && Array.isArray(value.richText)) {
      return value.richText.map((p) => p.text ?? "").join("");
    }
    if ("hyperlink" in value && "text" in value) return normalizeCellValue(value.text);
  }

  return String(value);
}

function getWorksheet(workbook, sheetName) {
  const byName = sheetName ? workbook.getWorksheet(sheetName) : null;
  const worksheet = byName ?? workbook.worksheets?.[0];
  if (!worksheet) return null;
  return { sheetName: worksheet.name, worksheet };
}

function getRowArray(worksheet, rowNumber, maxColumns) {
  const row = worksheet.getRow(rowNumber);
  const cols = Math.max(1, Number(maxColumns ?? Math.max(row.cellCount, worksheet.actualColumnCount ?? 0) ?? 1));
  const values = [];
  for (let c = 1; c <= cols; c++) {
    values.push(normalizeCellValue(row.getCell(c).value));
  }
  return values;
}

function toKey(input) {
  return String(input ?? "")
    .trim()
    .toLowerCase()
    .replaceAll(/\s+/g, "")
    .replaceAll(/[_-]+/g, "");
}

function detectFileType(file) {
  const name = String(file?.originalname ?? "").toLowerCase().trim();
  const mime = String(file?.mimetype ?? "").toLowerCase();
  if (name.endsWith(".xls")) return "xls";
  if (name.endsWith(".zip") || mime.includes("application/zip")) return "zip";
  if (
    name.endsWith(".csv") ||
    name.endsWith(".txt") ||
    name.endsWith(".tsv") ||
    mime.includes("text/csv") ||
    mime.includes("application/csv") ||
    mime.includes("vnd.ms-excel") ||
    mime.includes("text/plain")
  ) {
    return "csv";
  }
  return "excel";
}

function stripUtf8Bom(text) {
  if (text.charCodeAt(0) === 0xfeff) return text.slice(1);
  return text;
}

function normalizeDelimiter(delimiter) {
  const raw = String(delimiter ?? "").trim();
  if (!raw) return ",";
  if (raw === "\\t" || raw.toLowerCase() === "tab") return "\t";
  return raw[0];
}

function isZipBuffer(buffer) {
  if (!buffer || buffer.length < 4) return false;
  return buffer[0] === 0x50 && buffer[1] === 0x4b;
}

function looksLikeTextBuffer(buffer) {
  if (!buffer || !buffer.length) return false;
  const sample = buffer.subarray(0, Math.min(buffer.length, 4096));
  let suspicious = 0;
  for (const b of sample) {
    if (b === 0) return false;
    if (b < 9 || (b > 13 && b < 32)) suspicious += 1;
  }
  if (suspicious / sample.length > 0.05) return false;
  const text = sample.toString("utf8");
  return text.includes("\n") || text.includes("\r");
}

function detectDelimiterFromText(text) {
  const normalized = stripUtf8Bom(String(text ?? "")).replaceAll("\r\n", "\n").replaceAll("\r", "\n");
  const lines = normalized.split("\n").slice(0, 10).filter((l) => l.trim() !== "");
  const candidates = [",", ";", "\t", "|"];
  function countSep(line, sep) {
    let inQuotes = false;
    let count = 0;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        const next = line[i + 1];
        if (inQuotes && next === '"') {
          i += 1;
        } else {
          inQuotes = !inQuotes;
        }
        continue;
      }
      if (!inQuotes && ch === sep) count += 1;
    }
    return count;
  }
  let best = ",";
  let bestScore = -1;
  for (const sep of candidates) {
    const counts = lines.map((l) => countSep(l, sep));
    const sum = counts.reduce((a, b) => a + b, 0);
    const nonZero = counts.filter((c) => c > 0).length;
    const score = sum + nonZero * 2;
    if (score > bestScore) {
      bestScore = score;
      best = sep;
    }
  }
  return best;
}

function extractFirstTabularFromZip(buffer) {
  const zip = new AdmZip(buffer);
  const entries = zip.getEntries() || [];
  const candidates = entries
    .filter((e) => !e.isDirectory)
    .map((e) => ({ name: String(e.entryName || ""), size: Number(e.header?.size ?? 0), entry: e }))
    .filter((e) => {
      const n = e.name.toLowerCase();
      return n.endsWith(".csv") || n.endsWith(".txt") || n.endsWith(".tsv");
    })
    .sort((a, b) => (b.size || 0) - (a.size || 0));

  if (!candidates.length) return null;
  const chosen = candidates[0];
  const extracted = chosen.entry.getData();
  return { buffer: extracted, entryName: chosen.name };
}

function parseCsv(text, delimiter) {
  const sep = normalizeDelimiter(delimiter);
  const normalized = stripUtf8Bom(String(text ?? "")).replaceAll("\r\n", "\n").replaceAll("\r", "\n");

  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < normalized.length; i++) {
    const ch = normalized[i];

    if (inQuotes) {
      if (ch === '"') {
        const next = normalized[i + 1];
        if (next === '"') {
          field += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += ch;
      }
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
      continue;
    }

    if (ch === sep) {
      row.push(field);
      field = "";
      continue;
    }

    if (ch === "\n") {
      row.push(field);
      field = "";
      rows.push(row);
      row = [];
      continue;
    }

    field += ch;
  }

  row.push(field);
  rows.push(row);

  while (rows.length && rows[rows.length - 1].every((v) => String(v ?? "").trim() === "")) rows.pop();

  return rows;
}

function buildColumnsFromHeader(headerRow) {
  const cols = Array.isArray(headerRow) ? headerRow : [];
  return cols.map((name, idx) => {
    const trimmed = String(name ?? "").trim();
    const index = idx + 1;
    return {
      index,
      name: trimmed,
      displayName: trimmed ? `${trimmed} (col ${index})` : `Coluna ${index}`,
    };
  });
}

app.post("/api/test-connection", async (req, res) => {
  try {
    const connection = req.body?.connection;
    const defaultSchema = String(req.body?.schema ?? "public").trim() || "public";
    const userTableInput = String(req.body?.userTable ?? "").trim();
    logInfo("db.testConnection.start", { rid: req._rid, defaultSchema, userTableInput: userTableInput ? true : false });
    const payload = await withPgClient(connection, async (client) => {
      const infoRes = await client.query(
        `
          SELECT
            current_database() as database,
            current_user as "user",
            inet_server_addr() as server_addr,
            inet_server_port() as server_port,
            version() as version,
            now() as now
        `,
      );

      let users = [];
      try {
        const usersRes = await client.query(
          `
            SELECT rolname
            FROM pg_catalog.pg_roles
            WHERE rolcanlogin = true
            ORDER BY rolname
            LIMIT 50
          `,
        );
        users = usersRes.rows.map((r) => r.rolname);
      } catch {
        users = [];
      }

      let usersTable = null;
      if (userTableInput) {
        const tableRef = parseTableRef(defaultSchema, userTableInput);
        if (!tableRef) {
          usersTable = { ok: false, error: "Tabela de usuários inválida." };
        } else {
          try {
            const colsRes = await client.query(
              `
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = $1
                  AND table_name = $2
                ORDER BY ordinal_position
              `,
              [tableRef.schema, tableRef.table],
            );

            const allColumns = colsRes.rows.map((r) => String(r.column_name));
            const sensitiveKeys = new Set([
              "senha",
              "password",
              "pass",
              "passwd",
              "hash",
              "salt",
              "token",
              "secret",
              "apikey",
              "api_key",
              "refresh",
              "refreshtoken",
              "accesstoken",
            ].map((k) => toKey(k)));

            const safeColumns = allColumns.filter((c) => !sensitiveKeys.has(toKey(c)));
            const preferred = ["chave", "id", "nome", "login", "email", "cpf_cnpj", "tipocad", "ativo", "datahoracad"];
            const preferredKeys = preferred.map((p) => toKey(p));

            const selected = [];
            for (const k of preferredKeys) {
              const hit = safeColumns.find((c) => toKey(c) === k);
              if (hit && !selected.includes(hit)) selected.push(hit);
            }
            for (const c of safeColumns) {
              if (selected.length >= 8) break;
              if (!selected.includes(c)) selected.push(c);
            }

            if (!selected.length) {
              usersTable = {
                ok: false,
                table: `${tableRef.schema}.${tableRef.table}`,
                error: "Nenhuma coluna segura encontrada para listar.",
              };
            } else {
              const targetSql = `${quoteIdent(tableRef.schema)}.${quoteIdent(tableRef.table)}`;
              const colsSql = selected.map(quoteIdent).join(", ");
              const orderSql = selected[0] ? ` ORDER BY ${quoteIdent(selected[0])} ASC NULLS LAST` : "";
              const rowsRes = await client.query(`SELECT ${colsSql} FROM ${targetSql}${orderSql} LIMIT 50`);
              usersTable = {
                ok: true,
                table: `${tableRef.schema}.${tableRef.table}`,
                columns: selected,
                rows: rowsRes.rows ?? [],
              };
            }
          } catch (e) {
            usersTable = {
              ok: false,
              table: `${tableRef.schema}.${tableRef.table}`,
              error: e?.message ?? "Erro ao buscar usuários na tabela informada.",
            };
          }
        }
      }

      return { info: infoRes.rows?.[0] ?? null, users, usersTable };
    });

    logInfo("db.testConnection.done", { rid: req._rid, hasUsersTable: Boolean(payload?.usersTable?.ok) });
    res.json({ ok: true, ...payload });
  } catch (err) {
    logError("db.testConnection.error", { rid: req._rid, message: err?.message ?? String(err) });
    res.status(err.statusCode ?? 500).json({ ok: false, error: err.message ?? "Erro ao testar conexão." });
  }
});

app.post("/api/schema", async (req, res) => {
  try {
    const connection = req.body?.connection;
    const schema = String(req.body?.schema ?? "public").trim() || "public";
    logInfo("db.schema.start", { rid: req._rid, schema });

    const result = await withPgClient(connection, async (client) => {
      const tablesRes = await client.query(
        `
          SELECT table_name
          FROM information_schema.tables
          WHERE table_schema = $1
            AND table_type = 'BASE TABLE'
          ORDER BY table_name
        `,
        [schema],
      );

      const columnsRes = await client.query(
        `
          SELECT table_name, column_name, data_type, is_nullable
          FROM information_schema.columns
          WHERE table_schema = $1
          ORDER BY table_name, ordinal_position
        `,
        [schema],
      );

      const byTable = new Map();
      for (const row of tablesRes.rows) {
        byTable.set(row.table_name, []);
      }
      for (const row of columnsRes.rows) {
        if (!byTable.has(row.table_name)) continue;
        byTable.get(row.table_name).push({
          name: row.column_name,
          dataType: row.data_type,
          nullable: row.is_nullable === "YES",
        });
      }

      return Array.from(byTable.entries()).map(([table, columns]) => ({ table, columns }));
    });

    logInfo("db.schema.done", { rid: req._rid, schema, tables: result.length });
    res.json({ ok: true, schema, tables: result });
  } catch (err) {
    logError("db.schema.error", { rid: req._rid, message: err?.message ?? String(err) });
    res.status(err.statusCode ?? 500).json({ ok: false, error: err.message ?? "Erro ao buscar schema." });
  }
});

async function filePreviewHandler(req, res) {
  try {
    const file = req.file;
    if (!file) return res.status(400).json({ ok: false, error: "Arquivo não enviado." });

    const fileTypeByName = detectFileType(file);
    const bufferZip = isZipBuffer(file.buffer);
    const bufferText = looksLikeTextBuffer(file.buffer);
    let fileType = fileTypeByName;
    if (bufferText) fileType = "csv";
    else if (bufferZip && fileTypeByName !== "xls") fileType = "zip";
    logInfo("file.preview.start", {
      rid: req._rid,
      originalname: file.originalname,
      mimetype: file.mimetype,
      size: file.size,
      fileTypeByName,
      fileType,
      bufferZip,
      bufferText,
      sheetName: String(req.body?.sheetName ?? ""),
      headerRow: String(req.body?.headerRow ?? ""),
      delimiter: String(req.body?.delimiter ?? ""),
    });

    if (fileType === "xls") {
      return res.status(400).json({
        ok: false,
        error: "Formato .xls não suportado. Salve como .xlsx ou exporte para .csv/.txt.",
      });
    }

    if (fileType === "csv") {
      const providedDelimiter = String(req.body?.delimiter ?? "").trim();
      const text = file.buffer.toString("utf8");
      const delimiter = providedDelimiter ? providedDelimiter : detectDelimiterFromText(text);
      const rows = parseCsv(text, delimiter);
      if (!rows.length) return res.status(400).json({ ok: false, error: "CSV vazio." });

      const headerRow = 1;
      const header = rows[0] ?? [];
      const columns = buildColumnsFromHeader(header);
      const sampleRows = rows.slice(1, 11);
      logInfo("file.preview.csv.parsed", {
        rid: req._rid,
        rows: rows.length,
        cols: header.length,
        delimiterUsed: normalizeDelimiter(delimiter) === "\t" ? "\\t" : normalizeDelimiter(delimiter),
      });

      res.json({
        ok: true,
        fileType,
        delimiter,
        source: { type: "raw" },
        sheetNames: [],
        sheetName: "",
        headerRow,
        columns,
        sampleRows,
      });
      return;
    }

    if (bufferZip) {
      let extracted = null;
      try {
        extracted = extractFirstTabularFromZip(file.buffer);
      } catch (e) {
        logError("file.preview.zip.extract.error", { rid: req._rid, message: e?.message ?? String(e) });
        extracted = null;
      }

      if (extracted?.buffer) {
        const text = extracted.buffer.toString("utf8");
        const providedDelimiter = String(req.body?.delimiter ?? "").trim();
        const delimiter = providedDelimiter ? providedDelimiter : detectDelimiterFromText(text);
        const rows = parseCsv(text, delimiter);
        if (!rows.length) return res.status(400).json({ ok: false, error: "CSV vazio." });

        const headerRow = 1;
        const header = rows[0] ?? [];
        const columns = buildColumnsFromHeader(header);
        const sampleRows = rows.slice(1, 11);
        logInfo("file.preview.zip.csv.parsed", {
          rid: req._rid,
          entryName: extracted.entryName,
          rows: rows.length,
          cols: header.length,
          delimiterUsed: normalizeDelimiter(delimiter) === "\t" ? "\\t" : normalizeDelimiter(delimiter),
        });

        res.json({
          ok: true,
          fileType: "csv",
          delimiter,
          source: { type: "zip", entryName: extracted.entryName },
          sheetNames: [],
          sheetName: "",
          headerRow,
          columns,
          sampleRows,
        });
        return;
      }
    }

    let workbook = null;
    try {
      workbook = await parseWorkbookFromBuffer(file.buffer);
    } catch (e) {
      logError("file.preview.excel.open.error", { rid: req._rid, message: e?.message ?? String(e) });
      workbook = null;
    }

    const sheetNames = workbook ? (workbook.worksheets ?? []).map((ws) => ws.name) : [];

    if (!sheetNames.length) return res.status(400).json({ ok: false, error: "Arquivo Excel sem abas." });

    const sheetInfo = getWorksheet(workbook, req.body?.sheetName);
    if (!sheetInfo) return res.status(400).json({ ok: false, error: "Aba inválida." });

    const headerRow = Math.max(1, Number(req.body?.headerRow ?? 1));
    const headerValues = getRowArray(sheetInfo.worksheet, headerRow);
    const headers = headerValues.map((h) => String(h ?? "").trim());
    const columns = buildColumnsFromHeader(headers);

    const sampleRows = [];
    for (let r = headerRow + 1; r <= Math.min(headerRow + 10, sheetInfo.worksheet.rowCount); r++) {
      sampleRows.push(getRowArray(sheetInfo.worksheet, r, headers.length));
    }

    logInfo("file.preview.excel.parsed", {
      rid: req._rid,
      sheetName: sheetInfo.sheetName,
      sheetNamesCount: sheetNames.length,
      headerRow,
      cols: headers.length,
      rowsTotal: sheetInfo.worksheet.rowCount,
    });
    res.json({ ok: true, fileType: "excel", source: { type: bufferZip ? "zip" : "raw" }, sheetNames, sheetName: sheetInfo.sheetName, headerRow, columns, sampleRows });
  } catch (err) {
    logError("file.preview.error", { rid: req._rid, message: err?.message ?? String(err) });
    res.status(500).json({ ok: false, error: err.message ?? "Erro ao ler Excel." });
  }
}

app.post("/api/file/preview", upload.single("file"), filePreviewHandler);

app.post("/api/excel/preview", upload.single("file"), filePreviewHandler);

function normalizeInputValue(value) {
  if (value == null) return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed === "" ? null : trimmed;
  }
  return value;
}

function parseTableRef(defaultSchema, tableRef) {
  const raw = String(tableRef ?? "").trim();
  if (!raw) return null;
  const parts = raw.split(".");
  if (parts.length === 2 && parts[0] && parts[1]) return { schema: parts[0], table: parts[1] };
  return { schema: defaultSchema, table: raw };
}

async function insertRowsBatched(client, targetSql, dbColumns, rowsOfValues) {
  if (!rowsOfValues.length) return 0;
  const quotedColumnsSql = dbColumns.map(quoteIdent).join(", ");

  const values = [];
  const placeholders = [];
  let paramIndex = 1;

  for (const rowValues of rowsOfValues) {
    values.push(...rowValues);
    const oneRow = rowValues.map(() => `$${paramIndex++}`).join(", ");
    placeholders.push(`(${oneRow})`);
  }

  const sql = `INSERT INTO ${targetSql} (${quotedColumnsSql}) VALUES ${placeholders.join(", ")}`;
  const r = await client.query(sql, values);
  return r.rowCount ?? rowsOfValues.length;
}

app.post("/api/import-multi", upload.single("file"), async (req, res) => {
  try {
    const file = req.file;
    if (!file) return res.status(400).json({ ok: false, error: "Arquivo não enviado." });

    const connection = JSON.parse(String(req.body?.connection ?? "null"));
    const defaultSchema = String(req.body?.schema ?? "public").trim() || "public";
    const sheetName = String(req.body?.sheetName ?? "").trim();
    const headerRow = Math.max(1, Number(req.body?.headerRow ?? 1));
    const delimiter = String(req.body?.delimiter ?? ",");
    const mappingEntries = JSON.parse(String(req.body?.mappingEntries ?? "null"));
    let delimiterEffective = delimiter;

    if (!Array.isArray(mappingEntries) || !mappingEntries.length) {
      return res.status(400).json({ ok: false, error: "Mapeamento inválido." });
    }

    const fileTypeByName = detectFileType(file);
    if (fileTypeByName === "xls") return res.status(400).json({ ok: false, error: "Formato .xls não suportado." });

    const bufferZip = isZipBuffer(file.buffer);
    const bufferText = looksLikeTextBuffer(file.buffer);

    let inputKind = bufferText ? "csv" : bufferZip ? "zip" : fileTypeByName === "csv" ? "csv" : "excel";
    let csvBuffer = null;
    let csvSource = null;
    let excelWorkbook = null;

    if (inputKind === "csv") {
      csvBuffer = file.buffer;
      csvSource = { type: "raw" };
    } else if (inputKind === "zip") {
      let extracted = null;
      try {
        extracted = extractFirstTabularFromZip(file.buffer);
      } catch (e) {
        logError("file.importMulti.zip.extract.error", { rid: req._rid, message: e?.message ?? String(e) });
        extracted = null;
      }
      if (extracted?.buffer) {
        csvBuffer = extracted.buffer;
        csvSource = { type: "zip", entryName: extracted.entryName };
        inputKind = "csv";
      } else {
        try {
          excelWorkbook = await parseWorkbookFromBuffer(file.buffer);
        } catch (e) {
          logError("file.importMulti.excel.open.error", { rid: req._rid, message: e?.message ?? String(e) });
          excelWorkbook = null;
        }
        inputKind = "excel";
      }
    }

    logInfo("file.importMulti.start", {
      rid: req._rid,
      originalname: file.originalname,
      mimetype: file.mimetype,
      size: file.size,
      fileTypeByName,
      bufferZip,
      bufferText,
      inputKind,
      sheetName,
      headerRow,
      delimiter,
      mappingEntriesCount: mappingEntries.length,
    });

    const groups = new Map();
    for (const entry of mappingEntries) {
      const sourceIndex = Number(entry?.sourceIndex);
      const column = String(entry?.column ?? "").trim();
      const tableRef = parseTableRef(defaultSchema, entry?.table);
      if (!Number.isFinite(sourceIndex) || sourceIndex <= 0) continue;
      if (!tableRef || !column) continue;

      const key = `${tableRef.schema}.${tableRef.table}`;
      const existing = groups.get(key) ?? { schema: tableRef.schema, table: tableRef.table, dbColumns: [], sourceIndices: [] };
      if (!existing.dbColumns.includes(column)) {
        existing.dbColumns.push(column);
        existing.sourceIndices.push(sourceIndex);
      }
      groups.set(key, existing);
    }

    if (!groups.size) return res.status(400).json({ ok: false, error: "Nenhuma coluna mapeada." });

    const maxCol = Math.max(...Array.from(groups.values()).flatMap((g) => g.sourceIndices));

    const result = await withPgClient(connection, async (client) => {
      await client.query("BEGIN");
      try {
        const groupStates = new Map();
        for (const [key, g] of groups.entries()) {
          const colCount = g.dbColumns.length;
          const maxParams = 60000;
          const batchSize = Math.max(1, Math.min(500, Math.floor(maxParams / colCount)));

          groupStates.set(key, {
            schema: g.schema,
            table: g.table,
            dbColumns: g.dbColumns,
            sourceIndices: g.sourceIndices,
            batchSize,
            buffer: [],
            inserted: 0,
            rowsRead: 0,
          });
        }

        async function flush(state) {
          if (!state.buffer.length) return;
          const targetSql = `${quoteIdent(state.schema)}.${quoteIdent(state.table)}`;
          state.inserted += await insertRowsBatched(client, targetSql, state.dbColumns, state.buffer);
          state.buffer.length = 0;
        }

        if (inputKind === "csv") {
          const text = (csvBuffer ?? file.buffer).toString("utf8");
          const providedDelimiter = String(delimiter ?? "").trim();
          const delimiterUsed = providedDelimiter ? providedDelimiter : detectDelimiterFromText(text);
          delimiterEffective = delimiterUsed;
          const rows = parseCsv(text, delimiterUsed);
          if (!rows.length) throw new Error("CSV vazio.");
          logInfo("file.importMulti.csv.parsed", {
            rid: req._rid,
            rows: rows.length,
            cols: Array.isArray(rows[0]) ? rows[0].length : 0,
            delimiterUsed: normalizeDelimiter(delimiterUsed) === "\t" ? "\\t" : normalizeDelimiter(delimiterUsed),
            source: csvSource ?? { type: "raw" },
          });

          for (let i = 1; i < rows.length; i++) {
            const row = rows[i] ?? [];
            for (const state of groupStates.values()) {
              const rowValues = state.sourceIndices.map((idx) => normalizeInputValue(row[idx - 1]));
              const nonEmpty = rowValues.some((v) => (v == null ? "" : String(v).trim()) !== "");
              if (!nonEmpty) continue;
              state.rowsRead += 1;
              state.buffer.push(rowValues);
              if (state.buffer.length >= state.batchSize) await flush(state);
            }
          }
        } else {
          const workbook = excelWorkbook ?? (await parseWorkbookFromBuffer(file.buffer));
          const sheetInfo = getWorksheet(workbook, sheetName);
          if (!sheetInfo) throw new Error("Aba inválida.");
          logInfo("file.importMulti.excel.opened", {
            rid: req._rid,
            sheetName: sheetInfo.sheetName,
            rowsTotal: sheetInfo.worksheet.rowCount,
          });

          for (let r = headerRow + 1; r <= sheetInfo.worksheet.rowCount; r++) {
            const rowObj = sheetInfo.worksheet.getRow(r);
            if (!rowObj.hasValues) continue;

            const row = getRowArray(sheetInfo.worksheet, r, maxCol);
            for (const state of groupStates.values()) {
              const rowValues = state.sourceIndices.map((idx) => normalizeInputValue(row[idx - 1]));
              const nonEmpty = rowValues.some((v) => (v == null ? "" : String(v).trim()) !== "");
              if (!nonEmpty) continue;
              state.rowsRead += 1;
              state.buffer.push(rowValues);
              if (state.buffer.length >= state.batchSize) await flush(state);
            }
          }
        }

        for (const state of groupStates.values()) await flush(state);

        await client.query("COMMIT");

        return Array.from(groupStates.values()).map((s) => ({
          table: `${s.schema}.${s.table}`,
          inserted: s.inserted,
          rowsRead: s.rowsRead,
          mappedColumns: s.dbColumns,
        }));
      } catch (e) {
        await client.query("ROLLBACK");
        throw e;
      }
    });

    logInfo("file.importMulti.done", {
      rid: req._rid,
      result: Array.isArray(result)
        ? result.map((r) => ({ table: r.table, inserted: r.inserted, rowsRead: r.rowsRead, mappedColumnsCount: r.mappedColumns?.length ?? 0 }))
        : [],
    });
    res.json({ ok: true, fileType: inputKind === "excel" ? "excel" : "csv", sheetName: sheetName || "", delimiter: delimiterEffective, result });
  } catch (err) {
    logError("file.importMulti.error", { rid: req._rid, message: err?.message ?? String(err) });
    let message = err.message ?? "Erro ao importar.";
    if (typeof message === "string" && message.toLowerCase().includes("password")) message = "Erro ao importar.";
    res.status(500).json({ ok: false, error: message });
  }
});

app.post("/api/import", upload.single("file"), async (req, res) => {
  try {
    const file = req.file;
    if (!file) return res.status(400).json({ ok: false, error: "Arquivo não enviado." });

    const connection = JSON.parse(String(req.body?.connection ?? "null"));
    const schema = String(req.body?.schema ?? "public").trim() || "public";
    const table = String(req.body?.table ?? "").trim();
    const sheetName = String(req.body?.sheetName ?? "").trim();
    const headerRow = Math.max(1, Number(req.body?.headerRow ?? 1));
    const mapping = JSON.parse(String(req.body?.mapping ?? "null"));

    if (!table) return res.status(400).json({ ok: false, error: "Tabela não informada." });
    if (!mapping || typeof mapping !== "object") return res.status(400).json({ ok: false, error: "Mapeamento inválido." });

    const workbook = await parseWorkbookFromBuffer(file.buffer);
    const sheetInfo = getWorksheet(workbook, sheetName);
    if (!sheetInfo) return res.status(400).json({ ok: false, error: "Aba inválida." });

    const headers = getRowArray(sheetInfo.worksheet, headerRow).map((h) => String(h ?? "").trim());
    if (!headers.length) return res.status(400).json({ ok: false, error: "Cabeçalho não encontrado." });

    const headerIndex = new Map();
    headers.forEach((h, idx) => {
      const key = toKey(h);
      if (key && !headerIndex.has(key)) headerIndex.set(key, idx);
    });

    const columnPairs = [];
    for (const [excelColumnRaw, dbColumnRaw] of Object.entries(mapping)) {
      const excelKey = toKey(excelColumnRaw);
      const dbColumn = String(dbColumnRaw ?? "").trim();
      if (!excelKey || !dbColumn) continue;
      const idx = headerIndex.get(excelKey);
      if (idx === undefined) continue;
      columnPairs.push({ excelKey, dbColumn, excelColIndex: idx + 1 });
    }

    if (!columnPairs.length) return res.status(400).json({ ok: false, error: "Nenhuma coluna mapeada." });

    const dbColumns = columnPairs.map((p) => p.dbColumn);
    const quotedColumnsSql = dbColumns.map(quoteIdent).join(", ");
    const targetSql = `${quoteIdent(schema)}.${quoteIdent(table)}`;

    let totalRowsRead = 0;

    const inserted = await withPgClient(connection, async (client) => {
      await client.query("BEGIN");
      try {
        const colCount = dbColumns.length;
        const maxParams = 60000;
        const batchSize = Math.max(1, Math.min(500, Math.floor(maxParams / colCount)));

        const buffer = [];
        let insertedCount = 0;

        async function flush() {
          if (!buffer.length) return;
          const values = [];
          const placeholders = [];
          let paramIndex = 1;

          for (const rowValues of buffer) {
            values.push(...rowValues);
            const oneRow = rowValues.map(() => `$${paramIndex++}`).join(", ");
            placeholders.push(`(${oneRow})`);
          }

          const sql = `INSERT INTO ${targetSql} (${quotedColumnsSql}) VALUES ${placeholders.join(", ")}`;
          const r = await client.query(sql, values);
          insertedCount += r.rowCount ?? buffer.length;
          buffer.length = 0;
        }

        for (let r = headerRow + 1; r <= sheetInfo.worksheet.rowCount; r++) {
          const row = sheetInfo.worksheet.getRow(r);
          if (!row.hasValues) continue;

          const rowValues = columnPairs.map((p) => normalizeCellValue(row.getCell(p.excelColIndex).value));
          const nonEmpty = rowValues.some((v) => (v == null ? "" : String(v).trim()) !== "");
          if (!nonEmpty) continue;

          totalRowsRead += 1;
          buffer.push(rowValues);
          if (buffer.length >= batchSize) await flush();
        }

        await flush();
        await client.query("COMMIT");
        return insertedCount;
      } catch (e) {
        await client.query("ROLLBACK");
        throw e;
      }
    });

    res.json({
      ok: true,
      inserted,
      table: `${schema}.${table}`,
      sheetName: sheetInfo.sheetName,
      mappedColumns: dbColumns,
      totalRowsRead,
    });
  } catch (err) {
    let message = err.message ?? "Erro ao importar.";
    if (typeof message === "string" && message.toLowerCase().includes("password")) {
      message = "Erro ao importar.";
    }
    res.status(500).json({ ok: false, error: message });
  }
});

function getBasePort() {
  const raw = process.env.PORT ?? "";
  const parsed = Number(raw);
  if (Number.isFinite(parsed) && parsed > 0) return parsed;
  return 3000;
}

function startServer(port, triesLeft) {
  const server = app.listen(port, () => {
    process.stdout.write(`Servidor web em http://localhost:${port}/\n`);
  });

  server.on("error", (err) => {
    if (err && err.code === "EADDRINUSE" && triesLeft > 0) {
      startServer(port + 1, triesLeft - 1);
      return;
    }
    process.stderr.write(`${err?.message ?? err}\n`);
    process.exitCode = 1;
  });
}

startServer(getBasePort(), 20);
