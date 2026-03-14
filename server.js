const express = require("express");
const multer = require("multer");
const ExcelJS = require("exceljs");
const { Client } = require("pg");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(express.json({ limit: "2mb" }));
app.use(express.static("public"));

function normalizeConnection(connection) {
  if (!connection || typeof connection !== "object") return null;

  const host = String(connection.host ?? "").trim();
  const user = String(connection.user ?? "").trim();
  const database = String(connection.database ?? "").trim();
  const password = String(connection.password ?? "");

  const portRaw = connection.port ?? 5432;
  const port = Number(portRaw);

  const ssl = Boolean(connection.ssl);

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

app.post("/api/test-connection", async (req, res) => {
  try {
    const connection = req.body?.connection;
    await withPgClient(connection, async (client) => {
      await client.query("SELECT 1 as ok");
    });
    res.json({ ok: true });
  } catch (err) {
    res.status(err.statusCode ?? 500).json({ ok: false, error: err.message ?? "Erro ao testar conexão." });
  }
});

app.post("/api/schema", async (req, res) => {
  try {
    const connection = req.body?.connection;
    const schema = String(req.body?.schema ?? "public").trim() || "public";

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

    res.json({ ok: true, schema, tables: result });
  } catch (err) {
    res.status(err.statusCode ?? 500).json({ ok: false, error: err.message ?? "Erro ao buscar schema." });
  }
});

app.post("/api/excel/preview", upload.single("file"), async (req, res) => {
  try {
    const file = req.file;
    if (!file) return res.status(400).json({ ok: false, error: "Arquivo não enviado." });

    const workbook = await parseWorkbookFromBuffer(file.buffer);
    const sheetNames = (workbook.worksheets ?? []).map((ws) => ws.name);
    if (!sheetNames.length) return res.status(400).json({ ok: false, error: "Arquivo Excel sem abas." });

    const sheetInfo = getWorksheet(workbook, req.body?.sheetName);
    if (!sheetInfo) return res.status(400).json({ ok: false, error: "Aba inválida." });

    const headerRow = Math.max(1, Number(req.body?.headerRow ?? 1));
    const headerValues = getRowArray(sheetInfo.worksheet, headerRow);
    const headers = headerValues.map((h) => String(h ?? "").trim());

    const sampleRows = [];
    for (let r = headerRow + 1; r <= Math.min(headerRow + 10, sheetInfo.worksheet.rowCount); r++) {
      sampleRows.push(getRowArray(sheetInfo.worksheet, r, headers.length));
    }

    res.json({ ok: true, sheetNames, sheetName: sheetInfo.sheetName, headerRow, headers, sampleRows });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message ?? "Erro ao ler Excel." });
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
