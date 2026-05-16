/**
 * migrate-data.mjs
 * Scans all tables from SOURCE account and writes all items to DESTINATION account.
 * Handles empty-string GSI/LSI key values by removing those attributes from items
 * (so the item still migrates, just without the invalid index entry).
 *
 * RUN:
 *   node scripts/migrate-data.mjs
 */

import {
  DynamoDBClient,
  ListTablesCommand,
  ScanCommand,
  BatchWriteItemCommand,
  DescribeTableCommand,
} from "@aws-sdk/client-dynamodb";

// ─── CREDENTIALS ─────────────────────────────────────────────────────────────

const client_source = new DynamoDBClient({
  region: "ap-south-1",
  credentials: {
    accessKeyId: "XXXXXXXXXXXXX",
    secretAccessKey: "XXXXXXXXXXX",
  },
});

const client_destination = new DynamoDBClient({
  region: "ap-south-1",
  credentials: {
    accessKeyId: "XXXXXXXXXXX",
    secretAccessKey: "XXXXXXXXXXXX",
  },
});

// ─── CONFIG ───────────────────────────────────────────────────────────────────
const BATCH_SIZE = 25;  // DynamoDB max per BatchWriteItem
const DELAY_MS = 200; // delay between batches to avoid throttling

// ─── HELPERS ─────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ─── List all tables from source ─────────────────────────────────────────────

async function listAllTables() {
  const names = [];
  let last;
  do {
    const res = await client_source.send(
      new ListTablesCommand({ ExclusiveStartTableName: last })
    );
    names.push(...(res.TableNames || []));
    last = res.LastEvaluatedTableName;
  } while (last);
  return names;
}

// ─── Get GSI + LSI key attribute names from source table ─────────────────────
// Returns a Set of attribute names used as GSI or LSI keys.
// Items with empty string ("") for these attrs will have them removed before write.

async function getIndexKeyAttrs(tableName) {
  const res = await client_source.send(
    new DescribeTableCommand({ TableName: tableName })
  );
  const table = res.Table;
  const keyAttrs = new Set();

  // Primary keys — DynamoDB allows empty strings for primary keys (throws differently)
  // We only need to handle secondary index keys
  const collectKeys = (indexes = []) => {
    for (const idx of indexes) {
      for (const key of idx.KeySchema || []) {
        keyAttrs.add(key.AttributeName);
      }
    }
  };

  collectKeys(table.GlobalSecondaryIndexes);
  collectKeys(table.LocalSecondaryIndexes);

  return keyAttrs;
}

// ─── Check table exists in destination ───────────────────────────────────────

async function destTableExists(name) {
  try {
    await client_destination.send(new DescribeTableCommand({ TableName: name }));
    return true;
  } catch (e) {
    if (e.name === "ResourceNotFoundException") return false;
    throw e;
  }
}

// ─── Scan ALL items from source table (handles pagination) ───────────────────

async function scanAll(tableName) {
  const items = [];
  let lastKey;
  do {
    const res = await client_source.send(
      new ScanCommand({
        TableName: tableName,
        ExclusiveStartKey: lastKey,
      })
    );
    items.push(...(res.Items || []));
    lastKey = res.LastEvaluatedKey;
  } while (lastKey);
  return items;
}

// ─── Sanitize: remove empty-string values for GSI/LSI key attributes ─────────
// DynamoDB rejects empty string "" as a secondary index key value.
// Fix: remove the attribute from the item (item still written, just not indexed).

function sanitizeItems(items, indexKeyAttrs) {
  if (indexKeyAttrs.size === 0) return { clean: items, fixed: 0 };

  let fixed = 0;
  const clean = items.map((item) => {
    let changed = false;
    const sanitized = { ...item };

    for (const attr of indexKeyAttrs) {
      const val = sanitized[attr];
      // DynamoDB low-level format: { S: "" } or { N: "" } etc.
      if (val && typeof val === "object") {
        const isEmpty =
          (val.S !== undefined && val.S === "") ||
          (val.N !== undefined && val.N === "") ||
          (val.B !== undefined && val.B === "");
        if (isEmpty) {
          delete sanitized[attr];
          changed = true;
        }
      }
    }

    if (changed) fixed++;
    return sanitized;
  });

  return { clean, fixed };
}

// ─── Write items to destination in batches of 25 ─────────────────────────────

async function batchWrite(tableName, items) {
  let written = 0;
  let failed = 0;

  for (let i = 0; i < items.length; i += BATCH_SIZE) {
    const chunk = items.slice(i, i + BATCH_SIZE);

    const requestItems = {
      [tableName]: chunk.map((item) => ({
        PutRequest: { Item: item },
      })),
    };

    let unprocessed = requestItems;
    let attempts = 0;

    // Retry unprocessed items (DynamoDB may return some on throttle)
    while (Object.keys(unprocessed).length > 0 && attempts < 5) {
      const res = await client_destination.send(
        new BatchWriteItemCommand({ RequestItems: unprocessed })
      );

      const remaining = res.UnprocessedItems || {};
      const unprocessedCount = Object.values(remaining).flat().length;

      written += chunk.length - unprocessedCount;
      failed += unprocessedCount;

      unprocessed = remaining;
      attempts++;

      if (Object.keys(unprocessed).length > 0) {
        await sleep(DELAY_MS * attempts); // exponential backoff
      }
    }

    await sleep(DELAY_MS);
  }

  return { written, failed };
}

// ─── MAIN ────────────────────────────────────────────────────────────────────

console.log("\n========================================");
console.log("  DynamoDB Data Migration");
console.log("  Source → Destination");
console.log("========================================\n");

const tables = await listAllTables();
console.log(`Found ${tables.length} table(s) in source.\n`);

let totalWritten = 0;
let totalFailed = 0;
let totalFixed = 0;
let totalSkipped = 0;

for (const name of tables) {
  try {
    // Skip if dest table doesn't exist
    if (!(await destTableExists(name))) {
      console.log(`[SKIP]   ${name} — not in destination`);
      totalSkipped++;
      continue;
    }

    // Get GSI/LSI key attrs for sanitization
    const indexKeyAttrs = await getIndexKeyAttrs(name);

    // Scan source
    process.stdout.write(`[SCAN]   ${name} ...`);
    const items = await scanAll(name);
    process.stdout.write(` ${items.length} items\n`);

    if (items.length === 0) {
      console.log(`         (empty — skipping)`);
      continue;
    }

    // Sanitize: remove empty-string GSI/LSI key values
    const { clean, fixed } = sanitizeItems(items, indexKeyAttrs);
    if (fixed > 0) {
      console.log(`[FIX]    ${name} — removed empty GSI/LSI key attrs from ${fixed} item(s)`);
      totalFixed += fixed;
    }

    // Write to destination
    process.stdout.write(`[WRITE]  ${name} ...`);
    const { written, failed } = await batchWrite(name, clean);
    process.stdout.write(` ✓ ${written} written, ✗ ${failed} failed\n`);

    totalWritten += written;
    totalFailed += failed;

  } catch (e) {
    console.error(`[ERROR]  ${name} — ${e.message}`);
    totalFailed++;
  }
}

console.log("\n========================================");
console.log(`  Total Written : ${totalWritten}`);
console.log(`  Total Fixed   : ${totalFixed} (empty GSI keys removed)`);
console.log(`  Total Failed  : ${totalFailed}`);
console.log(`  Skipped       : ${totalSkipped}`);
console.log("========================================\n");
