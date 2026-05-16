/**
 * create-tables.mjs
 * Reads ALL table schemas from SOURCE account and recreates them
 * exactly (keys, GSIs, LSIs, billing) in DESTINATION account.
 *
 * RUN:
 *   node scripts/create-tables.mjs
 */

import {
  DynamoDBClient,
  ListTablesCommand,
  DescribeTableCommand,
  CreateTableCommand,
} from "@aws-sdk/client-dynamodb";

// ─── CREDENTIALS ─────────────────────────────────────────────────────────────

const client_source = new DynamoDBClient({
  region: "ap-south-1",
  credentials: {
    accessKeyId: "XXXXXXXXXXXXXXXXXXX",
    secretAccessKey: "XXXXXXXXXXXXXXXX",
  },
});

const client_destination = new DynamoDBClient({
  region: "ap-south-1",
  credentials: {
    accessKeyId: "XXXXXXXXXXXXXXXXX",
    secretAccessKey: "XXXXXXXXXXXXX",
  },
});

// ─── STEP 1: List all tables in source ───────────────────────────────────────

async function listAllSourceTables() {
  const tableNames = [];
  let lastEvaluatedTableName = undefined;

  do {
    const res = await client_source.send(
      new ListTablesCommand({ ExclusiveStartTableName: lastEvaluatedTableName })
    );
    tableNames.push(...(res.TableNames || []));
    lastEvaluatedTableName = res.LastEvaluatedTableName;
  } while (lastEvaluatedTableName);

  return tableNames;
}

// ─── STEP 2: Describe each table from source ─────────────────────────────────

async function describeSourceTable(tableName) {
  const res = await client_source.send(
    new DescribeTableCommand({ TableName: tableName })
  );
  return res.Table;
}

// ─── STEP 3: Build CreateTable params from real schema ───────────────────────

function buildCreateParams(table) {
  const billing =
    table.BillingModeSummary?.BillingMode === "PAY_PER_REQUEST"
      ? "PAY_PER_REQUEST"
      : "PROVISIONED";

  const params = {
    TableName: table.TableName,
    AttributeDefinitions: table.AttributeDefinitions,
    KeySchema: table.KeySchema,
    BillingMode: billing,
  };

  // Throughput only for PROVISIONED
  if (billing === "PROVISIONED") {
    params.ProvisionedThroughput = {
      ReadCapacityUnits: table.ProvisionedThroughput.ReadCapacityUnits,
      WriteCapacityUnits: table.ProvisionedThroughput.WriteCapacityUnits,
    };
  }

  // GSIs
  if (table.GlobalSecondaryIndexes?.length) {
    params.GlobalSecondaryIndexes = table.GlobalSecondaryIndexes.map((gsi) => {
      const g = {
        IndexName: gsi.IndexName,
        KeySchema: gsi.KeySchema,
        Projection: gsi.Projection,
      };
      if (billing === "PROVISIONED") {
        g.ProvisionedThroughput = {
          ReadCapacityUnits: gsi.ProvisionedThroughput.ReadCapacityUnits,
          WriteCapacityUnits: gsi.ProvisionedThroughput.WriteCapacityUnits,
        };
      }
      return g;
    });
  }

  // LSIs
  if (table.LocalSecondaryIndexes?.length) {
    params.LocalSecondaryIndexes = table.LocalSecondaryIndexes.map((lsi) => ({
      IndexName: lsi.IndexName,
      KeySchema: lsi.KeySchema,
      Projection: lsi.Projection,
    }));
  }

  // Stream
  if (table.StreamSpecification?.StreamEnabled) {
    params.StreamSpecification = table.StreamSpecification;
  }

  // SSE
  if (table.SSEDescription?.Status === "ENABLED") {
    params.SSESpecification = { Enabled: true };
  }

  return params;
}

// ─── STEP 4: Check if table exists in destination ────────────────────────────

async function existsInDest(tableName) {
  try {
    await client_destination.send(
      new DescribeTableCommand({ TableName: tableName })
    );
    return true;
  } catch (e) {
    if (e.name === "ResourceNotFoundException") return false;
    throw e;
  }
}

// ─── STEP 5: Create table in destination ─────────────────────────────────────

async function createInDest(params) {
  await client_destination.send(new CreateTableCommand(params));
}

// ─── MAIN ────────────────────────────────────────────────────────────────────

console.log("\n========================================");
console.log("  DynamoDB Table Migration");
console.log("  Source  → Destination (schema only)");
console.log("========================================\n");

const tableNames = await listAllSourceTables();
console.log(`Found ${tableNames.length} table(s) in source account.\n`);

let created = 0, skipped = 0, failed = 0;

for (const name of tableNames) {
  try {
    // Check destination
    if (await existsInDest(name)) {
      console.log(`[SKIP]   ${name}`);
      skipped++;
      continue;
    }

    // Fetch real schema from source
    const table = await describeSourceTable(name);

    // Build exact create params
    const params = buildCreateParams(table);

    // Log GSI count
    const gsiCount = params.GlobalSecondaryIndexes?.length ?? 0;
    const lsiCount = params.LocalSecondaryIndexes?.length ?? 0;

    // Create in destination
    await createInDest(params);
    console.log(`[OK]     ${name}  (GSIs: ${gsiCount}, LSIs: ${lsiCount})`);
    created++;

    // Small delay to avoid throttling
    await new Promise((r) => setTimeout(r, 300));

  } catch (e) {
    console.error(`[ERROR]  ${name} — ${e.message}`);
    failed++;
  }
}

console.log("\n========================================");
console.log(`  Created : ${created}`);
console.log(`  Skipped : ${skipped}`);
console.log(`  Failed  : ${failed}`);
console.log("========================================\n");
