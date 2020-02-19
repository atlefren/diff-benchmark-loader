const QueryStream = require("pg-query-stream");
const stream = require("async-iter-stream");
const { Pool } = require("pg");
require("dotenv").config();

async function* streamVersions(pool, tableName, numGeoms) {
  const client = await pool.connect();
  const query = new QueryStream(
    numGeoms !== null
      ? `SELECT id, initial_version, last_version FROM ${tableName} LIMIT ${numGeoms}`
      : `SELECT id, initial_version, last_version FROM ${tableName}`
  );
  const pgstream = client.query(query);

  //release the client when the stream is finished
  pgstream.on("end", () => client.release(true));

  for await (let x of stream.wrap(pgstream)) {
    yield x;
  }
}

async function* streamRemainingVersions(pool, versionsTable, resultsTable) {
  const client = await pool.connect();
  const query = new QueryStream(`
  SELECT id, initial_version, last_version
  FROM ${versionsTable} l 
  WHERE NOT EXISTS (
     SELECT  
     FROM ${resultsTable}
     WHERE l.id = id
  );`);
  const pgstream = client.query(query);

  //release the client when the stream is finished
  pgstream.on("end", () => client.release(true));

  for await (let x of stream.wrap(pgstream)) {
    yield x;
  }
}

async function readVersions(pool, table, offset, batchSize) {
  const client = await pool.connect();
  try {
    const res = await client.query({
      text: `SELECT * FROM ${table} LIMIT ${batchSize} OFFSET ${offset} `
    });

    if (res.rowCount === 0) {
      return null;
    }

    client.end();
    return res.rows;
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release(true);
  }
}

async function createResultsTable(pool, tableName) {
  const client = await pool.connect();
  try {
    const query = `CREATE TABLE IF NOT EXISTS ${tableName} (
      id bigint PRIMARY KEY,
      data jsonb
    )`;
    await client.query(query);
    //await client.query(`TRUNCATE TABLE ${tableName}`);
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release(true);
  }
}

async function insertResult(pool, tableName, id, data) {
  const client = await pool.connect();
  try {
    const query = `INSERT INTO ${tableName} (id, data) VALUES ($1, $2)
    ON CONFLICT (id) DO NOTHING;
    `;
    await client.query(query, [id, data]);
  } catch (e) {
    await client.query("ROLLBACK");
    console.log(e);
    throw e;
  } finally {
    client.release(true);
  }
}

async function* getRows(pool, versionsTable, batchSize, numGeoms) {
  let offset = 0;

  const shouldContinue = () => (numGeoms != null ? offset < numGeoms : true);

  while (shouldContinue()) {
    const res = await readVersions(pool, versionsTable, offset, batchSize);
    if (!res) {
      break;
    }
    for (let row of res) {
      yield row;
    }
    offset += batchSize;
  }
}

const getPool = () => {
  const POSTGRES_CONNECTION_STRING = process.env["CONN_STR"];
  return new Pool({ connectionString: POSTGRES_CONNECTION_STRING });
};

module.exports = {
  readVersions,
  createResultsTable,
  getRows,
  streamVersions,
  streamRemainingVersions,
  getPool,
  insertResult
};
