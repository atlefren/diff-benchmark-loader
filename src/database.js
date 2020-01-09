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
    await client.query('ROLLBACK');
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
      textDiffer jsonb,
      jsonDiffer jsonb,
      binaryDiffer jsonb,
      geomDiffer jsonb
    )`;
    await client.query(query);
    await client.query(`TRUNCATE TABLE ${tableName}`);
  } catch (e) {
    await client.query('ROLLBACK');
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

module.exports = {readVersions, createResultsTable, getRows};
