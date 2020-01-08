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

function getSaveResult(pool, tableName) {
  return function saveResult(id, result) {
    return pool.connect().then(client => {
      const query = {
        text: `INSERT INTO ${tableName} (id, textDiffer, jsonDiffer, binaryDiffer, geomDiffer) VALUES($1, $2, $3, $4, $5)`,
        values: [id, result.TextDiffer, result.JsonDiffer, result.BinaryDiffer, result.GeomDiffer]
      };

      return client
        .query(query)
        .then(res => client.release())
        .catch(err => client.release());
    });
  };
}

module.exports = {readVersions, createResultsTable, getSaveResult};
