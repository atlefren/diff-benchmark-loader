const {Pool} = require('pg');
const Table = require('cli-table');
require('dotenv').config();

async function getStats(pool, table) {
  const client = await pool.connect();
  try {
    const res = await client.query({
      text: `SELECT 
      ROUND(avg((textdiffer->>'CreateTime')::DECIMAL)::DECIMAL,4) textCreate,
      ROUND(avg((textdiffer->>'ApplyTime')::DECIMAL)::DECIMAL,4) textApply,
      ROUND(avg((textdiffer->>'UndoTime')::DECIMAL)::DECIMAL,4) textUndo,
      ROUND(avg((textdiffer->>'PatchSize')::DECIMAL)::DECIMAL,0) textSize,
      ROUND(100 * SUM(CASE WHEN (textdiffer->>'ForwardCorrect')::boolean = true THEN 1 ELSE 0 END)::DECIMAL /count(textdiffer)::DECIMAL, 2) textForwardCorrect,
      ROUND(100 * SUM(CASE WHEN (textdiffer->>'UndoCorrect')::boolean = true THEN 1 ELSE 0 END)::DECIMAL /count(textdiffer)::DECIMAL, 2) textUndoCorrect,

      ROUND(avg((jsondiffer->>'CreateTime')::DECIMAL)::DECIMAL,4) jsonCreate,
      ROUND(avg((jsondiffer->>'ApplyTime')::DECIMAL)::DECIMAL,4) jsonApply,
      ROUND(avg((jsondiffer->>'UndoTime')::DECIMAL)::DECIMAL,4) jsonUndo,
      ROUND(avg((jsondiffer->>'PatchSize')::DECIMAL)::DECIMAL,0) jsonSize,
      ROUND(100 * SUM(CASE WHEN (jsondiffer->>'ForwardCorrect')::boolean = true THEN 1 ELSE 0 END)::DECIMAL /count(jsondiffer)::DECIMAL, 2) jsonForwardCorrect,
      ROUND(100 * SUM(CASE WHEN (jsondiffer->>'UndoCorrect')::boolean = true THEN 1 ELSE 0 END)::DECIMAL /count(jsondiffer)::DECIMAL, 2) jsonUndoCorrect,


      ROUND(avg((binarydiffer->>'CreateTime')::DECIMAL)::DECIMAL,4) binaryCreate,
      ROUND(avg((binarydiffer->>'ApplyTime')::DECIMAL)::DECIMAL,4) binaryApply,
      ROUND(avg((binarydiffer->>'UndoTime')::DECIMAL)::DECIMAL,4) binaryUndo,
      ROUND(avg((binarydiffer->>'PatchSize')::DECIMAL)::DECIMAL,0) binarySize,
      ROUND(100 * SUM(CASE WHEN (binarydiffer->>'ForwardCorrect')::boolean = true THEN 1 ELSE 0 END)::DECIMAL /count(binarydiffer)::DECIMAL, 2) binaryForwardCorrect,
      ROUND(100 * SUM(CASE WHEN (binarydiffer->>'UndoCorrect')::boolean = true THEN 1 ELSE 0 END)::DECIMAL /count(binarydiffer)::DECIMAL, 2) binaryUndoCorrect,

      ROUND(avg((geomdiffer->>'CreateTime')::DECIMAL)::DECIMAL,4) geomCreate,
      ROUND(avg((geomdiffer->>'ApplyTime')::DECIMAL)::DECIMAL,4) geomApply,
      ROUND(avg((geomdiffer->>'UndoTime')::DECIMAL)::DECIMAL,4) geomUndo,
      ROUND(avg((geomdiffer->>'PatchSize')::DECIMAL)::DECIMAL,0) geomSize,
      ROUND(100 * SUM(CASE WHEN (geomdiffer->>'ForwardCorrect')::boolean = true THEN 1 ELSE 0 END)::DECIMAL /count(geomdiffer)::DECIMAL, 2) geomForwardCorrect,
      ROUND(100 * SUM(CASE WHEN (geomdiffer->>'UndoCorrect')::boolean = true THEN 1 ELSE 0 END)::DECIMAL /count(geomdiffer)::DECIMAL, 2) geomUndoCorrect,
      count(1) as count

	FROM ${table};`
    });

    client.end();
    return res.rows[0];
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release(true);
  }
}

const print = (data, tableName) => {
  const head = [
    'Alogrithm',
    'Create Time (ms)',
    'Apply Time (ms)',
    'Undo  Time (ms)',
    'Size (bytes)',
    'ApplyOk (%)',
    'UndoOk (%)'
  ];
  const types = ['text', 'json', 'binary', 'geom'];
  const measures = ['create', 'apply', 'undo', 'size', 'forwardcorrect', 'undocorrect'];

  const rows = types.reduce(
    (acc, type, i) => {
      acc[i] = [type, ...measures.map(m => data[`${type}${m}`])];
      return acc;
    },
    [[], [], [], []]
  );

  var table = new Table({
    head,
    colAligns: ['left', 'right', 'right', 'right', 'right', 'right', 'right'],
    chars: {mid: '', 'left-mid': '', 'mid-mid': '', 'right-mid': ''}
  });
  for (let row of rows) {
    table.push(row);
  }

  console.log(`Stats for ${tableName}`);
  console.log(`Num samples ${data.count}`);
  console.log(table.toString());
};

async function printStats(tableName) {
  const connectionString = process.env['CONN_STR'];

  const pool = new Pool({connectionString});

  const res = await getStats(pool, tableName);
  print(res, tableName);
}

if (require.main === module) {
  async function a() {
    await printStats('osm_test.point_results');
    await printStats('osm_test.line_results');
    await printStats('osm_test.polygon_results');
  }
  a();
}
module.exports = {printStats};
