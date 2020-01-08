const {Pool} = require('pg');
const Table = require('cli-table');
require('dotenv').config();

async function getStats(pool, table) {
  const client = await pool.connect();
  try {
    const res = await client.query({
      text: `SELECT 
      ROUND(avg((textdiffer->>'createTime')::DECIMAL)::DECIMAL,4) textCreate,
      ROUND(avg((textdiffer->>'applyTime')::DECIMAL)::DECIMAL,4) textApply,
      ROUND(avg((textdiffer->>'undoTime')::DECIMAL)::DECIMAL,4) textUndo,
      ROUND(avg((textdiffer->>'patchSize')::DECIMAL)::DECIMAL,0) textSize,
      bool_and((textdiffer->>'forwardCorrect')::boolean) textForwardCorrect,
      bool_and((textdiffer->>'undoCorrect')::boolean) textUndoCorrect,

      ROUND(avg((jsondiffer->>'createTime')::DECIMAL)::DECIMAL,4) jsonCreate,
      ROUND(avg((jsondiffer->>'applyTime')::DECIMAL)::DECIMAL,4) jsonApply,
      ROUND(avg((jsondiffer->>'undoTime')::DECIMAL)::DECIMAL,4) jsonUndo,
      ROUND(avg((jsondiffer->>'patchSize')::DECIMAL)::DECIMAL,0) jsonSize,
      bool_and((jsondiffer->>'forwardCorrect')::boolean) jsonForwardCorrect,
      bool_and((jsondiffer->>'undoCorrect')::boolean) jsonUndoCorrect,

      ROUND(avg((binarydiffer->>'createTime')::DECIMAL)::DECIMAL,4) binaryCreate,
      ROUND(avg((binarydiffer->>'applyTime')::DECIMAL)::DECIMAL,4) binaryApply,
      ROUND(avg((binarydiffer->>'undoTime')::DECIMAL)::DECIMAL,4) binaryUndo,
      ROUND(avg((binarydiffer->>'patchSize')::DECIMAL)::DECIMAL,0) binarySize,
      bool_and((binarydiffer->>'forwardCorrect')::boolean) binaryForwardCorrect,
      bool_and((binarydiffer->>'undoCorrect')::boolean) binaryUndoCorrect,

      ROUND(avg((geomdiffer->>'createTime')::DECIMAL)::DECIMAL,4) geomCreate,
      ROUND(avg((geomdiffer->>'applyTime')::DECIMAL)::DECIMAL,4) geomApply,
      ROUND(avg((geomdiffer->>'undoTime')::DECIMAL)::DECIMAL,4) geomUndo,
      ROUND(avg((geomdiffer->>'patchSize')::DECIMAL)::DECIMAL,0) geomSize,
      bool_and((geomdiffer->>'forwardCorrect')::boolean) geomForwardCorrect,
      bool_and((geomdiffer->>'undoCorrect')::boolean) geomUndoCorrect,
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
  const head = ['Alogrithm', 'Create', 'Apply', 'Undo', 'Size', 'ApplyOk', 'UndoOk'];
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
    colAligns: ['left', 'right', 'right', 'right', 'right', 'left', 'left'],
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
  printStats('osm_test.node_results');
}
module.exports = {printStats};
