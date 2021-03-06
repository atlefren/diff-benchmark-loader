const azure = require("azure-storage");
const { createResultsTable, getPool, insertResult } = require("./src/database");
require("dotenv").config();

const blacklist = ["RowKey", ".metadata", "Timestamp"];

const getVal = meh => meh["_"];

const toJson = data =>
  Object.keys(data)
    .filter(k => !blacklist.includes(k))
    .reduce((acc, key) => ({ ...acc, [key]: getVal(data[key]) }), {});

const queryEntitiesAsync = async (
  tableService,
  table,
  query,
  continuationToken
) =>
  new Promise((resolve, reject) => {
    tableService.queryEntities(
      table,
      query,
      continuationToken,
      (error, data) => {
        if (error) {
          reject(err);
        } else {
          resolve(data);
        }
      }
    );
  });

async function* iterQuery(tableService, table, query, parse) {
  console.log("start");
  let first = true;
  let continuationToken = null;
  while (continuationToken !== null || first === true) {
    const res = await queryEntitiesAsync(
      tableService,
      table,
      query,
      continuationToken
    );

    for (const e of res.entries) {
      yield new Result(parse ? parse(e) : e);
    }

    continuationToken = res.continuationToken;
    first = false;
  }
  console.log("!");
}

const getTable = (differ, geomType) => `results.${differ}_${geomType}`;

class Result {
  constructor(result) {
    this.setResult(result);
  }

  setResult(result) {
    this.result = result;
  }

  getResult() {
    return this.result;
  }

  free() {
    delete this.result;
  }
}

const main = async () => {
  const tableService = azure.createTableService(process.env["QUEUE_CONN_STR"]);
  const pool = getPool();
  const table = "benchmarkresults4";
  const query = new azure.TableQuery().top(500);
  const differs = ["text", "geojson", "binary", "geom"];
  const geomTypes = ["point2", "linestring", "polygon"];

  for (const differ of differs) {
    for (const geomType of geomTypes) {
      await createResultsTable(pool, getTable(differ, geomType));
    }
  }

  for await (let res of iterQuery(tableService, table, query, toJson)) {
    const e = res.getResult();

    if (differs.includes(e.Differ) && geomTypes.includes(e.GeometryType)) {
      await insertResult(
        pool,
        getTable(e.Differ, e.GeometryType),
        e.GeometryId,
        e
      );
    }
    res.free();
  }
};

main();
