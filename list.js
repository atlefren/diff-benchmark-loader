const { BlobServiceClient } = require("@azure/storage-blob");
const { createResultsTable, getPool, insertResult } = require("./src/database");
const result = require("dotenv").config();

const STORAGE_CONNECTION_STRING = result.parsed["QUEUE_CONN_STR"] || "";

const getType = name => name.split("_")[0];
const getId = name => name.split("_")[1];

const inc = (groups, t) => {
  if (!(t in groups)) {
    groups[t] = 0;
  }
  groups[t] += 1;
};

async function streamToString(readableStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readableStream.on("data", data => {
      chunks.push(data.toString());
    });
    readableStream.on("end", () => {
      resolve(chunks.join(""));
    });
    readableStream.on("error", reject);
  });
}

async function getData(containerClient, id) {
  const blockBlobClient = containerClient.getBlockBlobClient(id);
  const downloadBlockBlobResponse = await blockBlobClient.download(0);
  const data = await streamToString(
    downloadBlockBlobResponse.readableStreamBody
  );
  return JSON.parse(data);
}

async function process(differ, pool) {
  const geometryTypes = ["point", "linestring", "polygon"];

  for (let geometryType of geometryTypes) {
    const tableName = `benchmark_results.${geometryType}_${differ}`;
    await createResultsTable(pool, tableName);
  }

  const blobServiceClient = await BlobServiceClient.fromConnectionString(
    STORAGE_CONNECTION_STRING
  );

  const containerName = `benchmarkstorage${differ}`;
  const containerClient = await blobServiceClient.getContainerClient(
    containerName
  );

  console.log(differ);

  const groups = {};

  for await (const blob of containerClient.listBlobsFlat()) {
    const data = await getData(containerClient, blob.name);
    const geometryType = getType(blob.name);
    const id = getId(blob.name);
    const tableName = `benchmark_results.${geometryType}_${differ}`;
    insertResult(pool, tableName, id, data);
    inc(groups, getType(blob.name));
  }
  console.log(groups);
}

async function main() {
  const pool = getPool();

  await process("text", pool);

  await process("geojson", pool);
  await process("binary", pool);
  await process("geom", pool);

  /*
  const geometryType = "point";
  const differ = "text";
  const tableName = `benchmark_results.${geometryType}_${differ}`;
  await createResultsTable(pool, tableName);
  insertResult(pool, tableName, 1, { test: 1 });
  */
}

main()
  .then(() => console.log("Done"))
  .catch(ex => console.log(ex.message));
