const {
  getPool,
  streamVersions,
  streamRemainingVersions
} = require("./database");
const { QueueServiceClient } = require("@azure/storage-queue");
require("dotenv").config();

const encode = data =>
  Buffer.from(
    JSON.stringify({
      Id: data.id,
      InitialVersion: data.initial_version,
      LastVersion: data.last_version,
      GeomTable: data.geomTable,
      GeometryType: data.geometryType
    })
  ).toString("base64");

const getQueueClient = () => {
  const STORAGE_CONNECTION_STRING = process.env.QUEUE_CONN_STR || "";
  const queueServiceClient = QueueServiceClient.fromConnectionString(
    STORAGE_CONNECTION_STRING
  );

  const queueName = `geometrymessagestest`;
  return queueServiceClient.getQueueClient(queueName);
};

async function populateQueue(
  versionsTable,
  geomTable,
  geometryType,
  config = {}
) {
  const { numGeoms = null } = config;

  const pool = getPool();

  console.log(
    `Populating queue with ${
      numGeoms ? numGeoms : "all"
    } rows from ${versionsTable}`
  );
  const iterator = streamVersions(pool, versionsTable, numGeoms);

  dopopulateQueue(iterator, geomTable, geometryType);
}

async function populateRestOfQueue(versionsTable, geomTable, resultsTable) {
  const pool = getPool();
  const iterator = streamRemainingVersions(pool, versionsTable, resultsTable);
  populateQueue(iterator, geomTable, resultsTable);
}

async function dopopulateQueue(iterator, geomTable, geometryType) {
  const queueClient = getQueueClient();
  let c = 0;
  for await (const version of iterator) {
    queueClient
      .sendMessage(encode({ ...version, geomTable, geometryType }))
      .catch(e => console.error(e));
    c++;
    if (c % 100 === 0) {
      console.log(`Sent ${c}`);
    }
  }
}

module.exports = { populateQueue, populateRestOfQueue };
