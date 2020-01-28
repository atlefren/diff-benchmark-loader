const {Pool} = require('pg');
const {createResultsTable, streamVersions, streamRemainingVersions} = require('./database');
const {QueueServiceClient} = require('@azure/storage-queue');
require('dotenv').config();

const encode = data =>
  Buffer.from(
    JSON.stringify({
      Id: data.id,
      InitialVersion: data.initial_version,
      LastVersion: data.last_version,
      GeomTable: data.geomTable,
      ResultTable: data.resultsTable
    })
  ).toString('base64');

const getQueueClient = () => {
  const STORAGE_CONNECTION_STRING = process.env.QUEUE_CONN_STR || '';
  const queueServiceClient = QueueServiceClient.fromConnectionString(STORAGE_CONNECTION_STRING);

  const queueName = `diffqueue`;
  return queueServiceClient.getQueueClient(queueName);
};

const getPool = () => {
  const POSTGRES_CONNECTION_STRING = process.env['CONN_STR'];
  return new Pool({connectionString: POSTGRES_CONNECTION_STRING});
};

async function populateQueue(versionsTable, geomTable, resultsTable, config = {}) {
  const {numGeoms = null} = config;

  const pool = getPool();

  console.log(`Creating ${resultsTable}`);
  await createResultsTable(pool, resultsTable);

  console.log(`Populating queue with ${numGeoms ? numGeoms : 'all'} rows from ${versionsTable}`);
  const iterator = streamVersions(pool, versionsTable, numGeoms);

  populateQueue(iterator, geomTable, resultsTable);
}

async function populateRestOfQueue(versionsTable, geomTable, resultsTable) {
  const pool = getPool();
  const iterator = streamRemainingVersions(pool, versionsTable, resultsTable);
  populateQueue(iterator, geomTable, resultsTable);
}

async function populateQueue(iterator, geomTable, resultsTable) {
  const queueClient = getQueueClient();
  let c = 0;
  for await (const version of iterator) {
    queueClient.sendMessage(encode({...version, geomTable, resultsTable})).catch(e => console.error(e));
    c++;
    if (c % 100 === 0) {
      console.log(`Sent ${c}`);
    }
  }
}

module.exports = {populateQueue, populateRestOfQueue};
