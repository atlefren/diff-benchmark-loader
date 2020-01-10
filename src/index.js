const {Pool} = require('pg');
const {getRows, createResultsTable, streamVersions} = require('./database');
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

async function populateQueue(versionsTable, geomTable, resultsTable, config = {}) {
  const POSTGRES_CONNECTION_STRING = process.env['CONN_STR'];
  const STORAGE_CONNECTION_STRING = process.env.QUEUE_CONN_STR || '';
  const {numGeoms = null} = config;

  const pool = new Pool({connectionString: POSTGRES_CONNECTION_STRING});

  const queueServiceClient = QueueServiceClient.fromConnectionString(STORAGE_CONNECTION_STRING);

  const queueName = `diffqueue`;
  const queueClient = queueServiceClient.getQueueClient(queueName);

  console.log(`Creating ${resultsTable}`);
  await createResultsTable(pool, resultsTable);

  console.log(`Populating queue with ${numGeoms ? numGeoms : 'all'} rows from ${versionsTable}`);
  let c = 0;
  for await (const version of streamVersions(pool, versionsTable, numGeoms)) {
    queueClient.sendMessage(encode({...version, geomTable, resultsTable})).catch(e => console.error(e));
    c++;
    if (c % 1000 === 0) {
      console.log(`Sent ${c}`);
    }
  }
}

module.exports = {populateQueue};
