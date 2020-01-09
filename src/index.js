const {Pool} = require('pg');
const {getRows, createResultsTable} = require('./database');
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
  const {batchSize = 10, numGeoms = null} = config;

  const pool = new Pool({connectionString: POSTGRES_CONNECTION_STRING});
  const queueServiceClient = QueueServiceClient.fromConnectionString(STORAGE_CONNECTION_STRING);

  const queueName = `diffqueue`;
  const queueClient = queueServiceClient.getQueueClient(queueName);

  await createResultsTable(pool, resultsTable);

  try {
    let c = 0;
    for await (const r of getRows(pool, versionsTable, batchSize, numGeoms)) {
      // Send a message into the queue using the sendMessage method.
      await queueClient.sendMessage(encode({...r, geomTable, resultsTable}));
      c++;
      if (c % 1000 === 0) {
        console.log(`Sent ${c}`);
      }
    }
  } catch (e) {
    console.error(e);
  }
}

module.exports = {populateQueue};
