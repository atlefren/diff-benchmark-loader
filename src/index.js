const {Pool} = require('pg');
const {Timer} = require('process-stopwatch');
const {readVersions, getSaveResult, createResultsTable} = require('./database');
const {callBencmarkApi} = require('./api');
const {queue, getCaller} = require('./queue');
const {printStats} = require('./stats');
const {ms2Time} = require('./util');
require('dotenv').config();

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

const Counter = (initial = 0) => {
  let c = initial;

  return {
    get: () => c,
    inc: () => c++,
    dec: () => c--,
    isMultipleOf: num => c % num === 0
  };
};

async function runBenchmark(versionsTable, geomTable, resultsTable, config = {}) {
  const connectionString = process.env['CONN_STR'];
  const apiUrl = process.env['BENCHMARK_URL'];
  const {queueSize = 10, batchSize = 10, numGeoms = null, showStats = true} = config;

  const timer = new Timer();
  timer.start();

  const pool = new Pool({connectionString});
  await createResultsTable(pool, resultsTable);
  const saveResult = getSaveResult(pool, resultsTable);

  const complete = () => {
    pool.end();
    timer.stop();
    console.log(`Completed benchmark in ${ms2Time(timer.read().millis)}`);
    if (showStats) {
      printStats(resultsTable);
    }
  };

  const counter = Counter();

  const done = () => {
    counter.dec();

    if (counter.isMultipleOf(100)) {
      console.log(counter.get());
    }
    if (counter.get() < 1) {
      complete();
    }
  };

  const callQueue = queue(queueSize);

  for await (const r of getRows(pool, versionsTable, batchSize, numGeoms)) {
    counter.inc();
    callQueue.add(
      getCaller(callBencmarkApi)(apiUrl, geomTable, r),
      data => {
        saveResult(r.id, data)
          .then(() => done())
          .catch(e => {
            console.error(e);
            done();
          });
      },
      err => {
        console.error(err);
        done();
      }
    );
  }
}
module.exports = runBenchmark;
