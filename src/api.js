const fetch = require('node-fetch');

const createQueryString = obj =>
  Object.keys(obj)
    .map(key => `${encodeURIComponent(key)}=${encodeURIComponent(obj[key])}`)
    .join('&');

const callBencmarkApi = (baseUrl, table, row, doRetry = true) =>
  fetch(`${baseUrl}?${createQueryString({table, id: row.id, first: row.initial_version, last: row.last_version})}`, {
    timeout: 50000
  })
    .then(response => response.json())
    .catch(e => {
      if (doRetry) {
        console.log('retry');
        return callBencmarkApi(baseUrl, table, row, false);
      }
      throw e;
    });

module.exports = {callBencmarkApi};
