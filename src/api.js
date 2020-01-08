const fetch = require('node-fetch');

const createQueryString = obj =>
  Object.keys(obj)
    .map(key => `${encodeURIComponent(key)}=${encodeURIComponent(obj[key])}`)
    .join('&');

const callBencmarkApi = (baseUrl, table, row) =>
  fetch(
    `${baseUrl}?${createQueryString({table, id: row.id, first: row.initial_version, last: row.last_version})}`
  ).then(response => response.json());

module.exports = {callBencmarkApi};
