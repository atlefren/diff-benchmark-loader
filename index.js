const {populateQueue} = require('./src');

async function loadTest() {
  //create test tables with 1000 samples each
  await populateQueue('osm.point_versions', 'osm.nodes', 'osm_test.point_results', {numGeoms: 1000});
  await populateQueue('osm.line_versions', 'osm.ways', 'osm_test.line_results', {numGeoms: 1000});
  await populateQueue('osm.polygon_versions', 'osm.ways', 'osm_test.polygon_results', {numGeoms: 1000});
}

//loadTest();

async function load() {
  //create test tables with 1000 samples each
  await populateQueue('osm.point_versions_2', 'osm.nodes', 'osm.point_results');
  await populateQueue('osm.line_versions', 'osm.ways', 'osm.line_results');
  await populateQueue('osm.polygon_versions', 'osm.ways', 'osm.polygon_results');
}
load();

/*
populateQueue('osm.line_versions', 'osm.ways', 'osm.linestring_results', {batchSize: 100, numGeoms: 10000});
populateQueue('osm.polygon_versions', 'osm.ways', 'osm.polygon_results', {
  batchSize: 100,
  numGeoms: 10000
});
*/
