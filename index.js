const {populateQueue} = require('./src');

populateQueue('osm.point_versions', 'osm.nodes', 'osm.point_results', {batchSize: 1000});
/*
populateQueue('osm.line_versions', 'osm.ways', 'osm.linestring_results', {batchSize: 100, numGeoms: 10000});
populateQueue('osm.polygon_versions', 'osm.ways', 'osm.polygon_results', {
  batchSize: 100,
  numGeoms: 10000
});
*/
