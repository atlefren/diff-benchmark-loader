const runBenchmark = require('./src');

//runBenchmark('osm.point_versions', 'osm.nodes', 'osm.point_results', {queueSize: 50, batchSize: 100, numGeoms: 10000});
//runBenchmark('osm.line_versions', 'osm.ways', 'osm.linestring_results', {queueSize: 50, batchSize: 100, numGeoms: 10000});
runBenchmark('osm.polygon_versions', 'osm.ways', 'osm.polygon_results', {
  queueSize: 50,
  batchSize: 100,
  numGeoms: 10000
});
