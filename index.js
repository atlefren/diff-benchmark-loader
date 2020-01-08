const runBenchmark = require('./src');

runBenchmark('osm.point_versions', 'osm.nodes', 'osm.node_results', {queueSize: 50, batchSize: 100, numGeoms: 1000});
