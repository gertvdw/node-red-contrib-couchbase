/**
 * Couchbase node
 * @author Gert van der Weyde (gertvdw@gmail.com)
 */

module.exports = function(RED) {
    function CouchbaseNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        var couchbase = require('couchbase');
        var cluster = new couchbase.Cluster('couchbase://' + config.server);  // + config.server);
        var bucket = cluster.openBucket(config.bucket, function(err) {
            if (err) { 
                node.error("Failed to connect to " + config.bucket, err);
                node.status({ fill: 'red', shape: 'ring', text: 'disconnected'});
            } else {
                node.status({ fill: 'green', shape: 'dot', text: 'connected'});
            }
        });
        bucket.operationTimeout = 60 * 1000;
        var context = this.context();
        this.mybucket = bucket;

        this.on('input', function(msg) {
            var doc = msg.payload;
            try {
                this.mybucket.upsert(msg.topic, doc, function (err, result) {
                    if (err) {
                        node.status({fill:'red', shape:'ring'});
                        node.error(err);
                    } else {
                        node.status( { fill:'green', shape:'dot'} );
                    }
                });
            } catch (err) {
                node.status( { fill:'red', shape:'ring', text:'timeout'} );
                node.error('connection timeout', msg);
            }
        });
    }
    RED.nodes.registerType("couchbase-node-in", CouchbaseNode);
}