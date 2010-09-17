var sys = require('sys'),
    crypto = require('crypto'),
    sharding = require('../lib/redis-sharding'),
    redis = require('../lib/redis-client');
    
//var client = redis.createClient(6379, '192.168.178.38');
//client.select(1);
    
var client = sharding.createShard([
    {'host' : '192.168.56.101','port' : 6379,'dbindex' : 1},
    {'host' : '192.168.56.102','port' : 6379,'dbindex' : 1},
    {'host' : '192.168.56.103','port' : 6379,'dbindex' : 1}
]);


client.flushdb(function() {
    sys.log('DB Flushed');
});

client.set('foo0', 'bar0');
client.set('foo1', 'bar1');
client.set('foo2', 'bar2');
client.set('foo3', 'bar3');
client.set('foo4', 'bar4');

client.dbsize(function(err, val) {
    sys.log('SIZE:' + val);
});

client.flushdb(function() {
    sys.log('DB Flushed');
});

client.dbsize(function(err, val) {
    sys.log('SIZE:' + val);
});

client.set('foo5', 'bar5', function(){});
client.set('foo6', 'bar6', function(){});

client.dbsize(function(err, val) {
    sys.log('SIZE:' + val);
});

client.get('foo5', function(err, value){
    sys.log(value);
});

client.get('foo6', function(err, value){
    sys.log(value);
});

client.exists('foo5', function(err, value) {if (value) sys.log('yeah'); else sys.log('no');});
client.exists('foo6', function(err, value) {if (value) sys.log('yeah'); else sys.log('no');});

client.del('foo6', function(){});
client.exists('foo6', function(err, value) {if (value) sys.log('yeah'); else sys.log('no');});


client.set('foo7', 'bar7', function(){});
client.set('foo8', 'bar8', function(){});

client.type('foo8', function(err, val){sys.debug(val);});

client.set('foomove', 'barmove');
client.move('foomove', 2, function(err, val){if(!err) sys.log('moved');});

client.expire('foo8', 10, function(err, val){if(!err) sys.log('expire ' + val);});
client.ttl('foo8', function(err, val){if(!err) sys.log('ttl ' + val);else sys.log(err);});
