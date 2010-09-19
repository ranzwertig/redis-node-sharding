var crypto = require('crypto'),
      fs = require('fs'),
      http = require('http'),
      sys = require('sys'),
      sharding = require('../lib/redis-sharding'),
      redis = require('../lib/redis-client'); // requires the redit client 
                                             // http://github.com/fictorial/redis-node-client/

var client = sharding.createShard([
    {'host' : '192.168.56.101','port' : 6379,'dbindex' : 1},
    {'host' : '192.168.56.102','port' : 6379,'dbindex' : 1},
    {'host' : '192.168.56.103','port' : 6379,'dbindex' : 1}
]);
    
// empty the database before test
client.flushdb();

// define how many operations should be done
var opsToPerform = 10000;

var allDone = function(d2) {
    var time;
    if (d1 > d2)
        time = new Date(d1 - d2);
    else
        time = new Date(d2 - d1);
    var t = time.getMinutes() + ':' + time.getSeconds() + ':' + time.getMilliseconds();
    client.dbsize(function(err, value){sys.log('set ' + value + ' keys in ' + t  + '.');});
}

var counter = 0,
    d1 = new Date();

for(var i = 0; i < opsToPerform; i += 1) {
    client.set('folo' + i, 'bar' + i, function(err, value){
        counter += 1;
        if (counter >= opsToPerform) {
            allDone(new Date());
        }
    });
}
