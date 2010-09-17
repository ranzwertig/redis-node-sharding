## redis-node-sharding

This is a module based on the [redis-node-client](http://github.com/fictorial/redis-node-client) of Fictorial.
The module provides sharding by consistent hashing. 

Careful: this module is in a very early state of development. Some of the functions
are not implemented yet.

## TODO

* rename and renamenx requires additional computation
* use BigInt to calculate the hash for selecting the shard (currently just works with a odd number of servers)
* ...

### How to use this

After the initialisation it can be used exactly like Fictorials module.

#### Example

<pre><code>
var sharding = sharding = require('./redis-sharding');

var client = sharding.createShard([
    {'host' : '192.168.178.38','port' : 6379,'dbindex' : 1},
    {'host' : '192.168.178.37','port' : 6379,'dbindex' : 1},
    {'host' : '192.168.178.36','port' : 6379,'dbindex' : 1}
]);

client.set('foo', 'bar');

client.dbsize(function(err, value) {
    sys.log('SIZE:' + value);
});

client.get('foo', function(err, value){
    sys.log(value);
});
</code></pre>

### Author

Christian Ranz - [twitter](http://twitter.com/ranzwertig) - [blog](http://www.christianranz.com)
