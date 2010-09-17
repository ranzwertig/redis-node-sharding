var sys = require('sys'),
    crypto = require('crypto');
    redis = require('./redis-client'),
    
    DEBUG_MODE = false;
    
exports.allClientsConnected = false;
exports.connectedClients = 0;
exports.clients = [];

var onConnect = onReconnect = function() {
    log('client connects');
    exports.connectedClients += 1;
    if (exports.connectedClients == exports.clients.length)
        exports.allClientsConnected = true;
};

var onNoConnection = function() {
    exports.connectedClients += 1;
    if (exports.connectedClients == exports.clients.length)
        exports.allClientsConnected = true;
};

var onReconnecting = function() {
    if (exports.connectedClients == exports.clients.length)
        exports.allClientsConnected = true;
};

//Shard constructor
function Shard() {
    //build the command functions    
    createRedisCommands();

    // select the databases
    for (var clientIndex = 0; clientIndex < exports.clients.length; clientIndex += 1) {
        log('SELECT ' + exports.clients[clientIndex].dbindex + ' on host ' + exports.clients[clientIndex].host);
        exports.clients[clientIndex].client.select(exports.clients[clientIndex].dbindex, onDbSelected);
    }  
}

//function to create a shard
exports.createShard = function(servers, options) {

    var activeClients = [];
    var connectedClients = 0;

    for (var serverIndex = 0; serverIndex < servers.length; serverIndex += 1) {
        var client = redis.createClient(servers[serverIndex].port, servers[serverIndex].host, options);
        
        activeClients[serverIndex] = {
            'client' : client,
            'dbindex': servers[serverIndex].dbindex,
            'port' : servers[serverIndex].port,
            'host' : servers[serverIndex].host
        };
        
        // on client connect
        client.addListener('connect', onConnect);
        
        // on client reconnect
        client.addListener('reconnect', onReconnect);
        
        // client tries to reconnect
        client.addListener('reconnecting', onReconnecting);
        
        // on client close
        client.addListener('noconnection', onNoConnection);
        
        // on client end
        client.addListener('drained', function(){});
        
    }
    
    exports.clients = activeClients;
    
    var shard = new Shard();
    return shard;
};

exports.Shard = Shard;

var onDbSelected = function() {
    log('SELECTED');
};

var mappingFunctions = [
    // 3 arg functions
    'set',
    'move',
    'expire',
        
    // 2 arg functions
    'get',
    'exists',
    'del',
    'type',
    'ttl'
];

var createRedisCommands = function(){
    for (var fIndex = 0; fIndex < mappingFunctions.length; fIndex += 1) {
        var fName = mappingFunctions[fIndex];
        Shard.prototype[fName] = makeFunction(fName);
    }
};

var makeFunction = function(fName) {
    return function() {
        var args = Array.prototype.slice.call(arguments);
        if (args.length > 0 && Array.isArray(args[0])) 
          args = args.shift().concat(args);
        var client = this.selectClient(args[0]);
        log(fName + ' performed in shard ' + this.getClientNumberByKey(args[0],exports.clients.length));
        client[fName].apply(client, args);
    };
};

Shard.prototype.getClientNumberByKey = function(key, numberOfClients) {
    var hash = crypto.createHash('md5').update(key).digest('hex');
    var hashValue = parseInt(hash, 16);
    return hashValue % numberOfClients;
};

Shard.prototype.selectClient = function(key) {
    return exports.clients[this.getClientNumberByKey(key, exports.clients.length)].client;
};

Shard.prototype.flushdb = function(callback) {
    if (typeof callback === 'undefined') callback = function(){};
    for (var clientIndex = 0; clientIndex < exports.clients.length; clientIndex += 1) {
        exports.clients[clientIndex].client.flushdb(callback);
    }
};

Shard.prototype.dbsize = function(callback) {
    var totalSizeAllClients = 0,
        error = false,
        answered = 0;
    
    var dbsizeAllDone = function() {
        if (answered == exports.clients.length)
            callback(error, totalSizeAllClients);
    }
    
    for (var clientIndex = 0; clientIndex < exports.clients.length; clientIndex += 1) {
        exports.clients[clientIndex].client.dbsize(function(err, val){
            if (err) {
                error = err;
            }
            else {
                log('SHARD: ' + clientIndex + ' SIZE: ' + val);
                totalSizeAllClients += val;
            }
            answered += 1;
            dbsizeAllDone();
        });
    }
};

//logging
var log = function(message, type) {
    if (DEBUG_MODE) {
        sys.log(type + ': ' + message);
    }
};
