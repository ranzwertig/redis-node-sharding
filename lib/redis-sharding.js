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
    'set', //tested
    'move', //tested
    'expire', //tested
    'setnx',
    'get', //tested
    'exists', //tested
    'del', //tested
    'type', //tested
    'ttl', //tested
    'getset',
    'setex',
    'incr',
    'incrby',
    'decr',
    'decrby',
    'append',
    'substr',
    'persist',
  //'mget',
  //'mset',
  //'msetnx',
  //'rename',
  //'renamenx',
  //'keys',
  //'randomkey',
  //'select',
  //'flushall',
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

//calculate the hash modulo for a even number of clients
var modFromHexString = function(string, nINT) {
    var bpe = 0,         
        mask = 0,       
        radix = mask + 1,
        digitsStr='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_=!@#$%^&*()[]{}|;:,.<>/?`~ \\\'\"+-',
        minSize = 0;  
        
    for (bpe = 0; (1 << (bpe + 1)) > ( 1 << bpe); bpe++);
    bpe >>= 1;
    mask = (1 << bpe ) - 1;
    radix = mask + 1;
    
    var mod = function(x,n) {
        var i,
            c = 0;
        for (i = x.length - 1; i >= 0; i--)
            c = (c * radix + x[i]) % n;
        return c;
    };
    
    var add = function(x,n) {
        var i,k,b,
            c = 0;
            
        x[0] += n;
        k = x.length;
        
        for ( i = 0; i < k; i++) {
            c += x[i];
            b = 0;
            if (c < 0) {
                b =-(c >> bpe);
                c += b * radix;
            }
            x[i] = c & mask;
            c = (c >> bpe) - b;
            if (!c) return;
        }
    };
    
    var multiply = function(x,n) {
        var i,k,b,
            c = 0;
            
        if (!n)
            return;
            
        k = x.length;
        
        for (i = 0; i < k; i++) {
            c += x[i] * n;
            b = 0;
            if (c < 0) {
                b =-(c >> bpe);
                c += b * radix;
            }
            x[i] = c & mask;
            c = (c >> bpe) - b;
        }
    };
            
    var copy=function(x,n) {
        var i,c;
        for (c = n,i = 0; i < x.length; i++) {
            x[i] = c & mask;
            c >>= bpe;
        }
    };
            
    var makeBig = function(t,bits,minSize) {   
        var i,k;
        k = Math.ceil(bits / bpe) + 1;
        k = minSize > k ? minSize : k;
        buff = new Array(k);
        copy(buff,t);
        return buff;
    };
    
    var getBigByHexStr = function(s,minSize) {
          var d, i, j, x, y, kk,
            base = 16;
            
          var k=s.length;
          x=makeBig(0,base*k,0);
          for (i=0;i<k;i++) {
            d=digitsStr.indexOf(s.substring(i,i+1),0);
            if (base<=36 && d>=36)
              d-=26;
            if (d>=base || d<0) {
              break;
            }
            multiply(x,base);
            add(x,d);
          }

          for (k=x.length;k>0 && !x[k-1];k--);
          k=minSize>k+1 ? minSize : k+1;
          y=new Array(k);
          kk=k<x.length ? k : x.length;
          for (i=0;i<kk;i++)
            y[i]=x[i];
          for (;i<k;i++)
            y[i]=0;
          return y;
        };
        
        var bi = getBigByHexStr(string);
        return mod(bi, nINT);
};

var crc32 = function(str) { 

	var table = "00000000 77073096 EE0E612C 990951BA 076DC419 706AF48F E963A535 9E6495A3 0EDB8832 79DCB8A4 E0D5E91E 97D2D988 09B64C2B 7EB17CBD E7B82D07 90BF1D91 1DB71064 6AB020F2 F3B97148 84BE41DE 1ADAD47D 6DDDE4EB F4D4B551 83D385C7 136C9856 646BA8C0 FD62F97A 8A65C9EC 14015C4F 63066CD9 FA0F3D63 8D080DF5 3B6E20C8 4C69105E D56041E4 A2677172 3C03E4D1 4B04D447 D20D85FD A50AB56B 35B5A8FA 42B2986C DBBBC9D6 ACBCF940 32D86CE3 45DF5C75 DCD60DCF ABD13D59 26D930AC 51DE003A C8D75180 BFD06116 21B4F4B5 56B3C423 CFBA9599 B8BDA50F 2802B89E 5F058808 C60CD9B2 B10BE924 2F6F7C87 58684C11 C1611DAB B6662D3D 76DC4190 01DB7106 98D220BC EFD5102A 71B18589 06B6B51F 9FBFE4A5 E8B8D433 7807C9A2 0F00F934 9609A88E E10E9818 7F6A0DBB 086D3D2D 91646C97 E6635C01 6B6B51F4 1C6C6162 856530D8 F262004E 6C0695ED 1B01A57B 8208F4C1 F50FC457 65B0D9C6 12B7E950 8BBEB8EA FCB9887C 62DD1DDF 15DA2D49 8CD37CF3 FBD44C65 4DB26158 3AB551CE A3BC0074 D4BB30E2 4ADFA541 3DD895D7 A4D1C46D D3D6F4FB 4369E96A 346ED9FC AD678846 DA60B8D0 44042D73 33031DE5 AA0A4C5F DD0D7CC9 5005713C 270241AA BE0B1010 C90C2086 5768B525 206F85B3 B966D409 CE61E49F 5EDEF90E 29D9C998 B0D09822 C7D7A8B4 59B33D17 2EB40D81 B7BD5C3B C0BA6CAD EDB88320 9ABFB3B6 03B6E20C 74B1D29A EAD54739 9DD277AF 04DB2615 73DC1683 E3630B12 94643B84 0D6D6A3E 7A6A5AA8 E40ECF0B 9309FF9D 0A00AE27 7D079EB1 F00F9344 8708A3D2 1E01F268 6906C2FE F762575D 806567CB 196C3671 6E6B06E7 FED41B76 89D32BE0 10DA7A5A 67DD4ACC F9B9DF6F 8EBEEFF9 17B7BE43 60B08ED5 D6D6A3E8 A1D1937E 38D8C2C4 4FDFF252 D1BB67F1 A6BC5767 3FB506DD 48B2364B D80D2BDA AF0A1B4C 36034AF6 41047A60 DF60EFC3 A867DF55 316E8EEF 4669BE79 CB61B38C BC66831A 256FD2A0 5268E236 CC0C7795 BB0B4703 220216B9 5505262F C5BA3BBE B2BD0B28 2BB45A92 5CB36A04 C2D7FFA7 B5D0CF31 2CD99E8B 5BDEAE1D 9B64C2B0 EC63F226 756AA39C 026D930A 9C0906A9 EB0E363F 72076785 05005713 95BF4A82 E2B87A14 7BB12BAE 0CB61B38 92D28E9B E5D5BE0D 7CDCEFB7 0BDBDF21 86D3D2D4 F1D4E242 68DDB3F8 1FDA836E 81BE16CD F6B9265B 6FB077E1 18B74777 88085AE6 FF0F6A70 66063BCA 11010B5C 8F659EFF F862AE69 616BFFD3 166CCF45 A00AE278 D70DD2EE 4E048354 3903B3C2 A7672661 D06016F7 4969474D 3E6E77DB AED16A4A D9D65ADC 40DF0B66 37D83BF0 A9BCAE53 DEBB9EC5 47B2CF7F 30B5FFE9 BDBDF21C CABAC28A 53B39330 24B4A3A6 BAD03605 CDD70693 54DE5729 23D967BF B3667A2E C4614AB8 5D681B02 2A6F2B94 B40BBE37 C30C8EA1 5A05DF1B 2D02EF8D";
 
	var x = 0,
	    y = 0,
	    crc = 0; 
	var crc = crc ^ (-1);
	
	for( var i = 0, iTop = str.length; i < iTop; i++ ) {
		y = ( crc ^ str.charCodeAt( i ) ) & 0xFF;
		x = "0x" + table.substr( y * 9, 8 );
		crc = ( crc >>> 8 ) ^ x;
	}
	return crc ^ (-1);
};

Shard.prototype.getClientNumberByKey = function(key, numberOfClients) {
    var hash = crc32(key);
    var hashValue = Math.abs(parseInt(hash, 16));
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

Shard.prototype.auth = function(password, callback) {
    var error = false,
        answered = 0,
        value = [];
    
    var authAllDone = function() {
        if (answered == exports.clients.length)
            callback(error, value);
    }
    
    for (var clientIndex = 0; clientIndex < exports.clients.length; clientIndex += 1) {
        exports.clients[clientIndex].client.auth(password, function(err, val){
            if (err) {
                if (error === false) {
                    error = [];
                }
                err.answeringClient = clientIndex;
                error.push(err);
            }
            else {
                log('SHARD: ' + clientIndex + ' AUTH: ' + val);
                val.answeringClient = clientIndex;
                value.push(val);
            }
            answered += 1;
            authAllDone();
        });
    }
};

//logging
var log = function(message, type) {
    if (DEBUG_MODE) {
        sys.log(type + ': ' + message);
    }
};
