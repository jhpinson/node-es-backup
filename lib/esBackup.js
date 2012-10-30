var ElasticSearchClient = require('elasticsearchclient');
var fs = require('fs');
var async = require('async');
var byline = require('byline');

function EsBackup(host, port, index) {
  this._index = index;
  this._client = new ElasticSearchClient({
    hosts: [{
      host: host,
      port: port
    }]
  });
}


EsBackup.prototype._size = 1000;

EsBackup.prototype.getCount = function(callback) {
  var q = this._client.search(this._index, null, {
    size: 1
  })

  q.on('data', function(data) {

    var data = JSON.parse(data);

    if (data.error) {
      callback(data.error);
    } else {
      callback(null, data.hits.total);
    }


  })

  q.exec()
}

EsBackup.prototype._fetch = function(from, stream, callback) {
  var q = this._client.search(this._index, null, {
    size: this._size,
    from: from
  });

  q.on('data', function(data) {

    var data = JSON.parse(data);
    data.hits.hits.forEach(function(hit) {
      stream.write(JSON.stringify(hit) + '\n');
    }.bind(this))

    callback(null, data.hits.hits.length)
  });

  q.exec();
}

EsBackup.prototype._fetchDone = function(stream, err, res) {
  stream.end();

  if (err) {
    console.error(err);
    process.exit(1)
  } else {
    var count = 0;
    res.forEach(function (nb) {
      count += nb;
    })
    console.info('%d records dumped' , count);
  }
}

EsBackup.prototype.dump = function(filepath) {

  this.getCount(function(err, count) {

    if (err) {
      console.error(err)
      process.exit(1)
    }

    var stream = fs.createWriteStream(filepath, {
      flags: 'w',
      encoding: 'utf8',
      mode: 0666
    });

    var methods = [];
    var from = 0;
    methods.push(this._fetch.bind(this, from, stream));

    while(from <= count) {
      from += this._size;
      methods.push(this._fetch.bind(this, from, stream));
    }

    stream.once('open', function(fd) {
      async.series(methods, this._fetchDone.bind(this, stream));
    }.bind(this));

  }.bind(this));
}

EsBackup.prototype.restore = function(filepath) {
  var stream = byline(fs.createReadStream(filepath));
  var count = 0;
  var allRecordsReads = false;


  var q = async.queue(function(line, callback) {

    var data = line._source
    data.id = encodeURIComponent(line._id)
    //console.log(line._index, line._type, JSON.stringify(data))
    var q = this._client.index(this._index, line._type, data);
    q.on('data', function(data) {

      var data = JSON.parse(data);
      if (data.error) {
        callback(data.error);
        process.exit(1);
      }

      count++;
      callback();

    });
    q.exec();

  }.bind(this), 20);

  var checkEnd = function() {
    if (allRecordsReads && q.length() == 0) {
      console.info('%d records restored' , count);
    }
  };

  q.drain = checkEnd

  stream.on('end', function () {
    allRecordsReads = true;
    checkEnd();
  });

  stream.on('data', function(line) {
    var line = JSON.parse(line);
    q.push(line, function(err) {
      if (err) {
        console.log(err, count)
        console.error('Error inserting record %s', line._id);
        process.exit(1);
      }

    });
  });
}


exports.EsBackup = EsBackup