/*global require,module,process*/

var MongoClient = require('mongodb').MongoClient;
var winston = require('winston');

// A mongo document store
var MongoDocumentStore = function (options) {
  this.expireJS = options.expire;
  this.connectionUrl = process.env.DATABASE_URL || options.connectionUrl;
};

MongoDocumentStore.prototype = {

  // Set a given key
  set: function (key, data, callback, skipExpire) {
    var now = Math.floor(new Date().getTime() / 1000);
    var that = this;

    this.safeConnect(function (err, db) {
      if (err) { return callback(false); }

      db.collection('entries').updateOne({
        _id: key
      }, {
        $set: {
          _id: key,
          value: data,
          expiration: that.expireJS && !skipExpire ? that.expireJS + now : -1
        }
      }, {
        upsert: true
      }, function(err) {
        if (err) {
          winston.error('error persisting value to mongodb', { error: err });
          return callback(false);
        }

        callback(true);
      });
    });
  },

  // Get a given key's data
  get: function (key, callback, skipExpire) {
    var now = Math.floor(new Date().getTime() / 1000);
    var that = this;
    this.safeConnect(function (err, db) {
      if (err) { return callback(false); }

      db.collection('entries').findOne({
        _id: key,
        $or: [
          { expiration: -1 },
          { expiration: { $gt: now } }
        ]
      }, function(err, entry) {
        if (err) {
          winston.error('error retrieving value from mongodb', { error: err });
          return callback(false);
        }

        callback(entry !== null ? entry.value : false);

        if (entry !== null && entry.expiration !== -1 && that.expireJS && !skipExpire) {
          db.collection('entries').updateOne({
            _id: key
          }, {
            $set: {
              expiration: that.expireJS + now
            }
          });
        }
      });
    });
  },

  // A connection wrapper
  safeConnect: function (callback) {
    MongoClient.connect(this.connectionUrl, function (err, client) {
      var db = client.db();

      if (err) {
        winston.error('error connecting to mongodb', { error: err });
        callback(err);
      } else {
        callback(undefined, db);
      }
    });
  }

};

module.exports = MongoDocumentStore;
