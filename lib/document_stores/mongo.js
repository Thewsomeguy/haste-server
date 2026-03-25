

var MongoClient = require('mongodb').MongoClient,
    winston = require('winston');

var MongoDocumentStore = function (options) {
    this.expire = options.expire;
    this.connectionUrl = process.env.DATABASE_URl || options.connectionUrl;
    this.db = null;
};

MongoDocumentStore.prototype.set = function (key, data, callback, skipExpire) {
    var now = Math.floor(new Date().getTime() / 1000),
        that = this;

    this.safeConnect(function (err, db) {
        if (err)
            return callback(false);

        db.collection('entries').updateOne({
            'entry_id': key,
            $or: [
                { expiration: -1 },
                { expiration: { $gt: now } }
            ]
        }, {
            $set: {
                'entry_id': key,
                'value': data,
                'expiration': that.expire && !skipExpire ? that.expire + now : -1
            }
        }, {
            upsert: true
        }).then(function () {
            callback(true);
        }).catch(function (err) {
            winston.error('error persisting value to mongodb', { error: err });
            callback(false);
        });
    });
};

MongoDocumentStore.prototype.get = function (key, callback, skipExpire) {
    var now = Math.floor(new Date().getTime() / 1000),
        that = this;

    this.safeConnect(function (err, db) {
        if (err)
            return callback(false);

        db.collection('entries').findOne({
            'entry_id': key,
            $or: [
                { expiration: -1 },
                { expiration: { $gt: now } }
            ]
        }).then(function (entry) {
            callback(entry === null ? false : entry.value);

            if (entry !== null && entry.expiration !== -1 && that.expire && !skipExpire) {
                db.collection('entries').updateOne({
                    'entry_id': key
                }, {
                    $set: { 'expiration': that.expire + now }
                }).catch(function (err) {
                    winston.error('error updating expiration in mongodb', { error: err });
                });
            }
        }).catch(function (err) {
            winston.error('error retrieving value from mongodb', { error: err });
            callback(false);
        });
    });
};

MongoDocumentStore.prototype.safeConnect = function (callback) {
    if (this.db) {
        return callback(undefined, this.db);
    }

    var that = this;
    MongoClient.connect(this.connectionUrl)
        .then(function (client) {
            that.db = client.db();
            callback(undefined, that.db);
        })
        .catch(function (err) {
            winston.error('error connecting to mongodb', { error: err });
            callback(err);
        });
};

module.exports = MongoDocumentStore;
