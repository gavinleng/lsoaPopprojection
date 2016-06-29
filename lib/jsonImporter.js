/**
 * Created by toby on 16/11/15.
 * Modified by G Leng on 01/06/16.
 */

module.exports = (function() {
  "use strict";
  
  var debug = require("debug")("debug");
  var _ = require("lodash");
  var nqmUtils = require("nqm-utils");
  var parser = require("mongo-parse");
  var commands = require("nqm-json-import/lib/commands");

  function JSONImporter() {
  }

  var inferSchema = function(obj) {
    var recurse = function(schemaObj, val, prop) {
      if (Array.isArray(val)) {
        schemaObj[prop] = [];
      } else if (typeof val === "object") {
        schemaObj[prop] = {};
        _.forEach(val, function(v,k) {
          recurse(schemaObj[prop], v, k);
        });
      } else {
        schemaObj[prop] = {
          __tdxType: [typeof val]
        }
      }
    };

    var ds = {};
    recurse(ds, obj, "schema");

    return ds.schema;
  };

  var getAccessToken = function(cb) {
    var self = this;
    if (self._config.credentials) {
      console.log("requesting access token");
      commands.getAccessToken(self._config.commandHost, self._config.credentials, function(err, accessToken) {
        if (!err) {
          self._accessToken = accessToken;
        }
        cb(err);
      });
    } else {
      console.log("getAccessToken: no credentials");
      process.nextTick(cb);
    }
  };

  var transformForUpsert = function(data) {
    var self = this;
    var obj = {
      __update: []
    };    
    
    // Check there is data for each primary key field.
    _.forEach(this._config.primaryKey, function(keyField) {
      var keyDataPointers = parser.DotNotationPointers(data, keyField);
      if (keyDataPointers.length === 0) {
        console.log("no data for primary key field '%s' - %j", keyField, data);
        process.exit(-1);
      } else {
        var updatePointers = parser.DotNotationPointers(obj, keyField);
        updatePointers[0].val = keyDataPointers[0].val;
      }
    });

    var flattened = nqmUtils.flattenJSON(data);
    _.forEach(flattened, function (v, k) {
      if (self._config.primaryKey.indexOf(k) >= 0) {
        // This a primary key field => do nothing.
      } else {
        // Do upsert of value.
        var update = {
          m: "r",     // method is replace
          p: k,       // property is key name
          v: v        // value is data
        };
        obj.__update.push(update);                
      }      
    });

    return obj;
  };

  var transmitQueue = function(data, cb, retrying) {
    var self = this;

    if (self._config.upsertMode) {
      data = _.map(data, function(d) { return transformForUpsert.call(self, d); });
    }

    var bulkLoad = self._config.upsertMode ? commands.upsertDatasetDataBulk : commands.addDatasetDataBulk;
    bulkLoad(self._config.commandHost, self._accessToken, self._config.targetDataset, data, function(err) {
      if (err) {
        if (err.statusCode === 401 && !retrying) {
          // Possible token expiry - attempt to re-acquire a token and try again.
          console.log("failed with 401 (authorisation) - retrying");
          self._accessToken = "";
          getAccessToken.call(self, function(err) {
            if (err) {
              return cb(err);
            }                                   
            // Got a new access token => try to transmit again.
            transmitQueue.call(self, data, cb, true);
          });          
          // Return here, i.e. don't fall-through and callback until getAccessToken has completed.
          return;
        } else {
          // Review - failed to add items, continue with other items or abort?
          console.log("failed to write %d items [%s]", data.length, err.message);
          self._failedCount += data.length;          
        }
      } else {
        debug("added %d data items", data.length);
        self._addedCount += data.length;
      }
      cb(err);
    });
  };

  var getTargetDataset = function(err, cb) {
    var self = this;

    if (err) {
      return self._bulkQueue;
    } else {
      if (!self._config.targetDataset) {
        commands.createTargetDataset(
            self._config.commandHost,
            self._accessToken,
            self._config.targetFolder,
            self._config.datasetName,
            self._config.basedOnSchema,
            self._config.schema || {},
            self._config.primaryKey,
            function(err, ds) {
              if (err) {
                return self._bulkQueue;
              }
              self._config.targetDataset = ds;
              cb();
            });
      } else {
        // Get target dataset details.
        commands.getDataset(self._config.queryHost, self._accessToken, self._config.targetDataset, function(err, ds) {
          if (err) {
            return cb();
          }
          // Cache dataset schema and primary key.
          self._config.schema = ds.schemaDefinition.dataSchema;
          self._config.primaryKey = ds.schemaDefinition.uniqueIndex.map(function (idx) {
            return idx.asc || idx.desc
          });

          cb();
        });
      }
    }
  };
  
  var save2tdx = function(cb) {
    var self = this;
    
    getAccessToken.call(self, function(err) {
      getTargetDataset.call(self, err, function() {
        transmitQueue.call(self, self._bulkQueue, function(err) {
          if (err) {
            console.log("error: " + err.message);
          } else {
            console.log("complete");
          }
          console.log("added %d items, %d failures",self._addedCount, self._failedCount);
          cb(err);
        });
      });
    });
  };
  
  JSONImporter.prototype.start = function(config, data, cb) {
    var self = this;
    this._config = config;

    this._addedCount = this._failedCount = 0;
    this._bulkQueue = [];

    self._config.commandHost = self._config.commandHost || "https://cmd.nqminds.com";
    self._config.datasetName = self._config.datasetName;
    self._config.bulkCount = self._config.bulkCount || 100;
    self._config.primaryKey = self._config.primaryKey || [];
    self._config.inferSchema = self._config.inferSchema === true;
    self._config.upsertMode = self._config.upsertMode || false;
    self._config.startAt = self._config.startAt || 0;
    self._config.endAt = self._config.endAt || -1;
    self._config.basedOnSchema = self._config.basedOnSchema || "dataset";

    if (self._config.upsertMode && self._config.targetDataset.length === 0 && self._config.primaryKey.length === 0) {
      console.log("forcing non-upsert mode since there is no primary key defined");
      self._config.upsertMode = false;
    }

    self._bulkQueue = data;

    if (self._config.inferSchema) {
      self._config.schema = inferSchema(data);
      console.log("inferred schema is %j", self._config.schema);
    }

    save2tdx.call(self, function(err) {
      cb(err, self._config.targetDataset);
    });
  };

  return JSONImporter;
}());
