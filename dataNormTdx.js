/*
 * Created by G on 16/06/2016.
 */


"use strict";

var dataNormalize = require('./lib/dataNormalize.js');
var import2Tdx = require('./lib/import2Tdx.js');

module.exports = exports =  function(tdxConfig, year, urlIn, cb) {
    dataNormalize(year, urlIn, function(data) {
        import2Tdx(tdxConfig, data, function(id) {
            console.log('the normalized data id is: ' + id);
            console.log('LSOA population projection normalized data set is saved to TDX.');

            var urlOut = 'https://q.nqminds.com/v1/datasets/' + id + '/data?opts={"limit":15765120}';
            cb(urlOut);
        });
    });
};
