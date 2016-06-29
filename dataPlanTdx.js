/*
 * Created by G on 16/06/2016.
 */


"use strict";

var dataPlanning = require('./lib/dataPlanning.js');
var import2Tdx = require('./lib/import2Tdx.js');

module.exports = exports =  function(tdxConfig, year, urlIn, cb) {
    dataPlanning(year, urlIn, function (data) {
        import2Tdx(tdxConfig, data, function(id) {
            console.log('the planning normalized data id is: ' + id);
            console.log('Planning LSOA population projection normalized data set is saved to TDX.');

            var urlOut = 'https://q.nqminds.com/v1/datasets/' + id + '/data?opts={"limit":15765120}';
            cb(urlOut);
        });
    });
};
