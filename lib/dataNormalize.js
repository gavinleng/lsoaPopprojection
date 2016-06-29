/*
 * Created by G on 26/05/2016.
 */


"use strict";

var request = require("request");

module.exports = exports =  function(year, urlIn, cb) {
	var urlNorm = urlIn.urlNorm;
	var _idplus = 'norm';
	var _iddescription = '';

	var urldistinct = urlIn.urlIn.split('?')[0].slice(0, -4) + 'distinct?key=';

	// get Area array
	var getArea = function(cb) {
		var areaArrayurl = urldistinct + 'area_id';
		request(areaArrayurl, { json: true }, function(err, resp, body) {
			if (err || (resp && resp.statusCode !== 200)) {
				var msg = err ? err.message : body && body.message;
				console.log("failure running the area query: " + msg);
				process.exit(-1);
			} else {
				var areaArray = body.data;
				var lenAreaArray = areaArray.length;
				var _areaArray = [];
				for (var i = 0; i < lenAreaArray; i++) {
					_areaArray.push('"' + areaArray[i] + '"');
				}

				cb(_areaArray);
			}
		});
	};

	// get gender array
	var getGender = function(cb) {
		var genderArrayurl = urldistinct + 'gender';
		request(genderArrayurl, { json: true }, function(err, resp, body) {
			if (err || (resp && resp.statusCode !== 200)) {
				var msg = err ? err.message : body && body.message;
				console.log("failure running the gender query: " + msg);
				process.exit(-1);
			} else {
				var genderArray = body.data;
				var lenGenderArray = genderArray.length;
				var _genderArray = [];
				for (var i = 0; i < lenGenderArray; i++) {
					_genderArray.push('"' + genderArray[i] + '"');
				}

				cb(_genderArray);
			}
		});
	};

	// get age group array
	var getAg = function(cb) {
		var agArrayurl = urldistinct + 'age_band';
		request(agArrayurl, { json: true }, function(err, resp, body) {
			if (err || (resp && resp.statusCode !== 200)) {
				var msg = err ? err.message : body && body.message;
				console.log("failure running the age group query: " + msg);
				process.exit(-1);
			} else {
				var agArray = body.data;
				var lenAgArray = agArray.length;
				var _agArray = [];
				for (var i = 0; i < lenAgArray; i++) {
					_agArray.push('"' + agArray[i] + '"');
				}

				cb(_agArray);
			}
		});
	};

	// get year array
	var getYear = function(cb) {
		var yearArray = year;
		var lenYearArray = yearArray.length;
		var _yearArray = [];
		for (var i = 0; i < lenYearArray; i++) {
			_yearArray.push('"' + yearArray[i] + '"');
		}

		cb(_yearArray);
	};

	//get normalized data array
	var getNormData = function(cb) {
		getArea(function(_areaArray) {
			getGender(function(_genderArray) {
				getAg(function(_agArray) {
					getYear(function(_yearArray) {
						var _durlNorm = urlNorm+ '&filter={"area_id":{"$in":[' + _areaArray + ']},"gender":{"$in":[' + _genderArray + ']},"age_band":{"$in":[' + _agArray + ']},"year":{"$in":[' + _yearArray + ']}}';
						request(_durlNorm, { json: true }, function(err, resp, body) {
							if (err || (resp && resp.statusCode !== 200)) {
								var msg = err ? err.message : body && body.message;
								console.log("failure running the normalized data query: " + msg);
								process.exit(-1);
							} else {
								var normData = body.data;
								var lenNormData = normData.length;

								var _dataId = normData[0].popId + '-' + _idplus;
								
								var data = [];
								for (var i = 0; i < lenNormData; i++) {
									var _datas = {};
									_datas = normData[i];

									_datas.popId = _dataId;
									_datas.popId_description = _iddescription;

									data.push(_datas);
								}

								cb(data);
							}
						});
					});
				});
			});
		});
	};

	getNormData(function(data) {
		cb(data);
	});
};
