/*
 * Created by G on 26/05/2016.
 */


"use strict";

var request = require("request");

module.exports = exports =  function(year, urlIn, cb) {
	var urlModel = urlIn.urlModel;
	var _idplus = year.join('-');
	var _iddescription = '';

	var data = [];

	var yearArray = year;
	var lenYearArray = yearArray.length;

	var _index = 0;
	
	//get data
	var getData = function(urlIn, cb) {
		request(urlIn, { json: true }, function(err, resp, body) {
			if (err || (resp && resp.statusCode !== 200)) {
				var msg = err ? err.message : body && body.message;
				console.log("failure running the input query: " + msg);
				process.exit(-1);
			} else {
				var data = body.data;

				cb(data);
			}
		});
	};
	
	var getPrjData = function(dataArray, lenDataArray, _index, cb) {
		if (_index > lenDataArray - 1) {
			return cb(data);
		}

		var _data = {};
		var _dataId = dataArray[_index].popId + '-' + _idplus;
		_data = dataArray[_index];
		var _area = _data.area_id;
		var _gender = _data.gender;
		var _agegroup = _data.age_band;

		var _urlmodel = urlModel + '&filter={"area_id":"' + _area + '","gender":"' + _gender + '","age_band":"' + _agegroup + '"}';
		request(_urlmodel, { json: true }, function(err, resp, body) {
			if (err || (resp && resp.statusCode !== 200)) {
				var msg = err ? err.message : body && body.message;
				console.log("failure running the data query: " + msg);
				process.exit(-1);
			} else {
				var _parameter = body.data[0].model_parameter;

				for (var i = 0; i < lenYearArray; i++) {
					var _datas = {};
					_datas.area_id = _data.area_id;
					_datas.area_name = _data.area_name;
					_datas.age_band = _data.age_band;
					_datas.gender = _data.gender;
					_datas.year = '' + yearArray[i];
					_datas.persons = +(+_parameter[0] + (+yearArray[i]) * (+_parameter[1])).toFixed(2);
					_datas.popId = _dataId;
					_datas.popId_description = _iddescription;

					data.push(_datas);
				}

				_index++;
				
				getPrjData(dataArray, lenDataArray, _index, cb);
			}
		});
	};

	getData(urlIn.urlIn, function(dataArray) {
		var lenDataArray = dataArray.length;

		getPrjData(dataArray, lenDataArray, _index, function(data) {
			cb(data);
		});
	});
};
