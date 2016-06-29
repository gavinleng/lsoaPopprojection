/*
 * Created by G on 02/06/2016.
 */


"use strict";

var request = require("request");
var _ = require("lodash");

module.exports = exports =  function(year, url, cb) {
    var _idplus = 'planning';
    var _iddescription = '';

    var urlIn = url.urlIn;
    var lenurlArray = urlIn.length;
    var urlMapping = url.urlMapping;
    var urlNorm = url.urlNorm;

    var areaId = [];
    var conPop = [];
    var ageGroup = [];
    var gender = [];
    
    var dataArray = [];

    var popLetData = [];
    
    var _index = 0;

    //get data
    var getAllData = function(_index, cb) {
        if (_index > lenurlArray - 1) {
            return cb(dataArray);
        }

        request(urlIn[_index], { json: true }, function(err, resp, body) {
            if (err || (resp && resp.statusCode !== 200)) {
                var msg = err ? err.message : body && body.message;
                console.log("failure running the input data query: " + msg);
                process.exit(-1);
            } else {
                var data = body.data;

                dataArray.push.apply(dataArray, data);

                _index++;

                getAllData(_index, cb);
            }
        });
    };

    var getPlanningData = function(cb) {
        getAllData(_index, function(data) {
            var i;
            var dataArray = data;
            var lenDataArray = dataArray.length;
            var objSortArray = [];
            for (i = 0; i < lenDataArray; i++) {
                var objSort = {};
                objSort = sortObject(dataArray[i], "key");
                objSortArray.push(objSort);
            }

            dataArray = _.uniq(objSortArray, function(elem) { return JSON.stringify(elem); });

            lenDataArray = dataArray.length;
            for (i = 0; i < lenDataArray; i++) {
                areaId.push('"' + dataArray[i].area_id + '"');
                conPop.push(+dataArray[i].persons);
                ageGroup.push('"' + dataArray[i].age_band + '"');
                gender.push('"' + dataArray[i].gender + '"');
            }

            var urlparea = urlMapping.split('?')[0].slice(0, -4) + 'distinct?key=parent_id&filter={"child_id":{"$in":[' + areaId+ ']}}';
            request(urlparea, { json: true }, function(err, resp, body) {
                if (err || (resp && resp.statusCode !== 200)) {
                    var msg = err ? err.message : body && body.message;
                    console.log("failure running the mapping query: " + msg);
                    process.exit(-1);
                } else {
                    var pareaArrayT  = body.data;
                    var lenPareaArray = pareaArrayT.length;
                    var _pareaArrayd = [];
                    for (i = 0; i < lenPareaArray; i++) {
                        _pareaArrayd.push('"' + pareaArrayT[i] + '"');
                    }
                    
                    var _indexx = 0;
                    getPareaArray(_indexx, lenPareaArray, _pareaArrayd, function(popLetData) {
                        cb(popLetData);
                    });
                }
            });
        });
    };

    var getPopPlan = function(sameGroup, rateData) {
        var i, j, totalPersons, totalPop;

        var data = [];
        var _rateData = []; // no planning data

        var lenGroupData = sameGroup.length;
        var lenRateData = rateData.length;

        var _data = []; // planning data
        var _pop = [];
        for (i = 0; i < lenRateData; i++) {
            var _rData = {};

            _rData = rateData[i];

            _rateData.push(_rData);

            for (j = 0; j < lenGroupData; j++) {
                if ('"' + _rData.area_id + '"' == sameGroup[j].area) {
                    _data.push(_rData);
                    _pop.push(+sameGroup[j].pop);

                    _rateData.pop();

                    break;
                }
            }
        }

        // For no planning LSOAs
        totalPop = 0;
        for (i = 0; i < lenGroupData; i++) {
            totalPop +=  sameGroup[i].pop;
        }

        lenRateData = _rateData.length;
        totalPersons = 0;
        for (i = 0; i < lenRateData; i++) {
            totalPersons += _rateData[i].persons;
        }

        for (i = 0; i < lenRateData; i++) {
            _rateData[i].persons += (_rateData[i].persons / totalPersons) * totalPop;
            _rateData[i].persons = +(_rateData[i].persons).toFixed(2);
        }

        data.push.apply(data, _rateData);

        // For planning LSOAs
        for (i = 0; i < lenGroupData; i++) {
            _data[i].persons += _pop[i];
        }

        data.push.apply(data,  _data);

        return data;
    };

    var sortObject = function(obj, item, oflag) { //item: "key" or "value"; oflag: -1-sort string ascending, 1-sort string descending
        if ((arguments.length < 2) || (arguments.length > 3)) {
            throw new Error('illegal argument count');
        }

        var flag = -1;
        if (arguments.length == 3) { flag = oflag; }

        var arr = [];
        var prop, i;
        for (prop in obj) {
            if (obj.hasOwnProperty(prop)) {
                arr.push({
                    'key': prop,
                    'value': obj[prop]
                });
            }
        }

        arr.sort(function(a, b) {
            var aItem = a[item], bItem = b[item];
            if (aItem < bItem) {
                if (flag == -1) return -1;
                if (flag == 1) return 1;
            }
            if (aItem >bItem) {
                if (flag == -1) return 1;
                if (flag == 1) return -1;
            }
            return 0;
        });

        var objSort = {};
        var len = arr.length;
        for (i = 0; i < len; i++) {
            objSort[arr[i].key] = arr[i].value;
        }

        return objSort;
    };

    var getPareaArray = function(_indexx, lenPareaArray, _pareaArrayd, cb) {
        if (_indexx > lenPareaArray - 1) {
            return cb(popLetData);
        }

        var urlparea = urlMapping.split('?')[0].slice(0, -4) + 'distinct?key=child_id&filter={"parent_id":' + _pareaArrayd[_indexx] + '}';
        request(urlparea, { json: true }, function(err, resp, body) {
            if (err || (resp && resp.statusCode !== 200)) {
                var msg = err ? err.message : body && body.message;
                console.log("failure running the mapping query: " + msg);
                process.exit(-1);
            } else {
                var pareaArrayd  = body.data;
                var lenPareaArrayd = pareaArrayd.length;
                var _pareaArray = [];
                var i;
                for (i = 0; i < lenPareaArrayd; i++) {
                    _pareaArray.push('"' + pareaArrayd[i] + '"');
                }

                var groupData = [];
                var lenDataArray = dataArray.length;
                for (i = 0; i < lenDataArray; i++) {
                    if (_pareaArray.indexOf(areaId[i]) > -1) {
                        groupData.push({"area": areaId[i], "pop": conPop[i], "ag": ageGroup[i], "gender": gender[i]});
                    }
                }

                var lenGroupData = groupData.length;
                var _agArray = [];
                var _genderArray = [];
                for (i = 0; i < lenGroupData; i++) {
                    _agArray.push(groupData[i].ag);
                    _genderArray.push(groupData[i].gender);
                }
                _agArray = _.uniq(_agArray);
                _genderArray = _.uniq(_genderArray);

                var surlNorm = urlNorm + '&filter={"area_id":{"$in":[' + _pareaArray + ']},"year":"' + year + '","age_band":{"$in":['+ _agArray + ']},"gender":{"$in":[' + _genderArray + ']}}';
                request(surlNorm, { json: true }, function(err, resp, body) {
                    if (err || (resp && resp.statusCode !== 200)) {
                        var msg = err ? err.message : body && body.message;
                        console.log("failure running the data query: " + msg);
                        process.exit(-1);
                    } else {
                        var rateDataT = body.data;

                        while(1) {
                            var lenArray = groupData.length;

                            if (!lenArray) break;

                            var rateData = _.filter(rateDataT, function(obj) { return (('"' + obj.age_band + '"' == groupData[0].ag) && ('"' + obj.gender + '"' == groupData[0].gender)); });
                            
                            var groupby = _.groupBy(groupData, function(o) {
                                return (o.ag == groupData[0].ag) && (o.gender == groupData[0].gender);
                            });

                            var sameGroup = groupby.true;

                            if (_.isUndefined(groupby.false)) {
                                groupData = [];
                            } else {
                                groupData = groupby.false;
                            }

                            var pareaPopData = getPopPlan(sameGroup, rateData);
                            var lenPareaPopData = pareaPopData.length;
                            for (i = 0; i< lenPareaPopData; i++) {
                                var _datas = {};

                                _datas = pareaPopData[i];
                                
                                _datas.popId = pareaPopData[i].popId + '-' + _idplus;
                                _datas.popId_description = _iddescription;

                                popLetData.push(_datas);
                            }
                        }

                        _indexx++;
                        
                        getPareaArray(_indexx, lenPareaArray, _pareaArrayd, cb);
                    }
                });
            }
        });
    };

    getPlanningData(function (popLetData) {
        cb(popLetData);
    });
};
