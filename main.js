/*
 * Created by G on 16/06/2016.
 */


// tdxconfig file
var tdxconfig = require('./tdx-config.js');

//projection module
var dataPrjTdx = require('./dataPrjTdx.js');

var year1 = [2018, 2019];

var urlIn1 = {
	"urlIn": 'https://q.nqminds.com/v1/datasets/SyeM-95eM/data?opts={"limit":1390120}&filter={"gender":"male","area_id":"E01000027","age_band":"20-24","year":"2014"}',
	"urlModel": 'https://q.nqminds.com/v1/datasets/SyePdq0-m/data?opts={"limit":1390120}'
};

console.log('the input query is: ' + urlIn1.urlIn);

var tdxconfig1 = {};
tdxconfig1.credentials = tdxconfig.credentials;
tdxconfig1.datasetName = "popletProjection";
tdxconfig1.basedOnSchema = tdxconfig.basedOnSchema;
tdxconfig1.targetFolder = tdxconfig.targetFolder;
tdxconfig1.upsertMode = tdxconfig.upsertMode;
tdxconfig1.schema = tdxconfig.schema;

dataPrjTdx(tdxconfig1, year1, urlIn1, function(urlOut1) {
	console.log('the projection data url is: ' + urlOut1);

	//normalized module
	var dataNormTdx = require('./dataNormTdx.js');

	var year2 = [2018, 2019];

	var urlIn2 = {
		"urlIn": urlOut1,
		"urlNorm": 'https://q.nqminds.com/v1/datasets/BJg-oAZqz/data?opts={"limit":15765120}'
	};

	var tdxconfig2 = {};
	tdxconfig2.credentials = tdxconfig.credentials;
	tdxconfig2.datasetName = "popletNormalized";
	tdxconfig2.basedOnSchema = tdxconfig.basedOnSchema;
	tdxconfig2.targetFolder = tdxconfig.targetFolder;
	tdxconfig2.upsertMode = tdxconfig.upsertMode;
	tdxconfig2.schema = tdxconfig.schema;
	
	dataNormTdx(tdxconfig2, year2, urlIn2, function(urlOut2) {
		console.log('the normalized data url is: ' + urlOut2);
		
		//planning module
		var dataPlanTdx = require('./dataPlanTdx.js');

		var year3 = "2018";

		var urlIn3 = {
			"urlIn": ['https://q.nqminds.com/v1/datasets/r1et2WnBV/data?opts={"limit":1000}&filter={"year":"2018"}', 'https://q.nqminds.com/v1/datasets/SklcXCyvN/data?opts={"limit":1000}&filter={"year":"2018"}', 'https://q.nqminds.com/v1/datasets/Skg0UrbwV/data?opts={"limit":1000}&filter={"year":"2018"}'],
			"urlMapping": 'https://q.nqminds.com/v1/datasets/SylTjSY_M/data?opts={"limit":32844}',
			"urlNorm": 'https://q.nqminds.com/v1/datasets/BJg-oAZqz/data?opts={"limit":15765120}'
		};

		var tdxconfig3 = {};
		tdxconfig3.credentials = tdxconfig.credentials;
		tdxconfig3.datasetName = "popletPlanning";
		tdxconfig3.basedOnSchema = tdxconfig.basedOnSchema;
		tdxconfig3.targetFolder = tdxconfig.targetFolder;
		tdxconfig3.upsertMode = tdxconfig.upsertMode;
		tdxconfig3.schema = tdxconfig.schema;

		dataPlanTdx(tdxconfig3, year3, urlIn3, function (urlOut3) {
			console.log('the planning normalized data url is: ' + urlOut3);
		});
	});
});
