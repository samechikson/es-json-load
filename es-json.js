/**
 * Reads files from ./es directories and sub directories to create elasticsearch indexes and data.
 * Index Mapping: Use _mapping.json files to define mapping. Script will use
 */
var elasticsearch = require('elasticsearch');
var dir = require('node-dir');
var fs = require('fs');
var _ = require('lodash');
var $q = require('q');
var path = require('path');

var DATA_DIR = path.resolve(__dirname, './es');
var INDEX_NAME = 'facebook';
var TYPE_NAME = 'nodes'
var config = {
	host: 'http://localhost:9200'
}

// Connecting
console.log('Connecting to Elasticsearch @ ' + config.host);
var client = new elasticsearch.Client(config);

//load indexes
function getIndexName(fileName){
	var justFileName = _.last(fileName.split('/'));
	var indexName = justFileName.replace('_mapping.json', '');
	return indexName;
}

function getIndexTypeName(indexName){
	return _.last(indexName.split('_'));
}

function deleteIndex(index){
	var deferred = $q.defer();

	client.indices.delete({index:index}, function(err, resp){
		if(err) deferred.reject(err);
		console.log(index + ': deleted');
		deferred.resolve(resp);
	});

	return deferred.promise;
}

function isExistsIndex(index){

	var deferred = $q.defer();
	client.indices.exists({index: index}, function(err, resp){
		if(err){
			deferred.reject(err);
		}
		deferred.resolve(resp);
	});
	return deferred.promise;
}

function createIndex(index, type, mapping){
	var deferred = $q.defer();
	client.indices.create({index: index, type: type, body: mapping }, function(err, resp){
			if(err){
				deferred.reject(err);
			}
			console.log(index + ': mapping created.');
			deferred.resolve(resp);
		});
	return deferred.promise;
}


function bulkLoadData(data){
	var deferred = $q.defer();

	client.bulk({
		index: INDEX_NAME,
		type: TYPE_NAME,
		body:data
	}, function(err, resp){
		if(err){
				deferred.reject(err);
		}
		console.log('Finished loading data.');
		deferred.resolve(resp);
	});

	return deferred.promise;
}

function loadIndexes(){

	var deferred = $q.defer();

	dir.readFiles(DATA_DIR, {
			match: /_mapping.json/
		},
		function(err, content, fileName, next){
			console.log('--------------------');
			console.log('Creating index:' + INDEX_NAME + ' type:' + TYPE_NAME);
			var mapping = JSON.parse(content);

			isExistsIndex(INDEX_NAME).then(function(resp){
				if(resp){
					return deleteIndex(INDEX_NAME);
				}
			})
			.then(function(resp){
				return createIndex(INDEX_NAME,TYPE_NAME,mapping)
			})
			.then(function(resp){
					next();
			})
			.catch(function(err){
				console.log(err);
			});
		},
		function(err, files){
			if(err){
				deferred.reject(err);
			}
			deferred.resolve(files);
		});

	return deferred.promise;
}

function loadData(){

	var deferred = $q.defer();

	dir.readFiles(DATA_DIR, {
			match: /_data.json/
		},
		function(err, content, fileName, next){
			console.log('--------------------');
			console.log('Loading data for : index:' + INDEX_NAME);
			var data = addActionDescription(JSON.parse(content));
			bulkLoadData(data).then(function(resp){
				next();
			});
		},
		function(err, files){
			if(err){
				deferred.reject(err);
			}
			deferred.resolve(files);
		});

	return deferred.promise;
};

function addActionDescription(jsonData) {
	var returnThis = [];
	jsonData.forEach(function(entry) {
		returnThis.push({index:{}});
		returnThis.push(entry);
	})
	return returnThis;
}

// Executing scripts

loadIndexes()
.then(loadData)
.then(function(resp1, resp2){
	console.log('Done');
	process.exit(0);
})
.catch(function(err){
	throw err;
	process.exit(1);
})
