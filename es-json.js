/**
 * Reads files from ./es directories and sub directories to create elasticsearch indexes and data.
 * Index Mapping: Use _mapping.json files to define mapping. Script will use
 */
var elasticsearch = require('elasticsearch');
var config = require('../config');
var dir = require('node-dir');
var fs = require('fs');
var _ = require('lodash');
var $q = require('q');
var path = require('path');

var DATA_DIR = path.resolve(__dirname, './es');

// Connecting
console.log("Connecting to Elasticsearch @ " + config.elasticsearch.url);
config.elasticsearch.host = config.elasticsearch.url;
var client = new elasticsearch.Client(config.elasticsearch);

//load indexes
function getIndexName(fileName){
	var justFileName = _.last(fileName.split("/"));
	var indexName = justFileName.replace("_mapping.json", "");
	return indexName;
}

function getIndexTypeName(indexName){
	return _.last(indexName.split("_"));
}

function deleteIndex(index){
	var deferred = $q.defer();

	client.indices.delete({index:index}, function(err, resp){
		if(err) deferred.reject(err);
		console.log(index + ": deleted");
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
			console.log(index + ": mapping created.");
			deferred.resolve(resp);
		});
	return deferred.promise;
}


function bulkLoadData(data){
	var deferred = $q.defer();

	client.bulk({body:data}, function(err, resp){
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
			var index = getIndexName(fileName);
			var type = getIndexTypeName(index);
			console.log("--------------------");
			console.log("Creating index:" + index + " type:" + type);
			var mapping = JSON.parse(content);

			isExistsIndex(index).then(function(resp){
				if(resp){
					return deleteIndex(index);
				}
			})
			.then(function(resp){
				return createIndex(index,type,mapping)
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
			var index = getIndexName(fileName);
			var type = getIndexTypeName(index);
			console.log("--------------------");
			console.log("Loading data for : index:" + index);
			var data = content.toString().split("\n");
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

// Executing scripts

loadIndexes()
	.then(loadData)
	.then(function(resp1, resp2){
		console.log("Done");
		process.exit(0);
	})
	.catch(function(err){
		throw err;
		process.exit(1);
	})
