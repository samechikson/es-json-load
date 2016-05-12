/**
 * Reads files from ./es directories and sub directories to create elasticsearch indexes and data.
 * Index Mapping: Use _mapping.json files to define mapping. Script will use
 */
var elasticsearch = require('elasticsearch');
var dir = require('node-dir');
var fs = require('fs');
var _ = require('lodash');
var $q = require('q');

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

function doesIndexExist(index){

	var deferred = $q.defer();
	client.indices.exists({index: index}, function(err, resp){
		if(err){
			deferred.reject(err);
		}
		deferred.resolve(resp);
	});
	return deferred.promise;
}

function createIndex(index, mapping){
	var deferred = $q.defer();
	client.indices.create({index: index, body: mapping }, function(err, resp){
			if(err){
				deferred.reject(err);
			}
			console.log(index + ': mapping created.');
			deferred.resolve(resp);
		});
	return deferred.promise;
}


function bulkLoadData(data, indexName, typeName){
	var deferred = $q.defer();
	client.bulk({
		index: indexName,
		type: typeName,
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

function loadIndexes(filePath, indexName){

	var deferred = $q.defer();
  console.log('filePath', filePath);
	fs.readFile(filePath,
		function(err, content, fileName, next){
			console.log('--------------------');
			console.log('Creating index:' + indexName);
			var mapping = JSON.parse(content);

			doesIndexExist(indexName).then(function(resp){
				if(resp){
					return deleteIndex(indexName);
				}
			})
			.then(function(resp){
				return createIndex(indexName, mapping)
			})
			.then(function(resp){
				deferred.resolve();
			})
			.catch(function(err){
				deferred.reject(err);
				console.log(err);
			});
		});

	return deferred.promise;
}

function loadData(filePath, indexName, typeName){

	var deferred = $q.defer();

	fs.readFile(filePath,
		function(err, content, fileName, next){
			console.log('--------------------');
			console.log('Loading data for : index:' + indexName);
			var data = addActionDescription(JSON.parse(content));
			bulkLoadData(data, indexName, typeName).then(function(resp){
			  deferred.resolve();
				next();
			})
			.catch(err, function(err) {
				deferred.reject(err);
			});
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

module.exports.mapping = function(mappingFilePath, indexName) {
	loadIndexes(mappingFilePath, indexName)
	.then(function(resp1, resp2){
		console.log('Done');
		process.exit(0);
	})
	.catch(function(err){
		throw err;
		process.exit(1);
	})
}

module.exports.data = function(dataFilePath, indexName, typeName) {
	loadData(dataFilePath, indexName, typeName)
	.then(function(){
		console.log('Done');
		process.exit(0);
	})
	.catch(function(err){
		throw err;
		process.exit(1);
	})
}
