/**
 * Reads a JSON file to create elasticsearch indexes and data.
 */
var elasticsearch = require('elasticsearch');
var dir = require('node-dir');
var fs = require('fs');
var _ = require('lodash');
var $q = require('q');

function ESJSONLoad(host) {
	var self = this;

	// Connecting
	console.log('Connecting to Elasticsearch @ ' + host);
	self.client = new elasticsearch.Client({
		host: host
	});

	self._deleteIndex = function(index){
		var deferred = $q.defer();

		self.client.indices.delete({index:index}, function(err, resp){
			if(err) deferred.reject(err);
			console.log(index + ': deleted');
			deferred.resolve(resp);
		});

		return deferred.promise;
	}

	self._doesIndexExist = function(index){
		var deferred = $q.defer();
		self.client.indices.exists({index: index}, function(err, resp){
			if(err){
				deferred.reject(err);
			}
			deferred.resolve(resp);
		});
		return deferred.promise;
	}

	self._createIndex = function(index, mapping){
		var deferred = $q.defer();
		self.client.indices.create({index: index, body: mapping }, function(err, resp){
				if(err){
					deferred.reject(err);
				}
				console.log(index + ': mapping created.');
				deferred.resolve(resp);
			});
		return deferred.promise;
	}

	self._bulkLoadData = function(data, indexName, typeName){
		var deferred = $q.defer();
		self.client.bulk({
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

	self._addActionDescription = function(jsonData) {
		var returnThis = [];
		jsonData.forEach(function(entry) {
			returnThis.push({index:{}});
			returnThis.push(entry);
		})
		return returnThis;
	}

};

ESJSONLoad.prototype.loadMapping = function(filePath, indexName){
	var self = this;
	var deferred = $q.defer();
  console.log('filePath', filePath);
	fs.readFile(filePath,
		function(err, content, fileName, next){
			console.log('--------------------');
			console.log('Creating index:' + indexName);
			var mapping = JSON.parse(content);

			self._doesIndexExist(indexName).then(function(resp){
				if(resp){
					return self._deleteIndex(indexName);
				}
			})
			.then(function(resp){
				return self._createIndex(indexName, mapping)
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

ESJSONLoad.prototype.loadData = function(filePath, indexName, typeName){
	var self = this;
	var deferred = $q.defer();

	fs.readFile(filePath,
		function(err, content, fileName, next){
			console.log('--------------------');
			console.log('Loading data for : index:' + indexName);
			var data = self._addActionDescription(JSON.parse(content));
			self._bulkLoadData(data, indexName, typeName).then(function(resp){
			  deferred.resolve();
				next();
			})
			.catch(err, function(err) {
				deferred.reject(err);
			});
		});

	return deferred.promise;
};

module.exports = ESJSONLoad;
