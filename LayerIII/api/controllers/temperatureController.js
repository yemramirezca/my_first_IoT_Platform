'use strict';

var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client ({
	host: 'localhost:9200',
	log: 'trace'
});

exports.list_all_measures = function(req, res) {
	
	client.search({
	index:'viper-test',
	type: 'viper-log',
	body: {
		query:{
			match: {
				body: 'elasticsearch'
			}
		}
	}
	}).then (function(resp) {
		var measures = resp.hits.hits;
		res.json(measures);
	}, function (err) {
		console.trace(err.message);
		res.send(err.message);
	});
};