'use strict';
module.exports = function(app) {
	var temperature = require ('../controllers/temperatureController')
	
	//temperature Routes
	app.route('/temperatures')
		.get(temperature.list_all_measures);
	
};