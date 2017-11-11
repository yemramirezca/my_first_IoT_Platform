var express = require('express'),
	app = express(),
	port = process.env.PORT || 3000;

var routes = require ('./api/routes/platformRoutes');
routes(app);
	
app.listen(port);

console.log('IOT platform started on port: ' + port);