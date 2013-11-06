/**
 * Copyright [2013] [runrightfast.co]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * @param options
 * 
 * <code>
 * logLevel				OPTIONAL - String - default is 'WARN',
 * elasticSearch : {    REQUIRED - elasticsearch connection config
	host				REQUIRED - String
	port				OPTIONAL - Number - default is 9200
	index				OPTIONAL - String - the name of the elasticsearch index where the ObjectSchemas are stored - default is 'objectschema'
	type 				OPTIONAL - String - the name of the elasticsearch type - default is 'objectschema'
   }
 * 
 * </code>
 * 
 */
module.exports.register = function(plugin, options, next) {
	'use strict';
	
	var extend = require('extend');
	var joi = require('joi');
	var types = joi.types;

	var logging = require('runrightfast-commons').logging;
	var pkgInfo = require('./pkgInfo');
	var log = logging.getLogger(pkgInfo.name);

	var config = {
		logLevel : 'WARN',
		elasticSearch: {
			port : 9200,
			index : 'objectschema',
			type : 'objectschema'
		}
	};

	var validateConfig = function(config) {
		var configSchema = {
			logLevel: types.String(),
			elasticSearch : types.Object({
				host: types.String().required(),
				port : types.Number().min(1),
				index : types.String(),
				type : types.String()
			})
		};

		var err = joi.validate(config,configSchema);
		if(err){
			throw err;
		}
	};

	var registerPlugin = function(config) {
		var ObjectSchemaRegistryDatabase = require('runrightfast-elastic-object-schema-registry').ObjectSchemaRegistryDatabase;
		var ObjectSchema = require('runrightfast-validator').validatorDomain.ObjectSchema;
		var ElasticSearchClient = require('runrightfast-elasticsearch').ElasticSearchClient;
		var ejs = new ElasticSearchClient({
			host : config.elasticSearch.host,
			port : config.elasticSearch.port
		}).ejs;
		var database = new ObjectSchemaRegistryDatabase({
			ejs : ejs,
			index : config.elasticSearch.index,
			type : config.elasticSearch.type,
			entityConstructor : ObjectSchema,
			logLevel : config.logLevel
		}); 

		plugin.route(require('./object-schema-routes')(database,log));
	};

	extend(true, config, options);
	logging.setLogLevel(log, config.logLevel);
	if (log.isDebugEnabled()) {
		log.debug(config);
	}
	validateConfig(config);
	registerPlugin(config);

	next();

};