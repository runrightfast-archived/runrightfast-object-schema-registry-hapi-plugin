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
 * {
 * TODO:document the plugin's options 
 * logLevel : 'WARN'					// OPTIONAL -default is 'WARN'
 * }   
 * </code>
 * 
 */
module.exports.register = function(plugin, options, next) {
	'use strict';

	var lodash = require('lodash');
	var Hoek = require('hoek');
	var assert = Hoek.assert;
	var extend = require('extend');

	var logging = require('runrightfast-commons').logging;
	var pkgInfo = require('./pkgInfo');
	var logger = logging.getLogger(pkgInfo.name);

	var config = {
		logLevel : 'WARN'
	/* TODO: default config goes here */
	};

	var validateConfig = function(config) {
		/* TODO: config validation goes here */
	};

	var registerPlugin = function(config) {
		/* TODO: finish plugin registration here, e.g, add server routes, etc */
	};

	extend(true, config, options);
	logging.setLogLevel(logger, config.logLevel);
	if (logger.isDebugEnabled()) {
		logger.debug(config);
	}
	validateConfig(config);
	registerPlugin(config);

	next();

};