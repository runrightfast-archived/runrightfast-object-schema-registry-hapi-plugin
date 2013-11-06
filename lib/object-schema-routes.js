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


module.exports = function(objectSchemaDatabase,log){
	'use strict';

	var extend = require('extend');

	var apiVersion = 'v1';
	var resource = 'objectschemas';

	var path = function(resourcePath){
		var basePath = '/data/' + apiVersion + '/' + resource;
		if(resourcePath){
			return basePath + resourcePath;
		}
		return basePath;
	};

	var defaultGetObjectSchemasParams = {
		limit : 10,
		offset : 0,
		dataOnly : false,
		links : true,
		actions : true
	};

	var getObjectSchemas = function getObjectSchemas(request){
		var params = {
			limit : request.query.limit,
			offset : request.query.offset,
			sort : request.query.sort,
			dataFields : request.query.dataFields,
			dataOnly : request.query.dataOnly,
			actions : request.query.actions,
			links : request.query.links
		};

		extend(params,defaultGetObjectSchemasParams);

		if(log.isDebugEnabled()){
			log.debug('getObjectSchemas(): params: ' + JSON.stringify(params,undefined,2));
		}

		var searchResults = {};
		request.reply(searchResults);
	};
	
	return [
		{
			method: 'GET',
			path : path(),			
			config: {				
				description : 'Used to page through ObjectSchemas. By default, the first 10 are returned sorted by namespace and version.',
				tags : ['objectschemas','paging'],
				handler : getObjectSchemas
			}
		}
	];
};