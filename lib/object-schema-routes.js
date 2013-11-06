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
	var uuid = require('runrightfast-commons').uuid;
	var hapi = require('hapi');
	var types = hapi.types;
	var when = require('when');
	var lodash = require('lodash');

	var apiVersion = 'v1';
	var resource = 'objectschemas';

	var path = function(resourcePath){
		var routePath = '/data/' + apiVersion + '/' + resource;
		if(resourcePath){
			routePath += resourcePath;
		}
		if(log.isDebugEnabled()){
			log.debug('path(): ' + routePath);
		}
		return routePath;
	};

	var ResponseMetaData = function(path){
		this.path = path;
		this.id = uuid();
		this.receivedOn = new Date();
	};

	ResponseMetaData.prototype.stopTimer = function(){
		this.processingTime = Date.now() - this.receivedOn .getTime();
	};


	var getObjectSchemas = function getObjectSchemas(path,request){
		var defaultGetObjectSchemasParams = {
			limit : 10,
			offset : 0,
			dataOnly : false,
			links : true,
			actions : true,
			version : false,
			raw : false
		};

		var getParams = function(){
			var params = {
				limit : request.query.limit,
				offset : request.query.offset,								
				dataFields : request.query.dataFields,
				timeout: request.query.timeout,
				version: request.query.version,
				dataOnly : request.query.dataOnly,
				actions : request.query.actions,
				links : request.query.links,
				sort : request.query.sort,
				raw : request.query.raw				
			};

			params = extend(defaultGetObjectSchemasParams,params);

			if(log.isDebugEnabled()){
				log.debug('getObjectSchemas(): request.query: ' + JSON.stringify(request.query,undefined,2));
				log.debug('getObjectSchemas(): getParams(): ' + JSON.stringify(params,undefined,2));
			}

			return params;
		};

		var toSearchOptions = function(params){
			var findAllOptions = {
				pageSize : params.limit,
				from : params.offset,
				returnFields : (params.dataFields ? params.dataFields.split(',') : undefined),
				timeout : params.timeout,
				version : params.version
			};

			//TODO: actions: create,count
			//TODO: links: nextPage,previousPage,firstPage,lastPage
			//TODO: sort

			if(log.isDebugEnabled()){
				log.debug('getObjectSchemas(): toSearchOptions(): ' + JSON.stringify(findAllOptions,undefined,2));
			}

			return findAllOptions;
		};

		var searchResults = {
			meta : new ResponseMetaData(path)
		};
		var params = getParams();
		var findAllOptions = toSearchOptions(params);
		
		when(objectSchemaDatabase.database.findAll(findAllOptions),
			function(results){				
				searchResults.meta.searchResult = {
					limit: params.limit,
					offset: params.offset,
					total : results.hits.total,
					count: results.hits.hits.length
				};
				if(params.raw){
					searchResults.data = results;
				}else{
					if(params.dataFields){						
						searchResults.data = {
							hits: lodash.pluck(results.hits.hits,'fields')
						};
					}else{
						searchResults.data = {
							hits: lodash.pluck(results.hits.hits,'_source')
						};
					}					
				}
				request.reply(searchResults);
				searchResults.meta.stopTimer();				
			},
			function(err){
				searchResults.error = {
					code: 500,
					developerMessage : err.toString(),
					userMessage : 'Failed to retrieve object schemas'
				};
				request.reply(searchResults).code(searchResults.error.code);
				searchResults.meta.stopTimer();	
			}
		);
		

	};
	
	return [
		{
			method: 'GET',
			path : path(),			
			config: {				
				description : 'Used to page through ObjectSchemas. By default, the first 10 are returned sorted by updatedOn in descending order.',
				tags : ['objectschemas','paging'],
				validate: {
					query : {
						limit : types.Number(),
						offset : types.Number(),
						sort : types.String(),
						dataFields : types.String(),
						dataOnly : types.Boolean(),
						actions : types.Boolean(),
						links : types.Boolean(),
						timeout : types.Number(),
						version : types.Boolean(),
						raw : types.Boolean()
					}
				},
				handler : getObjectSchemas.bind(null,path())
			}
		}
	];
};