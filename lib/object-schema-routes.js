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
	var querystring = require('querystring');

	var apiVersion = '/v1';
	var resource = 'objectschemas';

	var path = function(resourcePath){
		var routePath =  apiVersion + '/resources/' + resource;
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


	var getObjectSchemas = function getObjectSchemas(_path,request){
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

			if(params.sort){
				var tokens;
				findAllOptions.multiFieldSort = params.sort.split(',').map(function(sortField){
					tokens = sortField.split('|');
					return {
						field : tokens[0],
						descending : (tokens.length > 1 ? tokens[1] === 'desc' : false)
					};
				});
			}

			if(log.isDebugEnabled()){
				log.debug('getObjectSchemas(): toSearchOptions(): ' + JSON.stringify(findAllOptions,undefined,2));
			}

			return findAllOptions;
		};

		var searchResults = {
			meta : new ResponseMetaData(_path)
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
					if(params.version){
						if(params.dataFields){						
							searchResults.data = {							
								hits: lodash.map(results.hits.hits,function(hit){
									return {
										version : hit._version,
										data : hit.fields
									};
								})
							};
						}else{
							searchResults.data = {							
								hits: lodash.map(results.hits.hits,function(hit){
									return {
										version : hit._version,
										data : hit._source
									};
								})
							};
						}					
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
				}

				var addActions = function(){					
					if(params.actions){							
						searchResults.actions = [
							{
								name : 'create',
								title : 'Create Object Schema',
								method : 'POST',						
								auth : ['hawk'],
								requestPayloadSchema : {
									namespace : 'ns://runrightfast.co/ObjectSchema',
									version : '1.0.0',
									type : 'ObjectSchema'
								},
								responsePayloadSchema : {
									namespace : 'ns://runrightfast.co/ObjectSchema',
									version : '1.0.0',
									type : 'ObjectSchemaId'
								},
								href : path()
							},
							{
								name : 'count',
								title : 'Total Number of Object Schemas',
								method : 'GET',						
								auth : ['hawk'],
								requestPayloadSchema : {
									namespace : 'ns://runrightfast.co/ObjectSchema',
									version : '1.0.0',
									type : 'ObjectSchema'
								},
								responsePayloadSchema : {
									namespace : 'ns://runrightfast.co/ObjectSchema',
									version : '1.0.0',
									type : 'ObjectSchemaCount'
								},
								href : path('/count')
							}
						];
					}										
				};

				var addLinks = function(){
					if(params.links){
						var linkQueryString = '?' + querystring.stringify(request.query);
						searchResults.links = [
							{
								rel: 'self',
								href : path() + linkQueryString,
								title : 'Get Object Schemas',
								auth : ['hawk']								
							}
						];

						if(searchResults.meta.searchResult.total > 0){
							linkQueryString = lodash.clone(request.query);
							linkQueryString.offset = 0;
							linkQueryString = '?' + querystring.stringify(linkQueryString);
							searchResults.links.push({
								rel: 'firstPage',
								href : path() + linkQueryString,
								title : 'Get Object Schemas - First Page',
								auth : ['hawk']								
							});

							if(searchResults.meta.searchResult.total > params.limit){
								linkQueryString = lodash.clone(request.query);
								linkQueryString.offset = searchResults.meta.searchResult.total - params.limit;
								if(linkQueryString.offset < (params.offset + params.limit)){
									linkQueryString.offset = (params.offset + params.limit);
								}
								linkQueryString = '?' + querystring.stringify(linkQueryString);
								searchResults.links.push({
									rel: 'lastPage',
									href : path() + linkQueryString,
									title : 'Get Object Schemas - Last Page',
									auth : ['hawk']								
								});
							}

							if(searchResults.meta.searchResult.count < searchResults.meta.searchResult.total){
								linkQueryString = lodash.clone(request.query);
								linkQueryString.offset = (params.offset + params.limit);
								if(linkQueryString.offset < searchResults.meta.searchResult.total){
									linkQueryString = '?' + querystring.stringify(linkQueryString);
									searchResults.links.push({
										rel: 'nextPage',
										href : path() + linkQueryString,
										title : 'Get Object Schemas - Next Page',
										auth : ['hawk']								
									});
								}
							}

							if(searchResults.meta.searchResult.offset > 0){
								linkQueryString = lodash.clone(request.query);
								linkQueryString.offset = params.offset - params.limit;
								if(linkQueryString.offset > 0){
									linkQueryString = '?' + querystring.stringify(linkQueryString);
									searchResults.links.push({
										rel: 'prevPage',
										href : path() + linkQueryString,
										title : 'Get Object Schemas - Previous Page',
										auth : ['hawk']								
									});
								}
							}
						}
					}
				};			

				addActions();
				addLinks();
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