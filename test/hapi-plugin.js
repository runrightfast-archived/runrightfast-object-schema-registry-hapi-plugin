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
'use strict';

var expect = require('chai').expect;
var Hapi = require('hapi');
var lodash = require('lodash');
var when = require('when');

var ObjectSchemaRegistryDatabase = require('runrightfast-elastic-object-schema-registry').ObjectSchemaRegistryDatabase;
var ObjectSchema = require('runrightfast-validator').validatorDomain.ObjectSchema;

var ElasticSearchClient = require('runrightfast-elasticsearch').ElasticSearchClient;
var ejs = new ElasticSearchClient({
	host: 'localhost',
	port: 9200
}).ejs;

var joi = require('joi');
var types = joi.types;

describe('ObjectSchema Registry Proxy Hapi Plugin', function() {

	var database = new ObjectSchemaRegistryDatabase({
		ejs: ejs,
		index: 'objectschema',
		type: 'objectschema',
		entityConstructor: ObjectSchema,
		logLevel: 'DEBUG'
	});

	var idsToDelete = [];

	before(function(done) {
		database.database.deleteIndex()
			.then(function(result) {
				console.log('DELETED INDEX: ' + JSON.stringify(result, undefined, 2));
				database.database.createIndex({})
					.then(function() {
						console.log('CREATED INDEX: ' + JSON.stringify(result, undefined, 2));
						database.setMapping().
						then(function(result) {
							console.log('SET MAPPING: ' + JSON.stringify(result, undefined, 2));
							done();
						}, done);
					}, done);
			}, done);
	});

	afterEach(function(done) {
		database.deleteObjectSchemas(idsToDelete).then(function() {
			idsToDelete = [];
			database.database.refreshIndex().then(function() {
				done();
			});
		}, function(error) {
			console.error(JSON.stringify(error, undefined, 2));
			done(error.error);
		});
	});

	it('can be added as a plugin to hapi', function(done) {

		var options = {
			logLevel: 'DEBUG',
			elasticSearch: {
				host: 'localhost'
			}
		};

		var server = new Hapi.Server();
		server.pack.require('../', options, function(err) {
			console.log('err: ' + err);
			expect( !! err).to.equal(false);
			done();
		});
	});

	it('GET /v1/resources/objectschemas - returns the first 10 ObjectSchemas sorted by namespace and version', function(done) {
		var objectSchemas = [];
		var i;
		for (i = 0; i < 15; i++) {
			objectSchemas.push(new ObjectSchema({
				namespace: 'ns://runrightfast.co/runrightfast-api-gateway',
				version: '1.0.' + i,
				description: 'runrightfast-api-gateway config'
			}));
		}

		var promises = lodash.foldl(objectSchemas, function(promises, objectSchema) {
			idsToDelete.push(objectSchema.id);
			promises.push(database.createObjectSchema(objectSchema));
			return promises;
		}, []);

		when(when.all(promises),
			function() {
				when(database.database.refreshIndex(),
					function() {
						var options = {
							logLevel: 'DEBUG',
							elasticSearch: {
								host: 'localhost'
							}
						};

						var server = new Hapi.Server();
						server.pack.require('../', options, function(err) {
							console.log('err: ' + err);
							try {
								expect( !! err).to.equal(false);

								var injectOptions = {
									method: 'GET',
									url: '/v1/resources/objectschemas'
								};

								server.inject(injectOptions, function(response) {
									var responseObject = JSON.parse(response.payload);
									console.log('response: ' + JSON.stringify(responseObject, undefined, 2));
									expect(response.statusCode).to.equal(200);
									expect(responseObject.data.hits.length).to.equal(10);

									lodash.forEach(responseObject.data.hits, function(hit) {
										// verifies that returned objects are valid ObjectSchemas
										console.log(JSON.stringify(new ObjectSchema(hit), undefined, 2));
									});
									done();
								});
							} catch (err2) {
								done(err2);
							}

						});
					},
					done
				);
			},
			done
		);
	});

	it('GET /v1/resources/objectschemas - returns specified fields for the first 10 ObjectSchemas sorted by namespace and version', function(done) {
		var objectSchemas = [];
		var i;
		for (i = 0; i < 15; i++) {
			objectSchemas.push(new ObjectSchema({
				namespace: 'ns://runrightfast.co/runrightfast-api-gateway',
				version: '1.0.' + i,
				description: 'runrightfast-api-gateway config'
			}));
		}

		var promises = lodash.foldl(objectSchemas, function(promises, objectSchema) {
			idsToDelete.push(objectSchema.id);
			promises.push(database.createObjectSchema(objectSchema));
			return promises;
		}, []);

		when(when.all(promises),
			function() {
				when(database.database.refreshIndex(),
					function() {
						var options = {
							logLevel: 'DEBUG',
							elasticSearch: {
								host: 'localhost'
							}
						};

						var server = new Hapi.Server();
						server.pack.require('../', options, function(err) {
							console.log('err: ' + err);
							try {
								expect( !! err).to.equal(false);

								var injectOptions = {
									method: 'GET',
									url: '/v1/resources/objectschemas?dataFields=id,namespace,version'
								};

								server.inject(injectOptions, function(response) {
									var responseObject = JSON.parse(response.payload);
									console.log('response: ' + JSON.stringify(responseObject, undefined, 2));
									expect(response.statusCode).to.equal(200);
									expect(responseObject.data.hits.length).to.equal(10);

									lodash.forEach(responseObject.data.hits, function(hit) {
										joi.validate(hit, {
											id: types.String().required(),
											namespace: types.String().required(),
											version: types.String().required(),
										});
									});
									done();
								});
							} catch (err2) {
								done(err2);
							}

						});
					},
					done
				);
			},
			done
		);
	});

	it('GET /v1/resources/objectschemas - can return the raw response from elasticsearch', function(done) {
		var objectSchemas = [];
		var i;
		for (i = 0; i < 15; i++) {
			objectSchemas.push(new ObjectSchema({
				namespace: 'ns://runrightfast.co/runrightfast-api-gateway',
				version: '1.0.' + i,
				description: 'runrightfast-api-gateway config'
			}));
		}

		var promises = lodash.foldl(objectSchemas, function(promises, objectSchema) {
			idsToDelete.push(objectSchema.id);
			promises.push(database.createObjectSchema(objectSchema));
			return promises;
		}, []);

		when(when.all(promises),
			function() {
				when(database.database.refreshIndex(),
					function() {
						var options = {
							logLevel: 'DEBUG',
							elasticSearch: {
								host: 'localhost'
							}
						};

						var server = new Hapi.Server();
						server.pack.require('../', options, function(err) {
							console.log('err: ' + err);
							try {
								expect( !! err).to.equal(false);

								var injectOptions = {
									method: 'GET',
									url: '/v1/resources/objectschemas?dataFields=id,namespace,version&raw=true'
								};

								server.inject(injectOptions, function(response) {
									var responseObject = JSON.parse(response.payload);
									console.log('response: ' + JSON.stringify(responseObject, undefined, 2));
									expect(response.statusCode).to.equal(200);
									expect(responseObject.data.hits.hits.length).to.equal(10);

									lodash.forEach(responseObject.data.hits.hits, function(hit) {
										joi.validate(hit.fields, {
											id: types.String().required(),
											namespace: types.String().required(),
											version: types.String().required(),
										});
									});
									done();
								});
							} catch (err2) {
								done(err2);
							}

						});
					},
					done
				);
			},
			done
		);
	});

	it('GET /v1/resources/objectschemas - can apply sorting against a single field', function(done) {
		var objectSchemas = [];
		var i;
		for (i = 0; i < 15; i++) {
			objectSchemas.push(new ObjectSchema({
				namespace: 'ns://runrightfast.co/runrightfast-api-gateway',
				version: '1.0.' + i,
				description: 'runrightfast-api-gateway config'
			}));
		}

		var promises = lodash.foldl(objectSchemas, function(promises, objectSchema) {
			idsToDelete.push(objectSchema.id);
			promises.push(database.createObjectSchema(objectSchema));
			return promises;
		}, []);

		when(when.all(promises),
			function() {
				when(database.database.refreshIndex(),
					function() {
						var options = {
							logLevel: 'DEBUG',
							elasticSearch: {
								host: 'localhost'
							}
						};

						var server = new Hapi.Server();
						server.pack.require('../', options, function(err) {
							console.log('err: ' + err);
							try {
								expect( !! err).to.equal(false);

								var injectOptions = {
									method: 'GET',
									url: '/v1/resources/objectschemas?sort=version|desc'
								};

								server.inject(injectOptions, function(response) {
									var responseObject = JSON.parse(response.payload);
									console.log('response: ' + JSON.stringify(responseObject, undefined, 2));
									expect(response.statusCode).to.equal(200);
									expect(responseObject.data.hits.length).to.equal(10);

									lodash.forEach(responseObject.data.hits, function(hit) {
										// verifies that returned objects are valid ObjectSchemas
										console.log(JSON.stringify(new ObjectSchema(hit), undefined, 2));
									});
									done();
								});
							} catch (err2) {
								done(err2);
							}

						});
					},
					done
				);
			},
			done
		);
	});

	it('GET /v1/resources/objectschemas - can apply sorting against a mulitple fields', function(done) {
		var objectSchemas = [];
		var i;
		for (i = 0; i < 15; i++) {
			objectSchemas.push(new ObjectSchema({
				namespace: 'ns://runrightfast.co/runrightfast-api-gateway',
				version: '1.0.' + i,
				description: 'runrightfast-api-gateway config - ' + (i % 2)
			}));
		}

		var promises = lodash.foldl(objectSchemas, function(promises, objectSchema) {
			idsToDelete.push(objectSchema.id);
			promises.push(database.createObjectSchema(objectSchema));
			return promises;
		}, []);

		when(when.all(promises),
			function() {
				when(database.database.refreshIndex(),
					function() {
						var options = {
							logLevel: 'DEBUG',
							elasticSearch: {
								host: 'localhost'
							}
						};

						var server = new Hapi.Server();
						server.pack.require('../', options, function(err) {
							console.log('err: ' + err);
							try {
								expect( !! err).to.equal(false);

								var injectOptions = {
									method: 'GET',
									url: '/v1/resources/objectschemas?sort=namespace|desc,version|desc&version=true'
								};

								server.inject(injectOptions, function(response) {
									var responseObject = JSON.parse(response.payload);
									console.log('response: ' + JSON.stringify(responseObject, undefined, 2));
									try {
										expect(response.statusCode).to.equal(200);
										expect(responseObject.data.hits.length).to.equal(10);

										var objectSchema1, objectSchema2;
										lodash.forEach(responseObject.data.hits, function(hit) {
											// verifies that returned objects are valid ObjectSchemas
											objectSchema1 = new ObjectSchema(hit.data);
											if (objectSchema2) {
												expect(objectSchema1.version).to.be.lte(objectSchema2.version);
											}
											objectSchema2 = objectSchema1;
										});
										done();
									} catch (err3) {
										done(err3);
									}
								});
							} catch (err2) {
								done(err2);
							}

						});
					},
					done
				);
			},
			done
		);
	});

	it('POST /v1/resources/objectschemas - creates a new ObjectSchema', function(done) {
		var schema = new ObjectSchema({
			namespace: 'ns://runrightfast.co/couchbase',
			version: '1.0.0',
			description: 'Couchbase config schema'
		});

		var serverOptions = {
			logLevel: 'DEBUG',
			elasticSearch: {
				host: 'localhost'
			}
		};

		var server = new Hapi.Server();
		server.pack.require('../', serverOptions, function(err) {
			console.log('err: ' + err);
			try {
				expect( !! err).to.equal(false);

				var createOptions = {
					method: 'POST',
					url: '/v1/resources/objectschemas',
					payload: schema
				};

				server.inject(createOptions, function(response) {
					var responseObject = JSON.parse(response.payload);
					console.log('response: ' + JSON.stringify(responseObject, undefined, 2));

					try {
						expect(response.statusCode).to.equal(201);
						idsToDelete.push(responseObject.data.id);
						expect(lodash.isString(responseObject.data.id)).to.equal(true);
						done();
					} catch (err) {
						done(err);
					}
				});
			} catch (serverErr) {
				done(serverErr);
			}
		});

	});

	it('POST /v1/resources/objectschemas - can return the raw elasticsearch response', function(done) {
		var schema = new ObjectSchema({
			namespace: 'ns://runrightfast.co/couchbase',
			version: '1.0.0',
			description: 'Couchbase config schema'
		});

		var serverOptions = {
			logLevel: 'DEBUG',
			elasticSearch: {
				host: 'localhost'
			}
		};

		var server = new Hapi.Server();
		server.pack.require('../', serverOptions, function(err) {
			console.log('err: ' + err);
			try {
				expect( !! err).to.equal(false);

				var createOptions = {
					method: 'POST',
					url: '/v1/resources/objectschemas?raw=true',
					payload: schema
				};

				server.inject(createOptions, function(response) {
					var responseObject = JSON.parse(response.payload);
					console.log('response: ' + JSON.stringify(responseObject, undefined, 2));

					try {
						expect(response.statusCode).to.equal(201);
						idsToDelete.push(responseObject.data._id);
						expect(lodash.isString(responseObject.data._id)).to.equal(true);
						expect(responseObject.data._version).to.equal(1);
						done();
					} catch (err) {
						done(err);
					}
				});
			} catch (serverErr) {
				done(serverErr);
			}
		});

	});

	it('POST /v1/resources/objectschemas - will fail if the ObjectSchema is invalid', function(done) {
		var schema = {
			namespace: 'INVALID NAMSPACE',
			version: '1.0.0',
			description: 'Couchbase config schema'
		};

		var serverOptions = {
			logLevel: 'DEBUG',
			elasticSearch: {
				host: 'localhost'
			}
		};

		var server = new Hapi.Server();
		server.pack.require('../', serverOptions, function(err) {
			console.log('err: ' + err);
			try {
				expect( !! err).to.equal(false);

				var createOptions = {
					method: 'POST',
					url: '/v1/resources/objectschemas',
					payload: schema
				};

				server.inject(createOptions, function(response) {
					var responseObject = JSON.parse(response.payload);
					console.log('response: ' + JSON.stringify(responseObject, undefined, 2));

					try {
						expect(response.statusCode).to.equal(400);
						done();
					} catch (err) {
						done(err);
					}
				});
			} catch (serverErr) {
				done(serverErr);
			}
		});

	});

	it('POST /v1/resources/objectschemas - creating a new ObjectSchema with an existing namespace and version will fail', function(done) {
		var schema = new ObjectSchema({
			namespace: 'ns://runrightfast.co/couchbase',
			version: '1.0.0',
			description: 'Couchbase config schema'
		});

		var serverOptions = {
			logLevel: 'DEBUG',
			elasticSearch: {
				host: 'localhost'
			}
		};

		var server = new Hapi.Server();
		server.pack.require('../', serverOptions, function(err) {
			console.log('err: ' + err);
			try {
				expect( !! err).to.equal(false);

				var createOptions = {
					method: 'POST',
					url: '/v1/resources/objectschemas',
					payload: schema
				};

				server.inject(createOptions, function(response) {
					var responseObject = JSON.parse(response.payload);
					console.log('response: ' + JSON.stringify(responseObject, undefined, 2));

					try {
						expect(response.statusCode).to.equal(201);
						idsToDelete.push(responseObject.data.id);

						server.inject(createOptions, function(response) {
							var responseObject = JSON.parse(response.payload);
							console.log('response: ' + JSON.stringify(responseObject, undefined, 2));

							try {
								expect(response.statusCode).to.equal(409);
								done();
							} catch (err) {
								done(err);
							}
						});

					} catch (err) {
						done(err);
					}
				});
			} catch (serverErr) {
				done(serverErr);
			}
		});

	});

	it('PUT /v1/resources/objectschemas/{id}/{version} - replaces an ObjectSchema', function(done) {
		var schema = new ObjectSchema({
			namespace: 'ns://runrightfast.co/couchbase',
			version: '1.0.0',
			description: 'Couchbase config schema'
		});

		var serverOptions = {
			logLevel: 'DEBUG',
			elasticSearch: {
				host: 'localhost'
			}
		};

		var server = new Hapi.Server();
		server.pack.require('../', serverOptions, function(err) {
			console.log('err: ' + err);
			try {
				expect( !! err).to.equal(false);

				var createOptions = {
					method: 'POST',
					url: '/v1/resources/objectschemas',
					payload: schema
				};

				server.inject(createOptions, function(response) {
					var responseObject = JSON.parse(response.payload);
					console.log('response: ' + JSON.stringify(responseObject, undefined, 2));

					try {
						expect(response.statusCode).to.equal(201);
						idsToDelete.push(responseObject.data.id);
						expect(lodash.isString(responseObject.data.id)).to.equal(true);
						schema.id = responseObject.data.id;

						var replaceOptions = {
							method: 'PUT',
							url: '/v1/resources/objectschemas/' + responseObject.data.id + '/1',
							payload: schema
						};
						server.inject(replaceOptions, function(response) {
							var responseObject = JSON.parse(response.payload);
							console.log('response: ' + JSON.stringify(responseObject, undefined, 2));

							try {
								expect(response.statusCode).to.equal(200);
								expect(lodash.isString(responseObject.data.id)).to.equal(true);
								expect(responseObject.data.version).to.equal(2);
								done();
							} catch (err) {
								done(err);
							}
						});

					} catch (err) {
						done(err);
					}
				});
			} catch (serverErr) {
				done(serverErr);
			}
		});

	});

	it('GET /v1/resources/objectschemas/{id}', function(done) {
		var schema = new ObjectSchema({
			namespace: 'ns://runrightfast.co/couchbase',
			version: '1.0.0',
			description: 'Couchbase config schema'
		});

		var serverOptions = {
			logLevel: 'DEBUG',
			elasticSearch: {
				host: 'localhost'
			}
		};

		var server = new Hapi.Server();
		server.pack.require('../', serverOptions, function(err) {
			console.log('err: ' + err);
			try {
				expect( !! err).to.equal(false);

				var createOptions = {
					method: 'POST',
					url: '/v1/resources/objectschemas',
					payload: schema
				};

				server.inject(createOptions, function(response) {
					var responseObject = JSON.parse(response.payload);
					console.log('response: ' + JSON.stringify(responseObject, undefined, 2));

					try {
						expect(response.statusCode).to.equal(201);
						idsToDelete.push(responseObject.data.id);
						expect(lodash.isString(responseObject.data.id)).to.equal(true);
						schema.id = responseObject.data.id;

						var getOptions = {
							method: 'GET',
							url: '/v1/resources/objectschemas/' + responseObject.data.id
						};
						server.inject(getOptions, function(response) {
							var responseObject = JSON.parse(response.payload);
							console.log('response: ' + JSON.stringify(responseObject, undefined, 2));

							try {
								expect(response.statusCode).to.equal(200);
								var retrievedObjectSchema = new ObjectSchema(responseObject.data);
								expect(retrievedObjectSchema.id).to.equal(responseObject.data.id);
								done();
							} catch (err) {
								done(err);
							}
						});

					} catch (err) {
						done(err);
					}
				});
			} catch (serverErr) {
				done(serverErr);
			}
		});

	});

	it('DELETE /v1/resources/objectschemas/{id}', function(done) {
		var schema = new ObjectSchema({
			namespace: 'ns://runrightfast.co/couchbase',
			version: '1.0.0',
			description: 'Couchbase config schema'
		});

		var serverOptions = {
			logLevel: 'DEBUG',
			elasticSearch: {
				host: 'localhost'
			}
		};

		var server = new Hapi.Server();
		server.pack.require('../', serverOptions, function(err) {
			console.log('err: ' + err);
			try {
				expect( !! err).to.equal(false);

				var createOptions = {
					method: 'POST',
					url: '/v1/resources/objectschemas',
					payload: schema
				};

				server.inject(createOptions, function(response) {
					var responseObject = JSON.parse(response.payload);
					console.log('response: ' + JSON.stringify(responseObject, undefined, 2));

					try {
						expect(response.statusCode).to.equal(201);
						idsToDelete.push(responseObject.data.id);
						expect(lodash.isString(responseObject.data.id)).to.equal(true);
						schema.id = responseObject.data.id;

						var getOptions = {
							method: 'GET',
							url: '/v1/resources/objectschemas/' + responseObject.data.id
						};
						server.inject(getOptions, function(response) {
							var responseObject = JSON.parse(response.payload);
							console.log('response: ' + JSON.stringify(responseObject, undefined, 2));

							try {
								expect(response.statusCode).to.equal(200);
								var retrievedObjectSchema = new ObjectSchema(responseObject.data);
								expect(retrievedObjectSchema.id).to.equal(responseObject.data.id);

								var deleteOptions = {
									method: 'DELETE',
									url: '/v1/resources/objectschemas/' + responseObject.data.id
								};
								server.inject(deleteOptions, function(response) {
									var responseObject = JSON.parse(response.payload);
									console.log('response: ' + JSON.stringify(responseObject, undefined, 2));

									try {
										expect(response.statusCode).to.equal(200);
										expect(responseObject.data.found).to.equal(true);
										expect(retrievedObjectSchema.id).to.equal(responseObject.data.id);
										expect(responseObject.data.version).to.equal(2);
										done();
									} catch (err) {
										done(err);
									}
								});

							} catch (err) {
								done(err);
							}
						});

					} catch (err) {
						done(err);
					}
				});
			} catch (serverErr) {
				done(serverErr);
			}
		});

	});

});