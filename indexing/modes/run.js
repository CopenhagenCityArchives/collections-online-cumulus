'use strict';

/**
 * Running the indexing procedure in whatever mode the state suggests.
 */

var Q = require('q');
var processQuery = require('../processing/query');
var config = require('collections-online/lib/config');

const POST_PROCESSING_STEPS = [
  require('../post-processing/delete-removed-assets'),
  require('../post-processing/clear-index')
];

module.exports = function(state) {
  var mode = require('./' + state.mode);

  state.queries = mode.generateQueries(state);

  // Add any indexing restrictions from the configuration.
  state.queries.forEach((q) => {
    if (config.cip.indexing.restriction) {
      q.query = '(' + q.query + ') AND ' + config.cip.indexing.restriction;
    }
  });

  console.log('\n=== Starting to process ===');

  // TODO: Consider if the two new Q(state)s need to be wrapped in promises.

  return state.queries.reduce(function(promise, query) {
    return promise.then(function(state) {
      query.indexedAssetIds = [];
      query.assetExceptions = [];
      return processQuery(state, query);
    });
  }, new Q(state)).then(function(state) {
    console.log('Finished processing!');
    return POST_PROCESSING_STEPS.reduce(Q.when, new Q(state));
  });
};
