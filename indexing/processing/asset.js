'use strict';

/**
 * The processor handling a single asset.
 */

var _ = require('lodash');
var Q = require('q');
var es = require('collections-online/lib/services/elasticsearch');
var config = require('collections-online/lib/config');

function AssetIndexingError(catalogAlias, assetId, innerError) {
  this.catalogAlias = catalogAlias;
  this.assetId = assetId;
  this.innerError = innerError;
}

// This list of transformations are a list of functions that takes two
// arguments (cip_client, metadata) and returns a mutated metadata, which
// is passed on to the next function in the list.
var METADATA_TRANSFORMATIONS = [
  require('../transformations/field-names'),
  require('../transformations/empty-title'),
  require('../transformations/dates'),
  require('../transformations/date-intervals'),
  require('../transformations/categories-and-suggest'),
  require('../transformations/relations'),
  require('../transformations/dimensions'),
  require('../transformations/is-searchable'),
  require('../transformations/latitude-longitude'),
  require('../transformations/split-tags'),
  require('../transformations/category-tags'),
  require('../transformations/vision-tags'),
  require('../transformations/tag-hierarchy')
];

function transformMetadata(state, metadata, transformations) {
  return transformations.reduce(function(metadata, transformation) {
    return Q.when(metadata).then(function(metadata) {
      return transformation(state, metadata);
    });
  }, new Q(metadata));
}

function processAsset(state, metadata, transformations) {
  if (!state) {
    throw new Error('A state is needed to have initialized clients.');
  }
  if (!metadata) {
    throw new Error('Metadata is needed to know what to transform.');
  }
  //console.log('Processing an asset.');
  // Use all transformations by default.
  if (typeof(transformations) === 'undefined') {
    if (config.assetTransformations) {
      transformations = config.assetTransformations.map(function(path){
        return require(path);
      });
    } else {
      transformations = METADATA_TRANSFORMATIONS;
    }
  }
  // Perform additional transformations and index the result.
  return transformMetadata(state, metadata, transformations)
    .then(function(metadata) {
      return es.index({
        index: state.index,
        type: 'asset',
        id: metadata.catalog + '-' + metadata.id,
        body: metadata
      });
    })
    .then(function(resp) {
      console.log('Successfully indexed ' + resp._id);
      return resp._id;
    }, function(err) {
      console.error('An error occured! Asset: ' + metadata.catalog + '-' +
                    metadata.id, err.stack || err.message || err);
      return new AssetIndexingError(metadata.catalog, metadata.id, err);
    });
}

module.exports = processAsset;
module.exports.METADATA_TRANSFORMATIONS = METADATA_TRANSFORMATIONS;
