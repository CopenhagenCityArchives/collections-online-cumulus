'use strict';

/**
 * The post processing step that goes through all indexed assets and extends
 * their metadata from their master assets and queue's their sub-assets for
 * the same.
 */

var Q = require('q');
var assetMapping = require('../../asset-mapping.js');
var es = require('collections-online/lib/services/elasticsearch');
var config = require('collections-online/lib/config');

// Given a sub asset's and a master asset's metadata:
// - Extend the metadata of an asset from it's master asset.
// - Re-index the sub asset in Elasticsearch.
function extendAndIndexAsset(state, subAssetMetadata, masterAssetMetadata) {
  console.log('Extending',
    subAssetMetadata.catalog + '-' + subAssetMetadata.id,
    'from',
    masterAssetMetadata.catalog + '-' + masterAssetMetadata.id);

  var extendedMetadata = assetMapping.extendMetadata(subAssetMetadata,
                                                     masterAssetMetadata);

  // Index the extended metadata and return the promise.
  var esID = subAssetMetadata.catalog + '-' + subAssetMetadata.id;
  return es.index({
    index: state.index,
    type: 'asset',
    id: esID,
    body: extendedMetadata
  });
}

function updateMetadataFromRelations(state, assetMetadata) {
  // Extend from it's master assets xor extend it's sub assets.
  // If this is both a master and a sub asset - throw an error
  // for now. This might change.
  if ('related_master_assets' in assetMetadata &&
     'related_sub_assets' in assetMetadata) {
    var masterAssets = assetMetadata.related_master_assets; // jscs:ignore
    var subAssets = assetMetadata.related_sub_assets; // jscs:ignore

    var subAssetIds = subAssets.map(function(subAsset) {
      return assetMetadata.catalog + '-' + subAsset.id;
    });

    // Let's wrap this in a promise, so we can take over any failed
    // promises, when returning it.
    subAssetIds = new Q(subAssetIds);

    if (masterAssets.length === 1) {
      var masterAssetId = assetMetadata.catalog + '-' + masterAssets[0].id;
      // Extend from it's master.
      return es.get({
        index: state.index,
        type: 'asset',
        id: masterAssetId
      }).then(function(response) {
        if (response && response._source) {
          var masterAssetMetadata = response._source;
          return extendAndIndexAsset(state, assetMetadata, masterAssetMetadata);
        } else {
          throw new Error('Expected a non-empty response.');
        }
      }).then(function() {
        // This returns the assets sub asset ids.
        return subAssetIds;
      }, function(reason) {
        console.error('Failed fetching the master asset',
                masterAssetId,
                'referenced by',
                assetMetadata.catalog + '-' + assetMetadata.id,
                'because:',
                reason.message || reason || 'No reason given.');
        return subAssetIds;
      });
    } else if (masterAssets.length > 1) {
      console.log('Skipping inherit metadata from asset',
        assetMetadata.catalog + '-' + assetMetadata.id,
        'with multiple master assets.');
    }

    // Return the sub asset ids.
    return subAssetIds;
  } else {
    console.error('Malformed metadata, expected two fields: ' +
                  'related_master_assets and related_sub_assets: ',
                   assetMetadata);
    return [];
    /*
    throw new Error('Malformed metadata, expected two fields: '+
            'related_master_assets and related_sub_assets');
    */
  }
}

/**
 * The post processor that enherits metadata from master assets on all indexed
 * assets, based on a list of indexed assets ids.
 */
module.exports = function(state) {

  var activity = 'Post-processing to inherit metadata from master assets';
  console.log('\n=== ' + activity + ' ===');

  if(config.cip.indexing.inheritMetadata === false) {
    console.log('Skipping this, because inheritMetadataFromMaster is false');
    return state;
  }

  var assetIdQueue = state.queries.reduce(function(result, query) {
    return result.concat(query.indexedAssetIds);
  }, []);

  console.log('Inheriting metadata from master assets of',
              assetIdQueue.length,
              'assets');

  var handledAssetIds = {};
  // Let's not update the same asset more than this many times.
  var MAX_RECURRANCES = 3;

  var deferred = Q.defer();

  function updateNextAssetFromRelations() {
    if (assetIdQueue.length === 0) {
      deferred.resolve(state);
      return; // Let's not do anything, if the queue is empty.
    }

    // Let's pop one from front of the queue.
    // TODO: Consider using a scroll search from the list of IDs currently in
    // the queue, instead of getting from the elasticsearch API once per asset.
    var assetId = assetIdQueue.shift();

    if (assetId in handledAssetIds &&
        handledAssetIds[assetId] > MAX_RECURRANCES) {
      console.log('Skipping asset',
            assetId,
            'as it was already updated',
            handledAssetIds[assetId],
            'times.');
      // Skip the asset.
      return updateNextAssetFromRelations();
    } else {
      // Fetch the asset metadata related to the asset.
      es.get({
        index: state.index,
        type: 'asset',
        id: assetId
      }).then(function(response) {
        var assetMetadata = response._source;
        if (!assetMetadata) {
          deferred.reject('Got an asset without metadata: ', assetId);
          return;
        }

        var newlyIndexAssetIds = updateMetadataFromRelations(state,
                                                             assetMetadata);

        Q.when(newlyIndexAssetIds, function(newlyIndexAssetIds) {
          // Increment the number of times we've handled this asset.
          if (!(assetId in handledAssetIds)) {
            handledAssetIds[assetId] = 0;
          }
          handledAssetIds[assetId]++;

          if (newlyIndexAssetIds.length > 0) {
            console.log('Adding',
                  newlyIndexAssetIds.length,
                  'assets to the queue, from',
                  assetId);
            // Concatinate the new IDs to the queue.
            assetIdQueue = assetIdQueue.concat(newlyIndexAssetIds);
          }

          // Let's take the next one - if any.
          setTimeout(function() {
            updateNextAssetFromRelations();
          }, 0); // The timeout is to prevent stack size exceeding.
        });
      }, function(reason) {
        console.error('Failed fetching newly indexed asset',
               assetId,
               'reason:',
               reason.message || reason || 'Not given.');
        if (reason.stack) {
          console.error(reason.stack);
        }
        // Next asset please ...
        return updateNextAssetFromRelations();
      });
    }
  }

  if (assetIdQueue.length > 0) {
    // Let's start the madness.
    updateNextAssetFromRelations();
  } else {
    deferred.resolve(state);
  }

  return deferred.promise;
};
