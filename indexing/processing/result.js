'use strict';

/**
 * The processor handling an entire result.
 */

const _ = require('lodash');
const cip = require('../../services/cip');
const config = require('collections-online/lib/config');
const es = require('collections-online/lib/services/elasticsearch');
const Q = require('q');

function AssetIndexingError(catalogAlias, assetId, innerError) {
  this.catalogAlias = catalogAlias;
  this.assetId = assetId;
  this.innerError = innerError;
}

const processAsset = require('./asset');

function saveChangesToCIP(catalogAlias, items) {
  const operation = [
    'metadata',
    'setfieldvalues',
    catalogAlias
  ].join('/');
  // Call the CIP
  return cip.request(operation, {}, {
    items
  });
}

function getResultPage(result, index, count) {
  var options = {
    collection: result.collection_id,
    startindex: index,
    maxreturned: count
  };

  if(config.cip.indexing.additionalFields) {
    options.field = config.cip.indexing.additionalFields;
  }
  return cip.request([
    'metadata',
    'getfieldvalues',
    config.cip.client.constants.layoutAlias
  ], options).then((response) => {
    if (!response ||
        !response.body ||
        typeof(response.body.items) === 'undefined') {
      console.error('Unexpected response:', response);
      throw new Error('The request for field values returned an empty result.');
    } else {
      return response.body.items;
    }
  });
}

/**
 * Process a specific result page, with assets.
 */
function processResultPage(result, context, pageIndex) {
  const collection = context.collection;

  var totalPages = Math.ceil(result.total_rows / context.pageSize);
  const progress = '[' + (pageIndex + 1) + '/' + totalPages + ']';
  console.log(progress + ' Queuing page');

  return getResultPage(result, pageIndex * context.pageSize, context.pageSize)
  .then(assets => {
    console.log(progress + ' Received metadata');
    // Perform a processing of all the assets on the page
    const assetPromises = assets.map(asset => {
      // Clone the context for every asset
      const clonedContext = _.cloneDeep(context);
      // Keep an object of requested changes to the asset in Cumulus
      clonedContext.changes = {};
      // Define a method to persist values in Cumulus
      clonedContext.persist = (name, value) => {
        // Determine the fields UUID in Cumulus
        if(!config.types || !config.types.asset || !config.types.asset.fields) {
          throw new Error('Cannot get field: Missing config.types.asset.fields');
        }
        const field = config.types.asset.fields.find(field => field.short === name);
        if(!field) {
          throw new Error('Field is missing in config.types.asset.fields: ' + name);
        }
        // Update the value in the changes object
        clonedContext.changes[field.cumulusKey] = value;
      };
      // Process each asset
      return processAsset(asset, clonedContext)
      .then(null, err => {
        const msg = 'ERROR processing ' + collection + '-' + asset.id;
        console.error(msg + (err.message && ': ' + err.message));
        return new AssetIndexingError(collection, asset.id, err);
      });
    });

    // Once all asset promises resolves:
    // 1. changes are saved to Cumulus
    // 2. metedata is indexed in elasticsearch
    // in two bulk calls

    return Q.all(assetPromises)
    .then(assets => {
      return {
        errors: assets.filter(a => a instanceof AssetIndexingError),
        assets: assets.filter(a => !(a instanceof AssetIndexingError))
      };
    })
    .then(({assets, errors}) => {
      // Save the changes to the CIP
      const changes = assets.filter(({context}) => {
        // Filter out assets without changes
        return context.changes && Object.keys(context.changes).length > 0;
      }).map(({metadata, context}) => {
        return Object.assign({
          id: metadata.id
        }, context.changes);
      });
      // If changes to the CIP assets is needed, save them
      if(changes.length > 0) {
        // TODO: Consider the response from the CIP - as a change might fail.
        return saveChangesToCIP(context.collection, changes)
        .then(response => {
          if(response.statusCode === 200) {
            console.log(progress + ' Updated', changes.length, 'assets in Cumulus');
            return { assets, errors };
          } else {
            throw new Error('Error updating assets in Cumulus');
          }
        });
      } else {
        return { assets, errors };
      }
    }).then(({assets, errors}) => {
      // Create a list of items for a bulk call, for assets that are not errors.
      const items = [];
      assets.filter(asset => !(asset instanceof AssetIndexingError))
      .forEach(({metadata, context}) => {
        items.push({
          'index' : {
            '_index': context.index,
            '_type': 'asset',
            '_id': metadata.collection + '-' + metadata.id
          }
        });
        items.push(metadata);
      });
      // Perform the bulk operation
      return es.bulk({
        body: items
      }).then(response => {
        const indexedIds = [];
        // Go through the items in the response and replace failures with errors
        // in the assets
        response.items.forEach(item => {
          if(item.index.status >= 200 && item.index.status < 300) {
            indexedIds.push(item.index._id);
          } else {
            // TODO: Consider using the AssetIndexingError instead
            errors.push(new Error('Failed index ' + item.index._id));
          }
        });
        console.log(progress + ' Indexed', indexedIds.length, 'assets in ES');
        // Return the result
        return { errors, indexedIds };
      });
    });
  });
}

function processResultPages(result, context) {
  // Build up a list of parameters for all the pages in the entire result
  const pageIndecies = [];
  for(let p = context.offset; p * context.pageSize < result.total_rows; p++) {
    pageIndecies.push(p);
  }
  // Return a promise of process result pages (evaluated one after another)
  return pageIndecies.reduce((idsAndErrors, pageIndex) => {
    return Q.when(idsAndErrors)
    .then(({allIndexedIds, allErrors}) => {
      return processResultPage(result, context, pageIndex)
      .then(({indexedIds, errors}) => {
        return {
          allIndexedIds: allIndexedIds.concat(indexedIds),
          allErrors: allErrors.concat(errors)
        };
      });
    });
  }, new Q({
    allIndexedIds: [],
    allErrors: []
  }))
  .then(({allIndexedIds, allErrors}) => {
    return {
      indexedIds: allIndexedIds,
      errors: allErrors
    };
  });
}

function processResult(context, query, result) {
  console.log('Processing a result of ' + result.total_rows + ' assets');
  return processResultPages(result, context);
}

module.exports = processResult;
