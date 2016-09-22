'use strict';

var union = require('lodash/union');
var config = require('collections-online/lib/config');
var cip = require('../../services/cip');

const TAGS_VISION_FIELD = '{6864395c-c433-2148-8b05-56edf606d4d4}';

function saveVisionTags(metadata, tags) {
  var values = {};
  values[TAGS_VISION_FIELD] = tags.join(',');
  return cip.setFieldValues(metadata.catalog, metadata.id, 'web', values);
}

module.exports = function(state, metadata) {
  // Let's save some cost and bandwidth and not analyze the asset unless
  // explicitly told. As in run only if one of the indexVison args
  // are specified.
  var runForced = state.indexVisionTagsForce;
  var runDefault = state.indexVisionTags && !metadata.tags_vision;
  var reviewState = metadata.review_state ? metadata.review_state.id : null;
  var isPublished = reviewState === 3 || reviewState === 4;

  if ((runForced || runDefault) && isPublished) {
    // increment the counter so we can keep track on when to pause and slow down
    state.indexVisionTagsPauseCounter++;

    // Still here. Let's grab the image directly from Cumulus.
    var url = config.cip.baseURL + '/preview/thumbnail/';
    url += metadata.catalog + '/' + metadata.id;

    // Loading here to prevent circular dependency.
    var motif = require('collections-online/lib/controllers/motif-tagging');

    return motif.fetchSuggestions(url, state.indexVisionTagsAPIFilter)
      .then(function(tags) {
        // Convert tags to a comma seperated string
        // Save the tags to Cumulus
        var tagsIsArray = !!metadata.tags_vision &&
          typeof metadata.tags_vision === 'object' &&
          metadata.tags_vision.constructor === Array;

        if (tagsIsArray === false) {
          metadata.tags_vision = [];
        }
        var oldTagsSize = metadata.tags_vision.length;
        var tagsUnion = union(metadata.tags_vision, tags);
        var diffSize = tagsUnion.length - oldTagsSize;

        // If no new tags was added, we don't save
        if (diffSize === 0) {
          return metadata;
        }

        console.log('Derived', diffSize, 'new tags, using AI.');

        return saveVisionTags(metadata, tagsUnion).then(function(response) {
          if (response.statusCode !== 200) {
            throw new Error('Failed to set the field values');
          }
          metadata.tags_vision = tagsUnion;
          return metadata;
        });
      });
  }
  return metadata;
};
