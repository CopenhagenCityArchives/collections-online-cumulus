'use strict';

var cipCategories = require('../../cip-categories.js');

/**
 * This initializes the CIP categories.
 *
 * @param {Object} state The state of which we are about to initialize.
 */

function initializeCipCategories(state) {
  console.log('Initializing CIP categories');

  state.categories = {};

  return cipCategories.loadCategories()
  .then(function(result) {
    // The categories pr catalog has been fetched from Cumulus.
    for (var i = 0; i < result.length; ++i) {
      if (result[i]) {
        state.categories[result[i].id] = result[i];
      } else {
        console.error('Skipping a catalog that turned out to be undefined.');
      }
    }

    var categoriesCount = Object.keys(state.categories).length;
    console.log('Loaded categories for', categoriesCount, 'catalogs');
    return state;
  });
}

module.exports = initializeCipCategories;
