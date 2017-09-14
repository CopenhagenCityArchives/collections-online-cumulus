'use strict';

const cip = require('../../services/cip');

/**
 * This initializes the CIP client.
 *
 * @param {Object} state The state of which we are about to initialize.
 */

module.exports = state => {
  return cip.initSession();
};
