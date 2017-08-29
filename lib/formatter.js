'use strict';
/*jshint esversion: 6, node:true, unused:false, varstmt:true */

const Transform = require('stream').Transform;
const inherits = require('util').inherits;

const extract_title = function(metadata) {
  let title = metadata.title || metadata.path_basename || 'data';
  title = title.replace(/[^A-Za-z0-9]/g,'.').replace(/\.+/,'.').replace(/\.$/,'').replace(/^[0-9\.]+/,'');
  return title;
};

function Formatter(metadata,options) {
  if ( ! (this instanceof Formatter)) {
    return new Formatter(metadata,options);
  }

  if (! options) {
    options = {};
  }
  this.keys = ['filename'];
  this.types = ['string'];
  this.annotations = [];

  this.title = extract_title(metadata);

  this.on('pipe', (src) => {
    this.keys = this.keys.concat(src.keys);
    this.types = this.types.concat(src.types);
    this.annotations = src.annotations;
  });

  options.objectMode = true;
  Transform.call(this, options);
}

inherits(Formatter, Transform);

Formatter.prototype._transform = function _transform(obj, encoding, callback) {

  obj.filename = this.title;
  this.push(obj);
  callback();
};


exports.Formatter = Formatter;