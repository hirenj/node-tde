"use strict";
/*jshint esversion: 6, node:true, unused:false, varstmt:true */

const Transform = require("stream").Transform;
const inherits = require("util").inherits;
const Null = Symbol("Null value");

function KeyExtractor(key,options) {
  if ( ! (this instanceof KeyExtractor)) {
    return new KeyExtractor(key,options);
  }

  if (! options) {
    options = {};
  }
  options.objectMode = true;
  console.log("Extracting key",key);
  this.key = key;
  Transform.call(this, options);
}

inherits(KeyExtractor, Transform);

KeyExtractor.prototype._transform = function _transform(obj, encoding, callback) {
  this.push(obj[this.key] || Null);
  callback();
};

function ObjectCounter(options) {
  if ( ! (this instanceof ObjectCounter)) {
    return new ObjectCounter(options);
  }

  if (! options) {
    options = {};
  }
  options.objectMode = true;
  this.total = 0;
  Transform.call(this, options);
}

inherits(ObjectCounter, Transform);

ObjectCounter.prototype._transform = function _transform(obj, encoding, callback) {
  this.total += 1;
  this.push(obj);
  callback();
};


exports.ObjectCounter = ObjectCounter;
exports.KeyExtractor = KeyExtractor;