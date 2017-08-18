"use strict";
/*jshint esversion: 6, node:true, unused:false, varstmt:true */

const async = require("async");
const Stream = require("stream");
const temp = require("temp");
const fs = require("fs");
const tableau = require('tableau-sdk');
const KeyExtractor = require("./transforms").KeyExtractor;

temp.track();

function ObjectWriter(stream) {
  this.stream = stream;
}

const not_implemented = function() {
  throw new Error('Not implemented');
}

const no_op = () => {};

const make_tables = function(pairs,types_map) {

};

ObjectWriter.prototype.stringVector = not_implemented;
ObjectWriter.prototype.realVector   = not_implemented;
ObjectWriter.prototype.intVector    = not_implemented;
ObjectWriter.prototype.logicalVector= not_implemented;
ObjectWriter.prototype.listPairs    = not_implemented;
ObjectWriter.prototype.environment  = make_tables;
ObjectWriter.prototype.dataFrame    = make_table;
ObjectWriter.prototype.writeHeader  = no_op;

module.exports = ObjectWriter;