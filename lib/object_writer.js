"use strict";
/*jshint esversion: 6, node:true, unused:false, varstmt:true */

const async = require("async");
const Stream = require("stream");
const temp = require("temp");
const fs = require("fs");
const tableau = require('tableau-sdk');
// const KeyExtractor = require("./transforms").KeyExtractor;
const TYPE_LOOKUPS = {
  'string' : 'string',
  'real' : 'float',
  'int' : 'int',
  'logical' : 'bool'
};

temp.track();

function ObjectWriter(stream) {
  this.stream = stream;
  this.tempfile = '/tmp/somefile.tde';
}

const not_implemented = function() {
  throw new Error('Not implemented');
}

const no_op = () => {};

const lookup_types = function(type) {
  return TYPE_LOOKUPS[type];
};

const make_table_definition = function(types) {
  if ( types.type !== 'dataframe') {
    throw new Error('Unsupported');
  }
  let keys = types.keys;
  let col_types = types.types.map( lookup_types );
  let columns = [];
  keys.forEach( (key,idx) => {
    columns.push( { id: key, dataType: col_types[idx] });
  });
  return { columns: columns };
};

const handle_data = function(data,extract) {
  data.forEach(item => {
    extract.insert(item);
  });
  extract.finished = Promise.resolve();
};

const extract_finished_promise = function(extract) {
  return extract.finished;
}

const make_table = () => {};

const make_tables = function(pairs,types_map) {
  let extracts = Object.keys(pairs).map( (id) => {
    let data = pairs[id];
    let type = types_map[id];
    let definition = make_table_definition(type);
    definition.id = id;
    definition.defaultAlias = id;
    console.log(definition);
    let extract = new tableau(this.tempfile, definition);

    // Maybe this section should be serialised?
    handle_data(data,extract);
    return extract;
  });
  return Promise.all(extracts.map(extract_finished_promise)).then( () => {
    extracts.forEach( extract => extract.close() );
  });
};

ObjectWriter.prototype.stringVector = not_implemented;
ObjectWriter.prototype.realVector   = not_implemented;
ObjectWriter.prototype.intVector    = not_implemented;
ObjectWriter.prototype.logicalVector= not_implemented;
ObjectWriter.prototype.listPairs    = not_implemented;
ObjectWriter.prototype.environment  = make_tables;
ObjectWriter.prototype.dataFrame    = make_table;
ObjectWriter.prototype.writeHeader  = no_op;

ObjectWriter.prototype.finish = function() {
  let self = this;
  return new Promise(function(resolve,reject) {
    self.stream.on("close",resolve);
    self.stream.on("finish",resolve);
    self.stream.on("error",reject);
    fs.createReadStream(self.tempfile).pipe(self.stream);
  });
};

module.exports = ObjectWriter;