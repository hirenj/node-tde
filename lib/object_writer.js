'use strict';
/*jshint esversion: 6, node:true, unused:false, varstmt:true */

const async = require('async');
const Stream = require('stream');
const temp = require('temp');
const fs = require('fs');
const tableau = require('tableau-sdk');
const convertxml = require('./json2xml.js').convert;
const transform = require('jsonpath-object-transform');


const tds_template = {
  'datasource' : {
    '@formatted-name' : '$.(@.name)',
    '@inline' : 'true',
    '@xmlns:user' : 'http://www.tableausoftware.com/xml/user',
    'connection' : {
      '@class' : 'federated',
      'named-connections' : { 'named-connection' : { '@name' : '$.(@.name+\'-connection\')' , 'connection' : { '@class' : 'dataengine', '@dbname' : '$.(@.filename)', '@password' : ''  } } },
      'relation' : {
        '@connection' : '$.(@.name+\'-connection\')',
        '@name' : '$.(@.name)',
        '@table' : '[Extract].[Extract]',
        '@type' : 'table'
      }
    }
  }
};

const generate_tds = function(setname,filename) {
  return convertxml(transform({name: setname, filename: filename},tds_template));
};

const TYPE_LOOKUPS = {
  'string' : 'string',
  'real' : 'float',
  'int' : 'int',
  'logical' : 'bool'
};

temp.track();

function ObjectWriter(stream) {
  this.stream = stream;
  this.tempfile = temp.path({suffix: '.tde'});
  this.temp_tds = temp.path({suffix: '.tds'});
}

const not_implemented = function() {
  throw new Error('Not implemented');
};

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

const promise_array = function( items, func ) {
  if (items.length == 0) {
    return Promise.resolve();
  }
  return func(items.shift()).then( () => {
    return promise_array(items,func);
  });
};

const handle_data_stream = function(object_stream,extract) {
  object_stream.on('data', obj => extract.insert(obj) );
  return new Promise((resolve,reject) => {
    object_stream.on('error',reject);
    object_stream.on('end', resolve);
  });
};

const handle_data = function(item) {
  let data = item.data;
  let extract = item.extract;
  if (data instanceof Stream && data._readableState.objectMode) {
    return handle_data_stream(data,extract);
  }
  data.forEach(item => {
    extract.insert(item);
  });

  return Promise.resolve();
};

const make_table = function(data,keys,types,options) {
  let definition = make_table_definition({type: 'dataframe', types: types, keys: keys});
  definition.id = 'Extract';// uuid 'Extract';
  let extract = new tableau(this.tempfile, definition);
  return promise_array( [{ data: data, extract: extract }], handle_data ).then( () => {
    extract.close();
  });
};

const make_single_table = function(params) {
  return make_table.bind(this)(params.data,params.keys,params.types,{});
};

const make_tables = function(pairs,types_map) {
  let extracts = [];
  let data_to_insert = Object.keys(pairs).map( (id) => {
    let data = pairs[id];
    let type = types_map[id];
    return { data: data, keys: type.keys, types: type.types };
  });
  return promise_array( data_to_insert, make_single_table.bind(this) );
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
    self.stream.on('close',resolve);
    self.stream.on('finish',resolve);
    self.stream.on('error',reject);
    fs.createReadStream(self.tempfile).pipe(self.stream);
  });
};

module.exports = ObjectWriter;