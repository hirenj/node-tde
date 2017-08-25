'use strict';
const ObjectWriter = require('..');
const chai = require('chai');
const expect = chai.expect;
const assert = chai.assert;
const tempfile = require('temp');

const fs = require('fs');
const zlib = require('zlib');
const Stream = require('stream');

const objects = [ { 'x' : 2, 'y' : 'ab', 'z': false},
                  { 'x' : 4, 'y' : 'ac', 'z': false},
                  { 'x' : 8, 'y' : 'ad', 'z': true},
                  { 'x' : 16, 'y' : 'ae', 'z': true},
                  { 'x' : 32, 'y' : 'af', 'z': true}
                ];


tempfile.track();

const Readable = require('stream').Readable;
const util = require('util');

function ObjectStream(max,options) {
  if (! (this instanceof ObjectStream)) return new ObjectStream(max,options);
  if (! options) options = {};
  options.objectMode = true;
  this.counter = 0;
  this.total = max;
  Readable.call(this, options);
}

util.inherits(ObjectStream, Readable);

ObjectStream.prototype._read = function read() {
  var self = this;
  if (typeof this.counter == 'undefined' || this.counter < self.total) {
    self.push(objects[0]);
    this.counter = this.counter || 0;
    this.counter += 1;
    return;
  }
  if (objects.length > 0) {
    self.push(objects.shift());
  } else {
    self.push(null);
    self.emit('close');
  }
};

describe('Writing a stream', function() {
  it('Writes data out from an object stream',function(done){
    this.timeout(20000);
    let vec_length = 5e04;

    let writer = tempfile.createWriteStream();

    let object_writer = new ObjectWriter(writer);
    console.log('Writing to ',writer.path);
    object_writer.environment( {'frame' : new ObjectStream(vec_length)},
                          {'frame': { 'type': 'dataframe', 'keys': ['x', 'y','z'], 'types' : ['real', 'string', 'logical'] }}
                          ).then( () => object_writer.finish() )
                          .then( () => writer.path )
    .then( () => done() )
    .catch( done );
  });

  it('Writes data out from an object stream including nulls',function(done){
    objects.push({ 'x' : null, 'y' : null, 'z': null});
    this.timeout(20000);
    let vec_length = 5e01;
    let writer = tempfile.createWriteStream();

    let object_writer = new ObjectWriter(writer);
    let path = object_writer.stream.path;
    object_writer.writeHeader();
    object_writer.environment( {'frame' : new ObjectStream(vec_length)},
                          {'frame': { 'type': 'dataframe', 'keys': ['x', 'y','z'], 'types' : ['real', 'string', 'logical'] }}
                          ).then( () => object_writer.finish() )
                          .then( () => writer.path )
    .then( () => done() )
    .catch( done );
  });

});
