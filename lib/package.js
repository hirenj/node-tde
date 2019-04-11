'use strict';
/*jshint esversion: 6, node:true, unused:false, varstmt:true */

const archiver = require('archiver');
const fs = require('fs');
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


const create_package = function(filedata, package_info ) {
  let package_prefix = package_info.prefix;
  if (package_prefix) {
    package_prefix = `${package_prefix}.`;
  } else {
    package_prefix = '';
  }
  let tde_filename = `${package_prefix}${filedata.title}`;
  let tde_path = `${tde_filename}.hyper`;
  let tds_path = `${tde_filename}.tds`;
  let tds_string = generate_tds(filedata.title, tde_path);
  let archive = archiver('zip', { store: true });
  archive.append(fs.createReadStream(filedata.path), { name: tde_path });
  archive.append(tds_string,{ name: `${tds_path}` });
  archive.finalize();
  return archive;
};

exports.create_package = create_package;