'use strict';

const writer = require('.');
const fs = require('fs');

let output = fs.createWriteStream('output.tde');
let objwriter = new writer(output);
let dataframe = [{x: 1, y: 'a', z: true }];
let env_done = objwriter.environment({'frame' : dataframe },{'frame' : { 'type': 'dataframe', 'keys' : ['x','y','z'], 'types' : ['int','string','logical'] }});
env_done.then( () => objwriter.finish() );