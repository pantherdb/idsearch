const fs = require('fs');
const zlib = require('zlib');
const readline = require('readline');
const axios = require('axios')

// const esearch_url = "http://localhost:9200";
const esearch_url = "https://search-panther-eb636tdq5a6sm5oeu7pi5ldtpe.us-west-1.es.amazonaws.com";
const index_name = "id";
const type_name = "mapping";

const max_docs_per_post = 5000;

if (process.argv.length <= 2) {
    console.log("Usage: " + __filename + " <mapping.gz:file>");
    process.exit(-1);
}


// listIndex();
// deleteIndex(index_name);
createIndex(index_name);


const args = process.argv.slice(2)
console.log("> reading file ", args[0]);


const gzFileInput = fs.createReadStream(args[0]);
const gunzip = zlib.createGunzip();

let lineCount = 0;

let map = new Map();

readline.createInterface({
    input: gunzip,
}).on('line', line => {
    lineCount++;
    // if(lineCount > 10000) {
    //     process.exit(-1);
    // }
    parse(line);
}).on('close', () => {
    addMap(lines);
    console.log(lineCount + " lines read (" + docCount + " docs)");
});

gzFileInput.on('data', function(data) {
    gunzip.write(data);
});
gzFileInput.on('end', function() {
    gunzip.end();
});

let current = undefined;
let lines = undefined;
function parse(line) {
    let split = line.split("\t");
    if(current != id(split[0])) {
        addMap(lines);
        current = id(split[0]);
        lines = [];
    }
    lines.push(split);
}

function id(id) {
    let pos = id.indexOf("-");
    if(pos == -1) return id;
    return id.substring(0, pos);
}

let docCount = 1;
let docs = [];
function addMap(lines) {
    let data = { };

    if(lines && lines.length > 0) {
        let set = new Set();
        for(let line of lines) {
            data[line[1]] = line[2];
            set.add(line[2]);
            set.add(line[0]);
        }
        data['all'] = Array.from(set).join(" ");
        docCount++;
    } else {
        data = undefined;
    }

    if(docs.length >= max_docs_per_post || (!lines || lines.length == 0)) {
        submit();
        docs = [];
    }

    if(data)
        docs.push(data);


//     axios.post(esearch_url + "/" + index_name + "/" + type_name + "/" + docCount++ + "?pretty", JSON.stringify(data), {
//         headers: {
//             'Content-Type': 'application/json',
//         }
//     })    
//     .then(response => {
// //        console.log(response);
//     })
//     .catch(error => {
//         console.error(error);
//     });
}

function submit() {
    console.log("Submit " + docs.length + " docs\t (total: " + docCount + ")");
    let data = "";
    for(let doc of docs) {
        data += "{\"index\": {} }\n" + JSON.stringify(doc) + "\n";
    }
//    console.log("SUBMITTING: ", data);
    axios.post(esearch_url + "/" + index_name + "/" + type_name + "/_bulk" + "?pretty", data, {
        headers: {
            'Content-Type': 'application/json',
        }
    })    
    .then(response => {
//        console.log(response);
    })
    .catch(error => {
        console.error(error);
    });    
}

function deleteIndex(name) {
    axios.delete(esearch_url + "/" + name + "?pretty")
    .then(response => {
        console.log(response);
    })
    .catch(error => {
        console.error(error);
    });
}

function createIndex(name) {
    console.log(esearch_url + "/" + name + "?pretty")
    let data = 
        {
            "settings": {
                "analysis": {
                    "filter": {
                        "autocomplete_filter": {
                            "type": "edge_ngram",
                            "min_gram": 1,
                            "max_gram": 20
                        }
                    },
                    "analyzer": {
                        "autocomplete": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "autocomplete_filter"
                            ]
                        }
                    }
                }
            },
            "mappings": {
                "mapping": {
                    "properties": {
                        "all": {
                            "type": "text",
                            "analyzer": "autocomplete",
                            "search_analyzer": "standard"
                        }
                    }
                }
            }
        };
    axios.put(esearch_url + "/" + name + "?pretty", JSON.stringify(data), {
        headers: {
            'Content-Type': 'application/json',
        }
    })    
    .then(response => {
        console.log(response);
    })
    .catch(error => {
        console.error(error);
    });
}

function listIndex() {
    axios.get(esearch_url + "/_cat/indices?v&pretty")
    .then(response => {
        console.log(response);
    })
    .catch(error => {
        console.error(error);
    });
}