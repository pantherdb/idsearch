const fs = require('fs');
const zlib = require('zlib');
const readline = require('readline');
const axios = require('axios')
var sleep = require('sleep');

// const esearch_url = "http://localhost:9200";
// const esearch_url = "https://search-panther-eb636tdq5a6sm5oeu7pi5ldtpe.us-west-1.es.amazonaws.com";

if (process.argv.length <= 4) {
    console.log("Usage: " + __filename + " <elasticsearch:ip> <mapping.gz:file> <taxon:id> [optional: <create_index:true/false>]\n");
    process.exit(-1);
}


const args = process.argv.slice(2)
const esearch_url = args[0];
const file = args[1];
const taxon = args[2];

let willCreateIndex = false;
if (args.length == 4) {
    willCreateIndex = (args[3] == "true");
}

console.log("> elastic search server: " + esearch_url);

const index_name = "id";
const type_name = "mapping";
const template_name = "t_search_key_taxon";
const max_docs_per_post = 1000;
const wait_after_submit = 1;


if (willCreateIndex) {
    createIndex(index_name);
    createTemplate(template_name);
    sleep.sleep(2);
}


console.log("> taxon defined: " + taxon);
console.log("> reading file " + file);

const gzFileInput = fs.createReadStream(file);
const gunzip = zlib.createGunzip();

let lineCount = 0;
let docCount = 1;

// batch of docs to be submitted
let docs = [];

readline.createInterface({
    input: gunzip,
}).on('line', line => {
    lineCount++;
    parse(line);
}).on('close', () => {
    submit();
    console.log(lineCount + " lines read (" + docCount + " docs)");
});

gzFileInput.on('data', function (data) {
    gunzip.write(data);
});
gzFileInput.on('end', function () {
    gunzip.end();
});

let current = undefined;
let lines = undefined;
function parse(line) {
    let split = line.split("\t");
    if (current != id(split[0])) {
        // add the taxon based on user parameter
        if (lines && lines.length > 0) {
            // I don't think we have the UniProtKB-AC field yet ?
            lines.push([lines[lines.length - 1][0], "UniProtKB-AC", lines[lines.length - 1][0]]);

            // manually add the NCBI_TaxID if missing
            let found = false;
            for (let line of lines) {
                if (line[1] == "NCBI_TaxID")
                    found = true;
            }
            if (!found) {
                console.log("NCBI_TaxID was not present for ", lines);
                lines.push([lines[lines.length - 1][0], "NCBI_TaxID", taxon]);
            }
        }
        addMap(lines);
        current = id(split[0]);
        lines = [];
    }
    lines.push(split);
}

/**
 * Trim any isoform in the id (in the form -X)
 * @param {*} id 
 */
function id(id) {
    let pos = id.indexOf("-");
    if (pos == -1) return id;
    return id.substring(0, pos);
}

/**
 * Note: could set the document ID to the uniprot AC number, I think it would work and give consistency to search acrosss multiple versions
 * @param {*} lines 
 */
function addMap(lines) {
    let data = {};

    if (lines && lines.length > 0) {
        let set = new Set();
        for (let line of lines) {
            data[line[1]] = line[2];
            set.add(line[2]);
            set.add(line[0]);
        }
        // data['_id'] = data["UniProtKB-AC"];
        data['all'] = Array.from(set).join(" ");
        data['any'] = Array.from(set);
        docCount++;
    } else {
        data = undefined;
    }

    if (docs.length >= max_docs_per_post) {
        submit();
        docs = [];
    }

    if (data)
        docs.push(data);

}

function submit() {
    if (docs.length == 0) return;

    console.log("Submit " + docs.length + " docs\t (total: " + docCount + ")");
    let data = "";
    for (let doc of docs) {
        data += "{\"index\": {\"_id\": \"" + doc["UniProtKB-AC"] + "\"} }\n" + JSON.stringify(doc) + "\n";
    }
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
            console.error("DATA: ", data);
        });
    sleep.sleep(wait_after_submit);
}

function deleteIndex(name) {
    axios.delete(esearch_url + "/" + name + "?pretty")
        .then(response => {
            console.log("Index <" + name + "> was successfully deleted");
        })
        .catch(error => {
            console.error(error);
        });
}

function createIndex(name) {
    let data =
        {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 0,

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
                            "search_analyzer": "standard",
                            "norms": {
                                "enabled": false
                            }
                        },
                        "any": {
                            "type": "text",
                            "norms": {
                                "enabled": false
                            }
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
            console.log("Index <" + name + "> was successfully created");
        })
        .catch(error => {
            console.error(error);
        });
}


function createTemplate(template_name) {
    let data =
    {
        "script": {
            "lang": "mustache",
            "source": {
                "size": 1,
                "_source": { "excludes": ["all", "any"] },
                
                "query": {
                    "bool": {                    
                        "must": {
                            "term": {
                            "any": "{{query_string}}"
                            }
                        },
                            
                        "should": {
                            "prefix": {
                            "UniProtKB-ID": "{{query_string}}"
                            }
                        },
                    
                        "filter": {
                            "term": { "NCBI_TaxID": "{{taxon_id}}" }
                        }
                    }
                }
    
            }
        }
    };
    
        axios.put(esearch_url + "/_scripts/" + template_name + "?pretty", JSON.stringify(data), {
        headers: {
            'Content-Type': 'application/json',
        }
    })
        .then(response => {
            console.log("Query template <" + template_name + "> was successfully created");
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