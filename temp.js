const fs = require('fs');

const args = process.argv.slice(2)
const file = args[0];

console.log("> reading file " + file);

 
var contents = fs.readFileSync(file, 'utf8');
var lines = contents.split("\n");

var set = new Set();

for(let line of lines) {
    let split = line.split("\t");
    set.add(split[1]);
    if(set.size == 20000) {
        for(let gp of set) {
            console.log(gp);
        }
        process.exit(0);        
    }
}

