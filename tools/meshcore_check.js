const fs=require('fs');
const path=require('path');
const readline=require('readline');
const {MeshCoreDecoder,Utils}=require('@michaelhart/meshcore-decoder');
const keys=require('./meshcore_keys.json');
const secrets=(keys.channels||[]).map(c=>c.secretHex).filter(Boolean);
const ks=secrets.length?MeshCoreDecoder.createKeyStore({channelSecrets:secrets}):null;
const keyMap={};(keys.channels||[]).forEach(c=>{if(c.hashByte&&c.name)keyMap[String(c.hashByte).toUpperCase()]=c.name;});
const rfPath=path.join(__dirname,'..','data','rf.ndjson');
let lines=[];
const rl=readline.createInterface({input:fs.createReadStream(rfPath,{encoding:'utf8'}),crlfDelay:Infinity});
rl.on('line',l=>{if(l.trim()){lines.push(l.trim());if(lines.length>2000)lines.shift();}});
rl.on('close',()=>{let group=0,dec=0;const counts={};for(const line of lines){let rec;try{rec=JSON.parse(line);}catch{continue;}
if(!rec.hex)continue;let d;try{d=MeshCoreDecoder.decode(String(rec.hex).toUpperCase(),ks?{keyStore:ks}:undefined);}catch{continue;}
const t=Utils.getPayloadTypeName(d.payloadType);if(t!=='GroupText')continue;group++;const p=d.payload?.decoded;if(!p?.decrypted)continue;
dec++;const ch=String(p.channelHash||'').toUpperCase();const name=keyMap[ch]||ch||'??';counts[name]=(counts[name]||0)+1;}
console.log({tail:lines.length,group,dec,counts});});
