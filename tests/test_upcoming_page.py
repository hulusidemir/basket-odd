import shutil
import subprocess
import unittest
from pathlib import Path


@unittest.skipUnless(shutil.which("node"), "Node.js is required for page behavior checks")
class UpcomingPageTests(unittest.TestCase):
    def test_missing_markets_errors_and_request_ordering(self):
        root = Path(__file__).resolve().parents[1]
        script = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const html = fs.readFileSync('templates/upcoming_matches.html', 'utf8');
const source = html.match(/<script>([\s\S]*?)<\/script>/)[1];
const elements = new Map();
function element(id) {
  if (!elements.has(id)) {
    const classes = new Set();
    elements.set(id, {value:'', textContent:'', innerHTML:'', hidden:false,
      addEventListener(){}, setAttribute(){}, classList:{
        add(c){classes.add(c)}, remove(c){classes.delete(c)},
        toggle(c,on){on ? classes.add(c) : classes.delete(c)}, contains(c){return classes.has(c)}
      }});
  }
  return elements.get(id);
}
const rows = [
  {match_id:'missing',match_name:'Missing',opening_total:null,prematch_total:null},
  {match_id:'empty',match_name:'Empty',opening_total:'',prematch_total:''},
  {match_id:'prematch',match_name:'Prematch',opening_total:null,prematch_total:160},
  {match_id:'opening',match_name:'Opening',opening_total:170,prematch_total:null},
];
const response = data => ({ok:true,json:async()=>data});
const tick = () => new Promise(resolve => setImmediate(resolve));
const context = vm.createContext({
  document:{getElementById:element,querySelectorAll:()=>[]},
  fetch:async()=>response({matches:rows,running:false}),
  setTimeout:()=>1,clearTimeout(){},confirm:()=>true,URL,
});
(async()=>{
  vm.runInContext(source, context);
  await tick();
  assert.equal(Number(element('statLines').textContent),2);
  assert.equal(vm.runInContext('fmt(null)',context),'–');
  assert.equal(vm.runInContext("fmt('')",context),'–');
  assert.equal(vm.runInContext('fmt(160)',context),'160.0');

  element('searchInput').value='absent';context.render();
  assert.equal(element('emptyTitle').textContent,'Filtreye uygun maç yok');
  assert.match(element('status').textContent,/0 maç/);
  element('searchInput').value='';

  context.fetch=async()=>response({matches:rows,running:false,report:{status:'failed'},last_fetch_error:'Kaynak hatası'});
  await context.load();
  assert.match(element('status').textContent,/Kaynak hatası/);
  assert.ok(element('status').classList.contains('error'));
  context.fetch=async()=>response({matches:rows,running:false,report:{status:'partial'},stale:true,stale_row_count:2});
  await context.load();
  assert.match(element('status').textContent,/Güncelleme eksik/);
  assert.match(element('status').textContent,/2 kaydın verisi eski/);

  let finishOlder;
  context.fetch=()=>new Promise(resolve=>{finishOlder=resolve});
  const older=context.load();
  context.fetch=async()=>response({matches:[rows[0]],running:false});
  await context.load();finishOlder(response({matches:rows,running:true}));await older;
  assert.equal(Number(element('statTotal').textContent),1);
  assert.equal(element('fetchBtn').disabled,false);

  let finishAction;let calls=0;
  context.fetch=()=>{calls++;return new Promise(resolve=>{finishAction=resolve})};
  const button={dataset:{matchId:"quote'id"}};
  const first=context.action(button,'follow');
  await context.action(button,'follow');assert.equal(calls,1);
  finishAction({ok:false,json:async()=>({error:'Takip hatası'})});await first;
  assert.match(element('status').textContent,/Takip hatası/);
  assert.ok(element('status').classList.contains('error'));
  assert.equal(element('clearBtn').disabled,false);

  context.fetch=async()=>({ok:false,json:async()=>({error:'Temizleme hatası'})});
  await context.clearUpcomingMatches();
  assert.match(element('status').textContent,/Temizleme hatası/);
  assert.equal(element('clearBtn').disabled,false);

  context.fetch=async()=>response({matches:rows,running:true});await context.load();
  assert.equal(element('clearBtn').disabled,true);
  assert.equal(element('fetchBtn').disabled,true);
  context.fetch=async()=>{throw new Error('Bağlantı kesildi')};await context.load();
  assert.equal(element('fetchBtn').disabled,true);
  assert.match(element('status').textContent,/Bağlantı kesildi/);
})().catch(error=>{console.error(error);process.exitCode=1});
"""
        result = subprocess.run(
            [shutil.which("node"), "-"], input=script, text=True,
            capture_output=True, cwd=root, timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
