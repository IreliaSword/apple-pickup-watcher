import assert from 'node:assert/strict';
import test from 'node:test';
import {parseRows, blockedTitle} from '../src-tauri/src/proxy5-browser.mjs';
const row=(overrides={})=>Object.assign(['8.8.8.8','8080','HTTP','anonymous','fixture','24 毫秒','100%','5 分钟'],overrides);
test('Proxy5 displayed rows are candidates, with no website API access',()=>{
  assert.deepEqual(parseRows([row()]),[{url:'http://8.8.8.8:8080',source:'proxy5',uptime:100,latencyMs:24}]);
  assert.equal(parseRows([row({2:'SOCKS5 HTTP'})])[0].url,'socks5h://8.8.8.8:8080');
});
test('Proxy5 parser does not silently relax required displayed filters',()=>{
  for(const changed of [{5:'51 ms'},{6:'89%'},{7:'11 min'},{7:'1 hour'},{5:'unknown'},{6:'unknown'},{1:'70000'},{2:'SOCKS4'},{2:'HTTPS'}])
    assert.equal(parseRows([row(changed)]).length,0,JSON.stringify(changed));
  assert.equal(parseRows([['incomplete']]).length,0);
});
test('source challenge titles are not interpreted as an empty healthy list',()=>{
  for(const title of ['请稍候…','正在进行安全验证','Just a moment...','Checking your browser'])assert.equal(blockedTitle(title),true);
  assert.equal(blockedTitle('免费代理列表'),false);
});
