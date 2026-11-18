// Normal visible Chrome only. No site JSON requests, cookie extraction,
// challenge interaction, stealth flags, proxy rotation or third-party packages.
import fs from 'node:fs/promises';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';

export function parseRows(rows) {
  const out=[];
  for(const cells of rows){
    if(!Array.isArray(cells)||cells.length<8)continue;
    const [host,port,protocols,,,latencyText,uptimeText,checkedText]=cells.map(String);
    const latency=Number(latencyText.match(/\d+(?:\.\d+)?/)?.[0]);
    const uptime=Number(uptimeText.match(/\d+(?:\.\d+)?/)?.[0]);
    const age=Number(checkedText.match(/\d+(?:\.\d+)?/)?.[0]);
    if(!Number.isFinite(latency)||latency>50||latency<0||!Number.isFinite(uptime)||uptime<90||uptime>100
      ||!Number.isFinite(age)||age>10||!/min|分钟|分鐘/i.test(checkedText))continue;
    const number=Number(port);if(!Number.isInteger(number)||number<1||number>65535)continue;
    const p=protocols.toUpperCase().split(/[,\s/]+/);
    const scheme=p.includes('SOCKS5')?'socks5h':p.includes('HTTP')?'http':null;
    // HTTPS-only labels do not specify whether TLS-to-proxy is supported.
    if(!scheme)continue;
    const address=host.includes(':')?`[${host}]`:host;
    out.push({url:`${scheme}://${address}:${number}`,source:'proxy5',uptime,latencyMs:latency});
  }
  return out;
}
const blockedTitle=t=>/just a moment|请稍候|安全验证|正在验证|checking your browser|attention required/i.test(t||'');

async function run(){
  if(typeof WebSocket!=='function')throw Error('需要 Node.js 22 或更新版本');
  const profile=process.env.APW_PROXY5_PROFILE;
  if(!profile)throw Error('缺少专用浏览器目录');
  await fs.mkdir(profile,{recursive:true});
  const portPath=path.join(profile,'DevToolsActivePort');
  let port;
  async function discover(){
    const value=Number((await fs.readFile(portPath,'utf8')).split('\n')[0]);
    if(!Number.isInteger(value)||value<1||value>65535)throw Error('无效本地调试端口');
    const r=await fetch(`http://127.0.0.1:${value}/json/list`,{signal:AbortSignal.timeout(1500)});
    if(!r.ok)throw Error('专用浏览器不可用');
    return {port:value,targets:await r.json()};
  }
  let connection;
  try{connection=await discover();}catch{
    const choices=[process.env.PROGRAMFILES,process.env['PROGRAMFILES(X86)'],process.env.LOCALAPPDATA]
      .filter(Boolean).map(d=>path.join(d,'Google','Chrome','Application','chrome.exe'));
    let executable;
    for(const c of choices){try{await fs.access(c);executable=c;break;}catch{}}
    if(!executable)throw Error('未找到 Chrome；不会自动安装或接入日常浏览器');
    const browser=spawn(executable,[`--user-data-dir=${profile}`,'--remote-debugging-address=127.0.0.1','--remote-debugging-port=0','--no-first-run','--no-default-browser-check','about:blank'],{stdio:'ignore',detached:true});
    browser.unref();
    for(let n=0;n<40;n++){try{connection=await discover();break;}catch{}await new Promise(r=>setTimeout(r,200));}
    if(!connection)throw Error('专用 Chrome 启动超时');
  }
  port=connection.port;
  const target=connection.targets.find(t=>t.type==='page'&&t.url.startsWith('https://proxy5.net/'))
    ||connection.targets.find(t=>t.type==='page'&&t.url==='about:blank');
  if(!target)throw Error('专用 Chrome 没有可用页面，请关闭该专用窗口后重试');
  const debug=new URL(target.webSocketDebuggerUrl);
  if(!['127.0.0.1','localhost'].includes(debug.hostname)||Number(debug.port)!==port)throw Error('拒绝非本地调试连接');
  const ws=new WebSocket(debug.href);
  await new Promise((res,rej)=>{const timer=setTimeout(()=>{ws.close();rej(Error('浏览器连接超时'));},5000);ws.addEventListener('open',()=>{clearTimeout(timer);res();},{once:true});ws.addEventListener('error',()=>{clearTimeout(timer);rej(Error('浏览器连接失败'));},{once:true});});
  let id=0;const waiting=new Map();
  ws.addEventListener('message',event=>{const m=JSON.parse(event.data);const item=waiting.get(m.id);if(!item)return;waiting.delete(m.id);clearTimeout(item.timer);m.error?item.reject(Error('浏览器命令失败')):item.resolve(m.result);});
  const send=(method,params={})=>new Promise((resolve,reject)=>{const n=++id;const timer=setTimeout(()=>{waiting.delete(n);reject(Error('浏览器操作超时'));},10000);waiting.set(n,{resolve,reject,timer});ws.send(JSON.stringify({id:n,method,params}));});
  async function evaluate(expression){const r=await send('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw Error('网页结构不兼容');return r.result.value;}
  try{
    await send('Page.enable');
    if(blockedTitle(await evaluate('document.title')))
      return {state:'blocked',message:'来源处于安全验证，请在专用 Chrome 中人工处理；自动获取已暂停。',candidates:[]};
    if(target.url==='about:blank')await send('Page.navigate',{url:'https://proxy5.net/cn/free-proxy'});
    else await send('Page.reload');
    let ready=false;
    for(let n=0;n<24;n++){
      const state=await evaluate('({title:document.title,ready:!!document.querySelector("#proxy-filter-form")&&!!document.querySelector("#proxy-table-body tr td:nth-child(8)")})');
      if(blockedTitle(state.title))return {state:'blocked',message:'来源要求安全验证，已停止；不会自动点击验证或重复尝试。',candidates:[]};
      if(state.ready){ready=true;break;}
      await new Promise(r=>setTimeout(r,500));
    }
    if(!ready)throw Error('页面表格未就绪，未获取候选');
    const filtered=await evaluate(`(()=>{const values={'filter-latency':'50','filter-speed':'250','filter-uptime':'90','filter-last-checked':'10'};for(const [id,value]of Object.entries(values)){const e=document.getElementById(id);if(!e||![...e.options].some(o=>o.value===value))return false;e.value=value;e.dispatchEvent(new Event('change',{bubbles:true}));}document.getElementById('proxy-filter-form').requestSubmit();const latency=document.querySelector('[data-sort=latency]');const uptime=document.querySelector('[data-sort=uptime]');if(!latency||!uptime)return false;latency.click();uptime.click();return true;})()`);
    if(!filtered)throw Error('来源筛选器已变更，未放宽筛选条件');
    if(blockedTitle(await evaluate('document.title')))return {state:'blocked',message:'来源要求安全验证，已暂停自动获取。',candidates:[]};
    const rows=await evaluate('Array.from(document.querySelectorAll("#proxy-table-body tr")).slice(0,20).map(tr=>Array.from(tr.cells).map(td=>td.innerText.trim()))');
    const candidates=parseRows(rows);
    if(!candidates.length)return {state:'error',message:'网页未显示符合条件的候选，未放宽筛选条件；保留已有线路。',candidates:[]};
    return {state:'ready',message:'按网页条件筛选；指标仅为来源展示值，不等于实际在线率或本机延迟。',candidates};
  }finally{
    for(const item of waiting.values()){clearTimeout(item.timer);item.reject(Error('浏览器连接已关闭'));}
    ws.close();
  }
}
if(process.argv[1]&&fileURLToPath(import.meta.url)===path.resolve(process.argv[1])){
  run().then(r=>console.log(JSON.stringify(r))).catch(()=>{console.log(JSON.stringify({state:'error',message:'无法读取专用 Chrome 表格；需要已安装 Chrome 与 Node.js 22+，请人工检查该窗口。',candidates:[]}));process.exitCode=1;});
}
export {blockedTitle};
