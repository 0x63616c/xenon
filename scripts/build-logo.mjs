#!/usr/bin/env node
// Reproduce the approved rounded-X geometry, small SVG loaders, and editor page.
import { readFileSync, writeFileSync, mkdirSync, copyFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const root = fileURLToPath(new URL('../', import.meta.url));
const out = path.join(root, 'assets/brand');
const settings = { layers: 3, spokeWidth: 1.2, roundness: .85, innerRoundness: .54, depth: .58, gap: 0, outline: .064, cycle: 2.5, stagger: .1, movingTime: .75, easing: [.75, 0, .25, 1], direction: 'anticlockwise' };
const r = .43 * settings.spokeWidth, q = r * settings.roundness;
const c = 1.38 - r * (Math.SQRT2 - (Math.SQRT2 - 1) * settings.roundness);
const k = r * Math.SQRT1_2, n = r * Math.SQRT2;
const d = Math.min(r * .45, (c-k) * .7) * settings.innerRoundness;
const f = x => Number(x.toFixed(5));
const xy = (x,y) => `${f(256+x*148)} ${f(256-y*148)}`;
let commands = [];
const M = (x,y) => commands.push(`M${xy(x,y)}`);
const L = (x,y) => commands.push(`L${xy(x,y)}`);
const Q = (x,y,a,b) => commands.push(`Q${xy(x,y)} ${xy(a,b)}`);
function cap(sx,sy) {
 const x=sx*c,y=sy*c,dx=sx*Math.SQRT1_2,dy=sy*Math.SQRT1_2,px=-dy,py=dx;
 L(x+px*r,y+py*r); L(x+dx*(r-q)+px*r,y+dy*(r-q)+py*r);
 const radius=f(q*148);
 commands.push(`A${radius} ${radius} 0 0 1 ${xy(x+dx*r+px*(r-q),y+dy*r+py*(r-q))}`);
 L(x+dx*r-px*(r-q),y+dy*r-py*(r-q));
 commands.push(`A${radius} ${radius} 0 0 1 ${xy(x+dx*(r-q)-px*r,y+dy*(r-q)-py*r)}`);
 L(x-px*r,y-py*r);
}
M(d,n+d);cap(1,1);L(n+d,d);Q(n,0,n+d,-d);cap(1,-1);
L(d,-n-d);Q(0,-n,-d,-n-d);cap(-1,-1);L(-n-d,-d);Q(-n,0,-n-d,d);cap(-1,1);L(-d,n+d);Q(0,n,d,n+d);commands.push('Z');
const shape=commands.join(' '),stroke=f(settings.outline*2*148);
const svg=(title,body)=>`<svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512" role="img" aria-label="${title}"><title>${title}</title>${body}</svg>\n`;
const mark=`<path d="${shape}" fill="none" stroke="#000000" stroke-width="${stroke}" stroke-linejoin="round"/>`;
writeFileSync(path.join(out,'mark.svg'),svg('Xenon rounded X',mark));
writeFileSync(path.join(out,'shape.json'),JSON.stringify({version:1,...settings,path:shape,strokeWidth:stroke,viewBox:'0 0 512 512'},null,2)+'\n');
copyFileSync(path.join(out,'mark.svg'),path.join(root,'docs/guide/public/xenon.svg'));
const motionDir=path.join(out,'spinners');mkdirSync(motionDir,{recursive:true});
for(const variant of ['stagger','counter','counter-v2','orbit'])for(const dark of [false,true]) {
 const ink=dark?'#ffffff':'#000000',paper=dark?'#000000':'#ffffff';
 const num=variant==='orbit'?1:settings.layers;
 const cycle=variant.startsWith('counter')?settings.cycle/1.2:settings.cycle;
 // Gentler quadratic speed ramps around a constant-speed middle; still moving for 75% of each loop.
 const staggerMotion='.disc{animation-name:stagger-turn}@keyframes stagger-turn{0%{transform:rotate(0deg);animation-timing-function:cubic-bezier(.333333,0,.666667,.333333)}22.5%{transform:rotate(19.285714deg);animation-timing-function:linear}52.5%{transform:rotate(70.714286deg);animation-timing-function:cubic-bezier(.333333,.666667,.666667,1)}75%,100%{transform:rotate(90deg)}}';
 const css=`.disc{transform-origin:256px 256px;animation:turn ${cycle}s cubic-bezier(.75,0,.25,1) infinite}.disc.reverse{animation-name:reverse}@keyframes turn{0%{transform:rotate(0deg)}75%,100%{transform:rotate(90deg)}}@keyframes reverse{0%{transform:rotate(0deg)}75%,100%{transform:rotate(-90deg)}}${variant==='orbit'?'.disc{animation:orbit 2.5s linear infinite}@keyframes orbit{to{transform:rotate(360deg)}}':''}${variant==='stagger'?staggerMotion:''}.breath{transform-origin:256px 256px;animation:breathe ${cycle}s cubic-bezier(.45,0,.25,1) infinite}@keyframes breathe{0%,75%,100%{transform:scale(1)}37.5%{transform:scale(1.065)}}@media(prefers-reduced-motion:reduce){.disc,.breath{animation:none!important}}`;
 const discs=Array.from({length:num},(_,i)=>{
  const delay=f(-cycle+(num-1-i)*cycle*settings.stagger);
  const face=`<path d="${shape}" fill="${paper}" stroke="${ink}" stroke-width="${stroke}" stroke-linejoin="round"/>`;
  const body=variant==='counter-v2'?`<g class="breath" style="animation-delay:${delay}s">${face}</g>`:face;
  return `<g class="disc${variant.startsWith('counter')&&i%2?' reverse':''}" style="animation-delay:${delay}s">${body}</g>`;
 }).join('');
 writeFileSync(path.join(motionDir,`${variant}${dark?'-white':''}.svg`),svg(`Xenon ${variant} loading animation`,`<style>${css}</style>${discs}`));
}
// The chosen primary identity is the original counter-spin, white on black.
const counter=readFileSync(path.join(motionDir,'counter-white.svg'),'utf8');
const badge=counter.replace('</title>','</title><rect width="512" height="512" fill="#000000"/>');
writeFileSync(path.join(out,'logo.svg'),badge);
writeFileSync(path.join(out,'loader.svg'),badge);
const fragment=readFileSync(path.join(root,'tools/logo-editor/editor.fragment.html'),'utf8');
writeFileSync(path.join(root,'tools/logo-editor/index.html'),`<!doctype html>\n<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Xenon logo editor</title><style>body{margin:0;background:#eee}#xenon-motion{max-width:1000px;margin:auto}</style></head><body>\n${fragment}\n</body></html>\n`);
const samples=name=>`<div class="samples"><img src="${name}.svg" width="120" height="120" alt="${name} loader"><img src="${name}.svg" width="48" height="48" alt=""><img src="${name}.svg" width="24" height="24" alt=""></div><div class="samples dark"><img src="${name}-white.svg" width="120" height="120" alt="${name} loader on black"><img src="${name}-white.svg" width="48" height="48" alt=""><img src="${name}-white.svg" width="24" height="24" alt=""></div>`;
const tiles=['stagger','counter','orbit'].map(name=>`<article><h2>${name}</h2>${samples(name)}${name==='counter'?'<h2>Counter v2 / pulse</h2><p>A small outward swell with each turn, then back into alignment.</p>'+samples('counter-v2'):''}</article>`).join('');
writeFileSync(path.join(motionDir,'index.html'),`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Xenon loading spinners</title><style>@font-face{font-family:Jakarta;src:url('../../fonts/PlusJakartaSans.ttf')}@font-face{font-family:Varela;src:url('../../fonts/VarelaRound-Regular.ttf')}body{font-family:Jakarta,sans-serif;color:#111;background:#fff;margin:0;padding:32px}main{max-width:1000px;margin:auto}h1{font-family:Varela,sans-serif;font-size:40px;letter-spacing:-2px}h2{text-transform:capitalize;font-size:18px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:24px}.samples{display:flex;align-items:center;justify-content:space-around;gap:12px;padding:24px 12px;border:1px solid #ddd}.dark{background:#000;border-color:#000}p{line-height:1.6}a{color:inherit}button{padding:10px 20px;background:white;border:1px solid #aaa;font:inherit}body.paused img{visibility:hidden}body.paused .samples{background-image:url('../mark.svg');background-position:center;background-repeat:no-repeat;background-size:70px}body.paused .samples.dark{background-image:url('../mark-white.svg')}</style></head><body><main><h1>xenon / loading</h1><p>Three lightweight SVG spinners, plus a counter-spin experiment. Your chosen shape, clockwise-led motion, and 2.5-second timing (counter-spin is 20% faster). Reduced-motion preferences show the static mark.</p><button onclick="document.body.classList.toggle('paused');this.textContent=document.body.classList.contains('paused')?'Play':'Pause'">Pause</button><div class="grid">${tiles}</div><p><a href="../../../tools/logo-editor/index.html">Open the 3D editor</a></p></main></body></html>`);
console.log('Built canonical rounded X, eight spinner SVGs, gallery, and standalone editor.');
