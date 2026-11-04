/* global URL, process, sessionStorage, console */
import {chromium} from '@playwright/test';
import {readFile,mkdir} from 'node:fs/promises';
const values=Object.fromEntries((await readFile(new URL('../.env',import.meta.url),'utf8')).split(/\r?\n/).filter(x=>x.includes('=')&&!x.startsWith('#')).map(x=>{const i=x.indexOf('=');return [x.slice(0,i),x.slice(i+1)]}));
const browser=await chromium.launch({headless:true,...(process.env.CHROME_PATH?{executablePath:process.env.CHROME_PATH}:{})});
try {
 const context=await browser.newContext({viewport:{width:1440,height:1000}});
 await context.addInitScript(token=>sessionStorage.setItem('vaultstore.token',token),values.API_TOKEN);
 const page=await context.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
 page.on('response',r=>{if(r.url().includes('/api/v1/')&&!r.ok())errors.push(`API ${r.status()} ${r.url()}`)});
 await page.goto(process.env.WEB_URL||'http://127.0.0.1:5173');
 await page.getByRole('heading',{name:'Protection overview'}).waitFor();
 await page.locator('.metric').first().waitFor();
 await page.locator('.recharts-surface').first().waitFor();
 await mkdir(new URL('../.runtime/screenshots/',import.meta.url),{recursive:true});
 await page.screenshot({path:new URL('../.runtime/screenshots/overview.png',import.meta.url).pathname.replace(/^\/([A-Za-z]:)/,'$1'),fullPage:true});
 for(const name of ['Backup Sources','Backups','Recovery Points','Restore Jobs','Verification','Storage','Workers','Jobs','Analytics','Settings']){
  await page.getByRole('link',{name,exact:true}).click();
  await page.locator('h1').waitFor();
  await page.waitForTimeout(300);
  if(await page.locator('[role="alert"]').count())throw new Error(`${name}: ${await page.locator('[role="alert"]').allTextContents()}`);
 }
 await page.getByRole('link',{name:'Recovery Points',exact:true}).click();
 await page.getByRole('link',{name:'Inspect →'}).first().click();
 await page.getByRole('heading',{name:'Snapshot files'}).waitFor();
 await page.getByRole('heading',{name:'Start restore'}).waitFor();
 await page.setViewportSize({width:390,height:844});
 await page.screenshot({path:new URL('../.runtime/screenshots/mobile.png',import.meta.url).pathname.replace(/^\/([A-Za-z]:)/,'$1'),fullPage:true});
 if(errors.length)throw new Error(errors.join('\n'));
 console.log('Browser smoke passed: 11 routes, live metrics, recovery-point file/restore view, mobile rendering, no JavaScript errors.');
}finally{await browser.close()}
