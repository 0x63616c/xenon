// UI-only regression: unchanged pinned UI imports a saved actual runtime history.
// Initial layout metadata is stubbed; this is not persistence/runtime proof.
import { chromium } from 'playwright';
import { mkdir, writeFile } from 'node:fs/promises';
const [base,history,output]=process.argv.slice(2);
if(!base||!history||!output)throw new Error('usage: URL HISTORY_JSON OUTPUT_DIR');
await mkdir(output,{recursive:true});
const browser=await chromium.launch({headless:true});
const page=await browser.newPage();
try {
 await page.route('**/api/v1/namespaces**',route=>route.fulfill({json:{namespaces:[{namespaceInfo:{name:'default',id:'11111111-1111-4111-8111-111111111111',state:'NAMESPACE_STATE_REGISTERED'},config:{},replicationConfig:{}}]}}));
 await page.route('**/api/v1/cluster-info**',route=>route.fulfill({json:{serverVersion:'1.31.2',clusterId:'diagnostic',clusterName:'active'}}));
 await page.route('**/api/v1/system-info**',route=>route.fulfill({json:{serverVersion:'1.31.2',capabilities:{}}}));
 await page.goto(base+'/import/events',{waitUntil:'networkidle',timeout:30000});
 await page.locator('input[type=file]').setInputFiles(history);
 await page.getByRole('button',{name:'Import',exact:true}).click();
 const terminal=page.getByTestId('event-summary-row').getByText(/Workflow Execution Completed$/);
 await terminal.waitFor({state:'visible',timeout:30000});
 if(await page.getByText('Workflow Execution Completed',{exact:true}).count()!==0)throw new Error('pinned nested-SVG exact-text regression no longer reproduced');
 await writeFile(output+'/dom.html',await page.content());
 await page.screenshot({path:output+'/history.png',fullPage:true});
 await writeFile(output+'/result.json',JSON.stringify({scope:'UI selector regression only',old_exact_text_count:0,terminal_visible:true,terminal_dom_text:await terminal.textContent()}));
} finally {await browser.close();}
