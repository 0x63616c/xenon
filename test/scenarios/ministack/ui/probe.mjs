import { chromium } from 'playwright';
import { mkdir, writeFile, readFile } from 'node:fs/promises';
const output=process.argv[2];
const cfg=JSON.parse(await readFile(process.argv[3],'utf8'));
const base=`http://127.0.0.1:${cfg.ports.ui}/namespaces/${encodeURIComponent(cfg.namespace)}/workflows`;
if(!output)throw new Error('evidence directory required');
await mkdir(output,{recursive:true});
const browser=await chromium.launch({headless:true});
const page=await browser.newPage();
const diagnostics={console_errors:[],page_errors:[],api_responses:[]};
page.on('pageerror',error=>diagnostics.page_errors.push(String(error)));
page.on('console',message=>{if(message.type()==='error')diagnostics.console_errors.push(message.text());});
page.on('response',response=>{if(new URL(response.url()).pathname.startsWith('/api/'))diagnostics.api_responses.push({url:response.url(),status:response.status()});});
try {
 await page.goto(base,{waitUntil:'networkidle',timeout:60000});
 const workflow=page.getByRole('link',{name:cfg.workflow_id,exact:true}).first();
 await workflow.waitFor({state:'visible',timeout:30000});
 await page.screenshot({path:output+'/workflow-list.png',fullPage:true});
 const query=`WorkflowId = '${cfg.workflow_id}' AND ExecutionStatus = 'Completed'`;
 await page.goto(base+'?query='+encodeURIComponent(query),{waitUntil:'networkidle',timeout:60000});
 const filtered=page.getByRole('link',{name:cfg.workflow_id,exact:true});
 await filtered.first().waitFor({state:'visible',timeout:30000});
 if(await filtered.count()!==1)throw new Error('filtered UI must contain exactly one completed run');
 await page.screenshot({path:output+'/workflow-filter.png',fullPage:true});
 const detailURL=new URL(await filtered.getAttribute('href'),base).href;
 await filtered.click();
 await page.waitForURL(url=>url.pathname===new URL(detailURL).pathname,{timeout:30000});
 await page.locator('#history-tab').waitFor({state:'visible',timeout:30000});
 await page.getByText('Completed',{exact:true}).first().waitFor({state:'visible',timeout:30000});
 await page.screenshot({path:output+'/workflow-detail.png',fullPage:true});
 // Pinned UI835b349 renders an SVG <title>Workflow</title> in this label.
 // Its DOM text includes that icon title before the visible event name; exact
 // text falsely rejects a rendered terminal event. Keep the terminal name anchored.
 await page.locator('#history-tab').click();
 await page.getByTestId('event-summary-row').getByText(/Workflow Execution Completed$/).first().waitFor({state:'visible',timeout:30000});
 await page.screenshot({path:output+'/workflow-event-history.png',fullPage:true});
 await writeFile(output+'/result.json',JSON.stringify({ui:'2.53.3',workflow:cfg.workflow_id,list_link:true,filtered_completed_run:true,detail_completed:true,event_history_completed:true,url:page.url()}));
} catch(error) {
 await writeFile(output+'/failure-dom.html',await page.content());
 await page.screenshot({path:output+'/failure.png',fullPage:true});
 throw error;
} finally {
 await writeFile(output+'/diagnostics.json',JSON.stringify(diagnostics,null,2));
 await browser.close();
}
