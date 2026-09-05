import { chromium } from 'playwright';
import { mkdir, writeFile, readFile } from 'node:fs/promises';
const output=process.argv[2];
const cfg=JSON.parse(await readFile(process.argv[3],'utf8'));
const base=`http://127.0.0.1:${cfg.ports.ui}/namespaces/${encodeURIComponent(cfg.namespace)}/workflows`;
if(!output)throw new Error('evidence directory required');
await mkdir(output,{recursive:true});
const browser=await chromium.launch({headless:true});
try {
 const page=await browser.newPage();
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
 await filtered.click();
 await page.getByText('Completed',{exact:true}).first().waitFor({state:'visible',timeout:30000});
 await page.screenshot({path:output+'/workflow-detail.png',fullPage:true});
 // Pinned UI835b349 workflow-header.svelte declares history-tab; event cards
 // render spaceBetweenCapitalLetters(event.name). Runtime visibility is mandatory.
 await page.locator('#history-tab').click();
 await page.getByText('Workflow Execution Completed',{exact:true}).first().waitFor({state:'visible',timeout:30000});
 await page.screenshot({path:output+'/workflow-event-history.png',fullPage:true});
 await writeFile(output+'/result.json',JSON.stringify({ui:'2.53.3',workflow:cfg.workflow_id,list_link:true,filtered_completed_run:true,detail_completed:true,event_history_completed:true,url:page.url()}));
} finally {await browser.close();}
