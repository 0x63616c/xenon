import { chromium } from 'playwright';
import { mkdir, writeFile } from 'node:fs/promises';
const output=process.argv[2];
if(!output)throw new Error('evidence directory required');
await mkdir(output,{recursive:true});
const browser=await chromium.launch({headless:true});
try {
 const page=await browser.newPage();
 await page.goto('http://127.0.0.1:18080/namespaces/xenon-ministack/workflows',{waitUntil:'networkidle',timeout:60000});
 const workflow=page.getByRole('link',{name:'xenon-durable-workflow-1',exact:true}).first();
 await workflow.waitFor({state:'visible',timeout:30000});
 await page.screenshot({path:output+'/workflow-list.png',fullPage:true});
 const query="WorkflowId = 'xenon-durable-workflow-1' AND ExecutionStatus = 'Completed'";
 await page.goto('http://127.0.0.1:18080/namespaces/xenon-ministack/workflows?query='+encodeURIComponent(query),{waitUntil:'networkidle',timeout:60000});
 const filtered=page.getByRole('link',{name:'xenon-durable-workflow-1',exact:true});
 await filtered.first().waitFor({state:'visible',timeout:30000});
 if(await filtered.count()!==1)throw new Error('filtered UI must contain exactly one completed run');
 await page.screenshot({path:output+'/workflow-filter.png',fullPage:true});
 await filtered.click();
 await page.getByText('Completed',{exact:true}).first().waitFor({state:'visible',timeout:30000});
 await page.screenshot({path:output+'/workflow-detail.png',fullPage:true});
 await writeFile(output+'/result.json',JSON.stringify({ui:'2.53.3',workflow:'xenon-durable-workflow-1',list_link:true,filtered_completed_run:true,detail_completed:true,url:page.url()}));
} finally {await browser.close();}
