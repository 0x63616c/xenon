import { cpSync, mkdirSync, rmSync } from 'node:fs'
// Only explicitly authored guide content enters the site; never copy repo/evidence.
rmSync(new URL('./.content/',import.meta.url),{recursive:true,force:true})
mkdirSync(new URL('./.content/',import.meta.url),{recursive:true})
cpSync(new URL('../docs/guide/',import.meta.url),new URL('./.content/',import.meta.url),{recursive:true})
