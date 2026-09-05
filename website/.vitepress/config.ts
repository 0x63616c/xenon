import { defineConfig } from 'vitepress'
export default defineConfig({
 title: 'Xenon', description: 'Temporal persistence, built on object storage.',
 srcDir: '.content', outDir: 'dist', base: process.env.SITE_BASE || '/',
 cleanUrls: false, appearance: false,
 head: [['meta',{name:'theme-color',content:'#f5f5f7'}],['meta',{name:'robots',content:'noindex,nofollow'}]],
 themeConfig: {
  logo: '/xenon.svg', siteTitle: 'Xenon',
  nav: [{text:'Architecture',link:'/architecture'},{text:'Documentation',link:'/overview'},{text:'Cloud',link:'/cloud'}],
  sidebar: [{text:'Understand Xenon',items:[{text:'Overview',link:'/overview'},{text:'Architecture',link:'/architecture'},{text:'Verification status',link:'/status'}]},{text:'Build with Xenon',items:[{text:'Local development',link:'/development'},{text:'Source code tour',link:'/code-tour'},{text:'Operations & recovery',link:'/operations'}]}],
  search:{provider:'local'}, outline:[2,3],
  footer:{message:'Private development preview · Proprietary software',copyright:'Xenon · Cloud coming soon'}
 }
})
