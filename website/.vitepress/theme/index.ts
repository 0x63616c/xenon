import DefaultTheme from 'vitepress/theme'
import Home from './Home.vue'
import Architecture from './Architecture.vue'
import './style.css'
export default { extends: DefaultTheme, enhanceApp({app}) { app.component('XenonHome',Home); app.component('XenonArchitecture',Architecture) } }
