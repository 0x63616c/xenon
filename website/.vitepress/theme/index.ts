import { h } from "vue";
import Footer from "./Footer.vue";
import RoutingDemo from "./RoutingDemo.vue";
import Blog from "./Blog.vue";
import Cloud from "./Cloud.vue";
import DefaultTheme from "vitepress/theme";
import Home from "./Home.vue";
import Architecture from "./Architecture.vue";
import "./style.css";
export default {
  extends: DefaultTheme,
  Layout: () =>
    h(DefaultTheme.Layout, null, { "layout-bottom": () => h(Footer) }),
  enhanceApp({ app }) {
    app.component("XenonHome", Home);
    app.component("XenonCloud", Cloud);
    app.component("XenonBlog", Blog);
    app.component("XenonRoutingDemo", RoutingDemo);
    app.component("XenonArchitecture", Architecture);
  },
};
