import { defineComponent, Fragment, h } from 'vue';
import type { Theme } from 'vitepress';
import DefaultTheme from 'vitepress/theme';
import BrandLoading from './BrandLoading.vue';
import XenonLoader from './XenonLoader.vue';

export function withBrandLoading(theme: Theme): Theme {
  const Layout = theme.Layout || theme.extends?.Layout || DefaultTheme.Layout;
  return {
    ...theme,
    async enhanceApp(context) {
      await theme.enhanceApp?.(context);
      context.app.component('XenonLoader', XenonLoader);
    },
    Layout: defineComponent({
      inheritAttrs: false,
      setup(_, { attrs, slots }) {
        return () => h(Fragment, [h(Layout, attrs, slots), h(BrandLoading)]);
      },
    }),
  };
}
