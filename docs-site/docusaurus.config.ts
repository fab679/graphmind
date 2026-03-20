import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Graphmind',
  tagline: 'High-performance distributed graph database written in Rust',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  markdown: {
    mermaid: true,
  },
  themes: ['@docusaurus/theme-mermaid'],

  url: 'https://graphmind-ai.github.io',
  baseUrl: '/graphmind/',

  organizationName: 'fab679',
  projectName: 'graphmind',
  deploymentBranch: 'gh-pages',
  trailingSlash: false,

  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/graphmind-ai/graphmind/tree/main/docs-site/',
          path: 'docs',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/graphmind-social-card.png',
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Graphmind',
      logo: {
        alt: 'Graphmind Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          to: '/docs/architecture',
          label: 'Architecture',
          position: 'left',
        },
        {
          to: '/docs/category/architecture-decision-records',
          label: 'ADRs',
          position: 'left',
        },
        {
          to: '/docs/api',
          label: 'API',
          position: 'left',
        },
        {
          href: 'https://github.com/graphmind-ai/graphmind',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            { label: 'Getting Started', to: '/docs/getting-started' },
            { label: 'Architecture', to: '/docs/architecture' },
            { label: 'Cypher Support', to: '/docs/cypher-compatibility' },
            { label: 'API Reference', to: '/docs/api' },
          ],
        },
        {
          title: 'Learn',
          items: [
            { label: 'ADRs', to: '/docs/category/architecture-decision-records' },
            { label: 'Benchmarks', to: '/docs/category/performance' },
            { label: 'LDBC', to: '/docs/category/ldbc-benchmarks' },
            { label: 'Tech Stack', to: '/docs/tech-stack' },
          ],
        },
        {
          title: 'More',
          items: [
            { label: 'GitHub', href: 'https://github.com/graphmind-ai/graphmind' },
            { label: 'Roadmap', href: 'https://github.com/graphmind-ai/graphmind/blob/main/ROADMAP.md' },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Graphmind. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['rust', 'bash', 'cypher', 'toml', 'yaml', 'python', 'typescript'],
    },
    mermaid: {
      theme: { light: 'default', dark: 'dark' },
    },
    algolia: undefined,
  } satisfies Preset.ThemeConfig,
};

export default config;
