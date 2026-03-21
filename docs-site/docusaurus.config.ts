import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Graphmind',
  tagline: 'High-performance distributed graph database written in Rust',
  favicon: 'img/favicon.svg',

  future: {
    v4: true,
  },

  markdown: {
    mermaid: true,
  },
  themes: ['@docusaurus/theme-mermaid'],

  url: 'https://fab679.github.io',
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
          editUrl: 'https://github.com/fab679/graphmind/tree/main/docs-site/',
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
          sidebarId: 'docs',
          position: 'left',
          label: 'Documentation',
        },
        {
          to: '/docs/cypher/basics',
          label: 'Cypher Guide',
          position: 'left',
        },
        {
          to: '/docs/sdks/index',
          label: 'SDKs',
          position: 'left',
        },
        {
          to: '/docs/sdks/rest-api',
          label: 'API',
          position: 'left',
        },
        {
          href: 'https://github.com/fab679/graphmind',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Getting Started',
          items: [
            { label: 'Quick Start', to: '/docs/getting-started' },
            { label: 'Docker', to: '/docs/installation/docker' },
            { label: 'Configuration', to: '/docs/installation/configuration' },
          ],
        },
        {
          title: 'Guides',
          items: [
            { label: 'Cypher Basics', to: '/docs/cypher/basics' },
            { label: 'CRUD Operations', to: '/docs/cypher/crud' },
            { label: 'SDKs', to: '/docs/sdks/index' },
            { label: 'REST API', to: '/docs/sdks/rest-api' },
          ],
        },
        {
          title: 'Advanced',
          items: [
            { label: 'Architecture', to: '/docs/advanced/architecture' },
            { label: 'Vector Search', to: '/docs/advanced/vector-search' },
            { label: 'Graph Algorithms', to: '/docs/advanced/algorithms' },
          ],
        },
        {
          title: 'More',
          items: [
            { label: 'GitHub', href: 'https://github.com/fab679/graphmind' },
            { label: 'Roadmap', href: 'https://github.com/fab679/graphmind/blob/main/ROADMAP.md' },
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
