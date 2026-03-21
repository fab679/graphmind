import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
    'getting-started',
    {
      type: 'category',
      label: 'Installation',
      items: ['installation/docker', 'installation/binary', 'installation/source', 'installation/configuration'],
    },
    {
      type: 'category',
      label: 'Cypher Guide',
      items: ['cypher/basics', 'cypher/crud', 'cypher/patterns', 'cypher/aggregations', 'cypher/functions', 'cypher/indexes'],
    },
    {
      type: 'category',
      label: 'Administration',
      items: ['admin/authentication', 'admin/multi-tenancy', 'admin/backup-restore', 'admin/monitoring'],
    },
    {
      type: 'category',
      label: 'SDKs & Drivers',
      items: ['sdks/index', 'sdks/rust', 'sdks/python', 'sdks/typescript', 'sdks/rest-api', 'sdks/resp-protocol'],
    },
    {
      type: 'category',
      label: 'Web Visualizer',
      items: ['visualizer/index', 'visualizer/features'],
    },
    {
      type: 'category',
      label: 'Advanced',
      items: ['advanced/vector-search', 'advanced/algorithms', 'advanced/nlq', 'advanced/architecture'],
    },
    'glossary',
  ],
};

export default sidebars;
