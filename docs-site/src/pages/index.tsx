import React from 'react';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import styles from './index.module.css';

// ─── SVG Icons ───

function IconCypher() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="16 18 22 12 16 6" />
      <polyline points="8 6 2 12 8 18" />
    </svg>
  );
}

function IconGraph() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="6" cy="6" r="3" />
      <circle cx="18" cy="18" r="3" />
      <circle cx="18" cy="6" r="3" />
      <line x1="8.5" y1="7.5" x2="15.5" y2="16.5" />
      <line x1="15.5" y1="6" x2="8.5" y2="6" />
    </svg>
  );
}

function IconShield() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
    </svg>
  );
}

function IconVector() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="10" />
      <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z" />
      <line x1="2" y1="12" x2="22" y2="12" />
    </svg>
  );
}

function IconTerminal() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="4 17 10 11 4 5" />
      <line x1="12" y1="19" x2="20" y2="19" />
    </svg>
  );
}

function IconAlgorithm() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="18" y1="20" x2="18" y2="10" />
      <line x1="12" y1="20" x2="12" y2="4" />
      <line x1="6" y1="20" x2="6" y2="14" />
    </svg>
  );
}

function IconArrowRight() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <line x1="5" y1="12" x2="19" y2="12" />
      <polyline points="12 5 19 12 12 19" />
    </svg>
  );
}

function IconGitHub() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
      <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0 0 24 12c0-6.63-5.37-12-12-12z" />
    </svg>
  );
}

// ─── Data ───

const features = [
  {
    icon: <IconCypher />,
    title: 'OpenCypher Queries',
    description:
      'Full Cypher support with MATCH, CREATE, SET, DELETE, MERGE, aggregations, and 30+ built-in functions.',
  },
  {
    icon: <IconGraph />,
    title: 'Web Visualizer',
    description:
      'Interactive graph explorer with D3.js force layout, fullscreen mode, search, custom colors and icons.',
  },
  {
    icon: <IconShield />,
    title: 'Multi-Tenancy',
    description:
      'Isolated graph namespaces with per-tenant storage, authentication, and RBAC.',
  },
  {
    icon: <IconVector />,
    title: 'Vector Search',
    description:
      'HNSW index for k-NN similarity search, hybrid queries combining vectors with graph patterns.',
  },
  {
    icon: <IconTerminal />,
    title: 'Redis Protocol',
    description:
      'Drop-in RESP compatibility, connect with any Redis client from any language.',
  },
  {
    icon: <IconAlgorithm />,
    title: 'Graph Algorithms',
    description:
      'PageRank, shortest path, community detection, connected components, and more.',
  },
];

const stats = [
  { value: '1800+', label: 'Tests' },
  { value: '90%', label: 'OpenCypher' },
  { value: '<1ms', label: 'Queries' },
  { value: 'Multi', label: 'Tenant' },
];

// ─── Components ───

function Hero() {
  return (
    <header className={styles.hero}>
      <div className="container">
        <img src="/graphmind/img/logo.svg" alt="Graphmind" style={{width: 64, height: 64, marginBottom: 16}} />
        <h1 className={styles.heroTitle}>Graphmind</h1>
        <p className={styles.heroTagline}>
          High-performance distributed graph database built in Rust with OpenCypher support, vector search, and real-time visualization.
        </p>
        <div className={styles.heroButtons}>
          <Link className={styles.btnPrimary} to="/docs/getting-started">
            Get Started <IconArrowRight />
          </Link>
          <Link
            className={styles.btnOutline}
            href="https://github.com/fab679/graphmind"
          >
            <IconGitHub /> View on GitHub
          </Link>
        </div>
      </div>
    </header>
  );
}

function StatsBar() {
  return (
    <section className={styles.statsBar}>
      <div className="container">
        <div className={styles.statsGrid}>
          {stats.map((stat) => (
            <div key={stat.label} className={styles.statItem}>
              <span className={styles.statValue}>{stat.value}</span>
              <span className={styles.statLabel}>{stat.label}</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function Features() {
  return (
    <section className={styles.section}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Built for Production</h2>
        <p className={styles.sectionSubtitle}>
          Everything you need to build, query, and scale graph-powered applications.
        </p>
        <div className={styles.featureGrid}>
          {features.map((feature) => (
            <div key={feature.title} className={styles.featureCard}>
              <div className={styles.featureIcon}>{feature.icon}</div>
              <div className={styles.featureTitle}>{feature.title}</div>
              <p className={styles.featureDesc}>{feature.description}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function QuickStart() {
  return (
    <section className={styles.sectionAlt}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Quick Start</h2>
        <p className={styles.sectionSubtitle}>
          Get up and running in under a minute.
        </p>
        <div className={styles.codeWrapper}>
          <div className={styles.codeHeader}>
            <span className={styles.codeDot} />
            <span className={styles.codeDot} />
            <span className={styles.codeDot} />
          </div>
          <pre className={styles.codeBody}>
            <code>
              <span className={styles.codeComment}>
                # Install and run
              </span>
              {'\n'}
              <span className={styles.codeCommand}>
                docker run
              </span>
              {' -d -p 6379:6379 -p 8080:8080 fab679/graphmind:latest'}
              {'\n\n'}
              <span className={styles.codeComment}>
                # Open the visualizer
              </span>
              {'\n'}
              <span className={styles.codeCommand}>open</span>
              {' http://localhost:8080'}
              {'\n\n'}
              <span className={styles.codeComment}>
                # Or connect via redis-cli
              </span>
              {'\n'}
              <span className={styles.codeCommand}>redis-cli</span>
              {' -p 6379'}
              {'\n'}
              <span className={styles.codePrompt}>{'> '}</span>
              <span className={styles.codeCommand}>GRAPH.QUERY</span>
              {' default '}
              <span className={styles.codeString}>
                {'"CREATE (n:Person {name: \'Alice\'}) RETURN n"'}
              </span>
            </code>
          </pre>
        </div>
      </div>
    </section>
  );
}

function SDKs() {
  return (
    <section className={styles.section}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Native SDKs</h2>
        <p className={styles.sectionSubtitle}>
          First-class client libraries for Rust, Python, and TypeScript.
        </p>
        <div className={styles.sdkGrid}>
          <div className={styles.sdkCard}>
            <div className={styles.sdkHeader}>
              <span className={`${styles.sdkLang} ${styles.sdkRust}`}>Rs</span>
              Rust
            </div>
            <pre className={styles.sdkCode}>
              <code>
                <span className={styles.codeComment}>{'// Embedded mode - zero network overhead'}</span>
                {'\n'}
                {'let store = GraphStore::new();\n'}
                {'let id = store.create_node('}
                <span className={styles.codeString}>{'"Person"'}</span>
                {');\n'}
                {'store.set_property(id, '}
                <span className={styles.codeString}>{'"name"'}</span>
                {', '}
                <span className={styles.codeString}>{'"Alice"'}</span>
                {');'}
              </code>
            </pre>
          </div>
          <div className={styles.sdkCard}>
            <div className={styles.sdkHeader}>
              <span className={`${styles.sdkLang} ${styles.sdkPython}`}>Py</span>
              Python
            </div>
            <pre className={styles.sdkCode}>
              <code>
                <span className={styles.codeComment}>{'# pip install graphmind'}</span>
                {'\n'}
                {'from graphmind import GraphmindClient\n'}
                {'client = GraphmindClient('}
                <span className={styles.codeString}>{'"localhost:6379"'}</span>
                {')\n'}
                {'result = client.query('}
                <span className={styles.codeString}>{'"MATCH (n) RETURN n"'}</span>
                {')'}
              </code>
            </pre>
          </div>
          <div className={styles.sdkCard}>
            <div className={styles.sdkHeader}>
              <span className={`${styles.sdkLang} ${styles.sdkTypescript}`}>TS</span>
              TypeScript
            </div>
            <pre className={styles.sdkCode}>
              <code>
                <span className={styles.codeComment}>{'// npm install @graphmind/client'}</span>
                {'\n'}
                {'import { Graphmind } from '}
                <span className={styles.codeString}>{'"@graphmind/client"'}</span>
                {';\n'}
                {'const gm = new Graphmind({ port: 8080 });\n'}
                {'const res = await gm.query('}
                <span className={styles.codeString}>{'"MATCH (n) RETURN n"'}</span>
                {');'}
              </code>
            </pre>
          </div>
        </div>
      </div>
    </section>
  );
}

function FooterCTA() {
  return (
    <section className={styles.footerCta}>
      <div className="container">
        <h2 className={styles.footerCtaTitle}>Ready to get started?</h2>
        <p className={styles.footerCtaDesc}>
          Read the docs, explore the API, or dive straight into the code.
        </p>
        <div className={styles.heroButtons}>
          <Link className={styles.btnPrimary} to="/docs/getting-started">
            Read the Documentation <IconArrowRight />
          </Link>
        </div>
      </div>
    </section>
  );
}

// ─── Page ───

export default function Home(): React.JSX.Element {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout title={siteConfig.title} description={siteConfig.tagline}>
      <Hero />
      <StatsBar />
      <main>
        <Features />
        <QuickStart />
        <SDKs />
      </main>
      <FooterCTA />
    </Layout>
  );
}
