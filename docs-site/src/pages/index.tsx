import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link className="button button--secondary button--lg" to="/docs/getting-started">
            Get Started →
          </Link>
          <Link className="button button--outline button--lg" style={{marginLeft: '1rem', color: 'white', borderColor: 'white'}} to="/docs/architecture">
            Architecture
          </Link>
        </div>
      </div>
    </header>
  );
}

const features = [
  {
    title: 'OpenCypher Query Engine',
    description: '~90% OpenCypher support with 50+ functions, aggregations, path finding, and EXPLAIN/PROFILE.',
    emoji: '🔍',
  },
  {
    title: 'Redis Protocol Compatible',
    description: 'Connect via redis-cli or any Redis client. GRAPH.QUERY commands work out of the box.',
    emoji: '🔌',
  },
  {
    title: 'Built-in Web Visualizer',
    description: 'Interactive graph explorer with D3.js, CodeMirror editor, fullscreen mode, and dark/light themes.',
    emoji: '🎨',
  },
  {
    title: 'Graph Algorithms',
    description: 'PageRank, BFS, Dijkstra, shortest path, connected components, MST, max flow, and more.',
    emoji: '🧮',
  },
  {
    title: 'Vector Search (HNSW)',
    description: 'Native HNSW vector index for similarity search, hybrid queries, and AI embeddings.',
    emoji: '🧠',
  },
  {
    title: 'Multi-Tenancy & Persistence',
    description: 'RocksDB persistence, WAL, multi-tenant isolation, snapshots, and Raft consensus.',
    emoji: '🏢',
  },
  {
    title: 'Natural Language Queries',
    description: 'Ask questions in plain English. Supports OpenAI, Gemini, Ollama, and Claude.',
    emoji: '💬',
  },
  {
    title: 'High Performance',
    description: 'Written in Rust. 1M+ node ingestion in seconds. Late materialization and columnar storage.',
    emoji: '⚡',
  },
];

function Feature({title, description, emoji}: {title: string; description: string; emoji: string}) {
  return (
    <div className={clsx('col col--3')}>
      <div className="feature-card text--center padding--md">
        <div style={{fontSize: '2.5rem', marginBottom: '0.5rem'}}>{emoji}</div>
        <Heading as="h3" style={{fontSize: '1.1rem'}}>{title}</Heading>
        <p style={{fontSize: '0.9rem', opacity: 0.8}}>{description}</p>
      </div>
    </div>
  );
}

export default function Home(): React.JSX.Element {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout title={siteConfig.title} description={siteConfig.tagline}>
      <HomepageHeader />
      <main>
        <section style={{padding: '3rem 0'}}>
          <div className="container">
            <div className="row">
              {features.map((props, idx) => (
                <Feature key={idx} {...props} />
              ))}
            </div>
          </div>
        </section>

        <section style={{padding: '2rem 0', textAlign: 'center'}}>
          <div className="container">
            <Heading as="h2">Quick Start</Heading>
            <div style={{maxWidth: 600, margin: '0 auto', textAlign: 'left'}}>
              <pre style={{padding: '1.5rem', borderRadius: '8px'}}>
                <code>{`# Build and run
cargo build --release
cargo run

# Open the visualizer
open http://localhost:8080

# Connect via Redis CLI
redis-cli
> GRAPH.QUERY default "MATCH (n) RETURN n"`}</code>
              </pre>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}
