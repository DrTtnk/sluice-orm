import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';

type FeatureItem = {
  title: string;
  icon: string;
  description: JSX.Element;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Full Type Inference',
    icon: '🔬',
    description: (
      <>
        Every <code>$group</code>, <code>$project</code>, and <code>$addFields</code> stage
        produces a precisely inferred output type. Chain 10 stages and the final type is
        exactly what MongoDB returns — no casts, no generics, no guessing.
      </>
    ),
  },
  {
    title: 'Pipeline Type Flow',
    icon: '🔗',
    description: (
      <>
        Each stage's output becomes the next stage's input. Rename a field in <code>$project</code>?
        The next <code>$group</code> immediately sees the new shape. Typos are
        caught before your code ever runs.
      </>
    ),
  },
  {
    title: 'Zero Runtime Cost',
    icon: '⚡',
    description: (
      <>
        All type checking happens at compile time. Your production bundle
        contains only the MongoDB queries you wrote — no wrappers, no
        reflection, no overhead.
      </>
    ),
  },
  {
    title: 'Schema Agnostic',
    icon: '🧩',
    description: (
      <>
        Works with Effect Schema, Zod, or plain TypeScript types.
        Bring your own validation library, or skip runtime validation entirely.
      </>
    ),
  },
  {
    title: 'Full MongoDB 8.0+',
    icon: '🗄️',
    description: (
      <>
        Every aggregation stage, expression operator, accumulator, and window
        function — typed. <code>$facet</code>, <code>$setWindowFields</code>,
        <code>$lookup</code> with sub-pipelines, all covered.
      </>
    ),
  },
  {
    title: 'Effect Integration',
    icon: '🎭',
    description: (
      <>
        Optional first-class Effect.ts support. Get tagged errors, dependency
        injection, and composable pipelines for production applications.
      </>
    ),
  },
];

function Feature({title, icon, description}: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      <div className={styles.featureCard}>
        <div className={styles.featureIcon}>{icon}</div>
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
