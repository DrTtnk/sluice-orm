import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import CodeBlock from '@theme/CodeBlock';

import styles from './index.module.css';

const heroExample = `import { registry, $match, $group, $project, $sort } from "sluice-orm";
import { Schema as S } from "@effect/schema";

const OrderSchema = S.Struct({
  _id: S.String,
  userId: S.String,
  amount: S.Number,
  items: S.Array(S.Struct({
    name: S.String,
    price: S.Number,
    quantity: S.Number,
  })),
  status: S.Literal("pending", "paid", "shipped"),
  createdAt: S.Date,
});

const db = registry("8.0", { orders: OrderSchema });
const { orders } = db(client.db("shop"));

// Every stage's output type flows to the next — fully inferred
const report = await orders
  .aggregate(
    $match($ => ({ status: "paid" })),
    //    ^? { _id: string; userId: string; amount: number; ... }

    $group($ => ({
      _id: "$userId",
      totalSpent: $.sum("$amount"),
      orderCount: $.sum(1),
    })),
    //    ^? { _id: string; totalSpent: number; orderCount: number }

    $project($ => ({
      userId: "$_id",
      totalSpent: 1,
      orderCount: 1,
      avgOrder: $.divide("$totalSpent", "$orderCount"),
      _id: 0,
    })),
    //    ^? { userId: string; totalSpent: number; orderCount: number; avgOrder: number }

    $sort({ totalSpent: -1 }),
  )
  .toList();

// report: { userId: string; totalSpent: number; orderCount: number; avgOrder: number }[]
`;

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <h1 className="hero__title">{siteConfig.title}</h1>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <p className={styles.heroTagline}>
          Write MongoDB aggregation pipelines where the return type is <em>inferred from the query</em> —
          not from a manual generic annotation.
        </p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/intro">
            Get Started
          </Link>
          <Link
            className="button button--outline button--lg"
            to="/docs/advanced-typings">
            See Type Inference
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): JSX.Element {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title="Type-safe MongoDB pipelines"
      description="Type-safe MongoDB aggregation pipeline builder where return types are inferred from your queries">
      <HomepageHeader />
      <main>
        <HomepageFeatures />

        <section className={styles.showcase}>
          <div className="container">
            <h2 className={styles.showcaseTitle}>Types Flow Through Your Pipeline</h2>
            <p className={styles.showcaseSubtitle}>
              Each stage transforms the document shape. Sluice infers the output type at every step —
              no manual annotations needed.
            </p>
            <CodeBlock language="typescript" title="pipeline-type-flow.ts">
              {heroExample}
            </CodeBlock>
          </div>
        </section>

        <section className={styles.comparison}>
          <div className="container">
            <div className="row">
              <div className="col col--6">
                <h3>Without Sluice</h3>
                <CodeBlock language="typescript">
{`// No type safety — runtime errors waiting to happen
collection.aggregate([
  { $group: { _id: "$departement", avg: { $avg: "$salray" } } },
  //                  ^ typo                        ^ typo
  //  You won't know until production 💥
]);`}
                </CodeBlock>
              </div>
              <div className="col col--6">
                <h3>With Sluice</h3>
                <CodeBlock language="typescript">
{`// Compile-time safety — errors caught instantly
$group($ => ({
  _id: "$departement",  // ❌ TS Error: no field "departement"
  avg: $.avg("$salray"),  // ❌ TS Error: no field "salray"
}));
// Fix the typos, ship with confidence ✅`}
                </CodeBlock>
              </div>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}
