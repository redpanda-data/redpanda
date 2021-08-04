import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';

const FeatureList = [
  {
    title: 'Get started with Redpanda',
    Svg: require('../../static/img/Redpanda Rocket.svg').default,
    description: (
      <>
        Redpanda was designed from the ground up to be easily installed
        to get streaming up and running quickly.
      </>
    ),
  },
  {
    title: 'Production Redpanda deployments',
    Svg: require('../../static/img/Redpanda Sitting.svg').default,
    href: `/docs/deploy-self-hosted/production-deployment`,
    description: (
      <>
        After you see its power, put Redpanda to the real test in production.
      </>
    ),
  },
  {
    title: 'Deeper dive into Redpanda',
    Svg: require('../../static/img/Redpanda Transporter.svg').default,
    description: (
      <>
        When you want to use the more advanced Redpanda features...
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} alt={title} />
      </div>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
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
