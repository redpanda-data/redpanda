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
    url: '/',
  },
  {
    title: 'Deploy Redpanda in production',
    Svg: require('../../static/img/Redpanda Sitting.svg').default,
    href: `/docs/deploy-self-hosted/production-deployment`,
    description: (
      <>
        After you see its power, put Redpanda to the real test in production.
      </>
    ),
    url: '/',
  },
  {
    title: 'Deeper dive into Redpanda',
    Svg: require('../../static/img/Redpanda Transporter.svg').default,
    description: (
      <>
        When you want to use the more advanced Redpanda features...
      </>
    ),
    url: '/',
  },
];

function Feature({Svg, title, description, url}) {
  return (
    <div className={clsx('col col--4')}>
      <a href={url}>
        <div className="text--center">
          <Svg className={styles.featureSvg} alt={title} />
        </div>
      </a>
      <div className="text--center padding-horiz--md">
        <a href={url}>
          <h3>{title}</h3>
        </a>
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
