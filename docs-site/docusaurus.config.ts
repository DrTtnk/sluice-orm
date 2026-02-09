import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Sluice ORM',
  tagline: 'Type-safe MongoDB aggregation pipeline builder',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://drttnk.github.io',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/sluice-orm/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'DrTtnk', // Usually your GitHub org/user name.
  projectName: 'sluice-orm', // Usually your repo name.

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
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
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/DrTtnk/sluice-orm/tree/main/docs-site/',
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/DrTtnk/sluice-orm/tree/main/docs-site/blog/',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    // Replace with your project's social card
    image: 'img/sluice-social-card.jpg',
    navbar: {
      title: 'Sluice ORM',
      logo: {
        alt: 'Sluice ORM Logo',
        src: 'img/logo.png',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'tutorialSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          to: '/docs/advanced-typings',
          label: 'Advanced Typings',
          position: 'left',
        },
        {
          to: '/docs/api',
          label: 'API Reference',
          position: 'left',
        },
        {
          href: 'https://github.com/DrTtnk/sluice-orm',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Getting Started',
              to: '/docs/intro',
            },
            {
              label: 'Advanced Typings',
              to: '/docs/advanced-typings',
            },
            {
              label: 'API Reference',
              to: '/docs/api',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/DrTtnk/sluice-orm',
            },
            {
              label: 'Issues',
              href: 'https://github.com/DrTtnk/sluice-orm/issues',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/DrTtnk/sluice-orm',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} DrTtnk. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;