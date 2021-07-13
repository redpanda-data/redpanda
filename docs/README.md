_The master branch_ => [![CircleCI](https://circleci.com/gh/vectorizedio/redpanda-docs/tree/master.svg?style=svg)](https://circleci.com/gh/vectorizedio/redpanda-docs/tree/master)

# Vectorized Documentation Center

The Vectorized Documentation Center is built with [Hugo](https://gohugo.io/) version 0.48 and is based on the [DocDock](https://github.com/vjeantet/hugo-theme-docdock.git) theme.

## Installing the Vectorized Documentation Center

To run the Vectorized Documentation Center locally:

1. Install the latest Hugo:

   - On CentOS:

     1. Install the copr plugin for yum: `sudo yum install yum-plugin-copr`
     1. Enable the Hugo repository: `sudo yum copr enable daftaupe/hugo`
     1. Install Hugo: `sudo yum install hugo`

   - On Ubuntu:

     - Install Hugo: `sudo apt-get install hugo`

   - On Windows:

     1. Install [chocolatey](https://chocolatey.org/install).
     1. Install Hugo: `choco install hugo -confirm`

   - On MacOS:

     1. Install [homebrew](https://brew.sh/)
     2. Install Hugo: `brew install hugo`

1. Verify that Hugo is installed: `hugo version`
1. Clone this repository to your local host.
1. Change directory to the redpanda-docs directory.
1. Start the hugo web server: `hugo server`

To access the site, go to: http://localhost:1313

## Publishing

The latest branch is the latest stable version of documentation for the latest publicly available release at: https://docs.vectorized.io/latest

Documentation for previous versions is published through the version build branches (for example 5.2-build).

The master branch is published to https://docs.vectorized.io/staging/dev and represents the unstable version of documentation for the latest publicly available release. When stable, this branch is published to the latest branch.

## Contributing to the documentation

Whether you see a typo or you want to suggest a change to the technical content, we encourage you to fork this repository and submit a pull request.

The "Edit on GitHub" link that is in the upper-left corner of every page opens the source file of that page so you can easily submit a change.

## Content organization

Articles are organized in directories that present the articles in a heirarchy in the site sidebar. When you add a file to a directory, it is automatically listed in that section in the sidebar.

The metadata (front matter) of a page is used to:

- Order the articles in a section by the 'weight' parameter (Remember, lower weight = higher priority
- Mark articles as `draft: true` so that they are not published

To add a new section in the sidebar, you must add a directory and add an `_index.md` file in that directory. The content of the \_index.md file is shown when you click on the category in the sidebar, and the `weight` of the \_index.md file determines the order in which the category is listed in the sidebar.

## Markdown

For more information about markdown syntax for our docs, see the [cheatsheet](https://docs.vectorized.io/latest/cheatsheet).
