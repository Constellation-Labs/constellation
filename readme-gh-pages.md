## Required tools
Install [MkDocs](https://www.mkdocs.org/).
Available through homebrew, pip, etc.

## Development
```mkdocs serve``` from the constellation root directory wil start a development server that auto-builds and serves the docs at http://localhost:8000. This is very useful for testing out changes.

## Deployment
```mkdocs gh-deploy``` will build the the files in docs-gh-pages, merge the changes into the constellation gh-pages branch, and push to origin/gh-pages.

Github is configured to serve the gh-pages branch at https://constellation-labs.github.io/constellation/.
