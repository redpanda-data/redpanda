package template

const packageJson = `{
  "name": "wasm-panda",
  "version": "1.0.0",
  "description": "inline wasm transforms sdk",
  "main": "bin/index.js",
  "bin": { "iwt": "./bin/index.js" },
  "scripts": {
    "build": "./webpack.js",
    "test": "node_modules/mocha/bin/mocha"
  },
  "keywords": ["inline-wasm-transform", "redpanda"],
  "author": "",
  "license": "ISC",
  "dependencies": {},
  "devDependencies": {
    "ts-loader": "8.0.4",
    "webpack": "4.44.2",
    "mocha": "8.1.3"
  }
}
`

func GetPackageJson() string {
	return packageJson
}
