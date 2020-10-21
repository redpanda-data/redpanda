package template

const webpack = `#!/usr/bin/env node
const webpack = require("webpack");
const fs = require("fs");

fs.readdir("./src/", (err, files) => {
  if (err) {
    console.log(err);
    return;
  }
  const webPackOptions = files.map((fileName) => {
    return {
      mode: "production",
      entry: { main: ` + "`./src/${fileName}`" + ` },
      output: {
        filename: fileName,
        libraryTarget: "commonjs2",
      },
    };
  });
  webpack(webPackOptions, (err, stat) => {
    if (err) {
      console.log(err);
    }
    const info = stat.toJson();
    if (stat.hasErrors()) {
      console.error(info.errors);
    }
    if (stat.hasWarnings()) {
      console.warn(info.warnings);
    }
  });
});
`

func GetWebpack() string {
	return webpack
}
