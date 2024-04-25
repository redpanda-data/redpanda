import * as esbuild from 'esbuild'
import { polyfillNode } from "esbuild-plugin-polyfill-node";

await esbuild.build({
  entryPoints: ["src/index%s"],
  outfile: "dist/%s",
  bundle: true,
  external: [
    // This package is provided by the Redpanda JavaScript runtime.
    "@redpanda-data/transform-sdk",
  ],
  target: "es2022",
  platform: "neutral", // We're running in Wasm
  plugins: [
    polyfillNode({
      globals: {
        // Allow a global Buffer variable if referenced.
        buffer: true,
        // Don't inject the process global, the Redpanda JavaScript runtime
        // does that.
        process: false,
      },
      polyfills: {
        // Any NodeJS APIs that need to polyfilled can be added here.
      },
    }),
  ],
});
