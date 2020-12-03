/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

import * as winston from "winston";

export function newLogger(
  module_name: string,
  logLevel = "info",
  simplify = false
) {
  const rpWasmFormatter = () => {
    const logLineFormatter = winston.format.printf(
      ({ level, message, label, timestamp }) => {
        return `${timestamp} [${label}] ${level}: ${message}`;
      }
    );
    const simpleLogLineFormatter = winston.format.printf(
      ({ level, message, label }) => {
        return `[${label}] ${level}: ${message}`;
      }
    );

    return (simplify ? [] : [winston.format.timestamp()]).concat([
      winston.format.splat(),
      winston.format.label({ label: module_name }),
      simplify ? simpleLogLineFormatter : logLineFormatter,
    ]);
  };

  const logger = winston.createLogger({
    level: logLevel,
    format: winston.format.combine(...rpWasmFormatter()),
    defaultMeta: { service: module_name },
    transports: [
      new winston.transports.Console({
        level: logLevel,
      }),
    ],
  });
  return logger;
}
