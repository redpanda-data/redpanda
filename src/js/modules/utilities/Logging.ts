/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import * as winston from "winston";
import { Container, Logger, LoggerOptions } from "winston";

const maxLogFileSize = 5000000;

const rpWasmFormatter = (moduleName: string, simplify: boolean) => {
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
    winston.format.label({ label: moduleName }),
    simplify ? simpleLogLineFormatter : logLineFormatter,
  ]);
};

export class LogService {
  private static instance: LogService;
  private logFilePath: string;
  private winstonContext: Container;
  private isClosed = false;

  constructor() {
    this.winstonContext = new winston.Container();
  }

  static getInstance(): LogService {
    if (this.instance == undefined) {
      this.instance = new LogService();
      return this.instance;
    }
    return this.instance;
  }

  setPath(path: string): void {
    if (this.logFilePath == undefined) {
      this.logFilePath = path;
    }
  }

  getPath(): string {
    return this.logFilePath;
  }

  createLogger(
    moduleName: string,
    option?: LoggerOptions,
    simplify = false
  ): Logger {
    if (!this.isClosed) {
      const prevModule = this.winstonContext.has(moduleName);
      if (prevModule) {
        return this.winstonContext.get(moduleName);
      }

      const loggerConfig = {
        ...option,
        level: option?.level,
        format: winston.format.combine(
          ...rpWasmFormatter(moduleName, simplify)
        ),
        defaultMeta: { service: moduleName },
        transports: [
          new winston.transports.Console({
            level: option?.level,
          }),
          new winston.transports.File({
            maxsize: maxLogFileSize,
            filename: this.logFilePath,
            level: option?.level,
          }),
        ],
        exitOnError: false,
      };
      return this.winstonContext.add(moduleName, loggerConfig);
    } else {
      throw Error(
        "Service Logger was closed, it's not possible create a new " + "logger"
      );
    }
  }

  getLogger(id: string): Logger {
    return this.winstonContext.get(id);
  }

  close(): Promise<void> {
    this.isClosed = true;
    const closeLoggers = [...this.winstonContext.loggers.values()].map(
      (logger) =>
        new Promise((resolve) => {
          logger.on("finish", resolve);
          logger.end();
        })
    );
    return Promise.all(closeLoggers).then(() =>
      console.log("All outstanding logs have been flushed")
    );
  }
}

export default LogService.getInstance();
