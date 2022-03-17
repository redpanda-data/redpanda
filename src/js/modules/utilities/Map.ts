/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

export const find = function <A, B>(
  map: Map<A, B>,
  fn: (key: A, value: B) => boolean
): [A, B] | undefined {
  for (const [key, value] of map) {
    if (fn(key, value)) return [key, value];
  }
  return undefined;
};

export const map = function <A, B, C, D>(
  map: Map<A, B>,
  fn: (key: A, value: B) => { key: C; value: D }
): Map<C, D> {
  const newMap = new Map<C, D>();
  for (const [key, value] of map) {
    const { key: nKey, value: nValue } = fn(key, value);
    newMap.set(nKey, nValue);
  }
  return newMap;
};

export const groupBy = function <A, B, C>(
  map: Map<A, B>,
  fn: (key: A, value: B) => C
): Map<C, B[]> {
  const newMap = new Map<C, B[]>();
  for (const [key, value] of map) {
    const newKey = fn(key, value);
    const currentValue = newMap.get(newKey);
    if (!currentValue) {
      newMap.set(newKey, [value]);
    } else {
      currentValue.push(value);
      newMap.set(newKey, currentValue);
    }
  }
  return newMap;
};
