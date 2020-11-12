/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

import * as assert from "assert";
import Repository from "../../modules/supervisors/Repository";

import { createHandle, createMockCoprocessor } from "../testUtilities";

describe("Repository", function () {
  it("should initialize with an empty map", function () {
    const repository = new Repository();
    assert(repository.size() === 0);
  });

  it("should add a handle to the repository", function () {
    const repository = new Repository();
    repository.add(createHandle());
    assert(repository.size() === 1);
  });

  it(
    "should replace a handle if a new one with the same globalId " +
      "is added.",
    function () {
      const topicA = "topicA";
      const topicB = "topicB";
      const coprocessorA = createMockCoprocessor(BigInt(1), [topicA]);
      const coprocessorB = createMockCoprocessor(BigInt(1), [topicB]);
      const repository = new Repository();
      repository.add(createHandle(coprocessorA));
      assert(repository.findByCoprocessor(coprocessorA));
      assert(
        repository
          .findByCoprocessor(coprocessorA)
          .coprocessor.inputTopics.includes(topicA)
      );
      repository.add(createHandle(coprocessorB));
      assert(repository.findByCoprocessor(coprocessorA));
      assert(repository.findByCoprocessor(coprocessorB));
      assert(
        repository
          .findByCoprocessor(coprocessorB)
          .coprocessor.inputTopics.includes(topicB)
      );
    }
  );

  it("should find a handle by another Handle", function () {
    const repository = new Repository();
    const handleA = createHandle();
    const handleB = createHandle({
      globalId: BigInt(2),
      inputTopics: ["topicB"],
    });
    repository.add(handleA);
    assert(repository.findByGlobalId(handleA));
    assert(!repository.findByGlobalId(handleB));
  });

  it("should find a handle by another Coprocessor", function () {
    const repository = new Repository();
    const handleA = createHandle();
    const handleB = createHandle({
      globalId: BigInt(2),
      inputTopics: ["topicB"],
    });
    repository.add(handleA);
    assert(repository.findByCoprocessor(handleA.coprocessor));
    assert(!repository.findByCoprocessor(handleB.coprocessor));
  });
});
