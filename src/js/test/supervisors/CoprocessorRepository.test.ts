import * as assert from "assert";
import CoprocessorRepository from "../../modules/supervisors/CoprocessorRepository";

import { createHandle, createMockCoprocessor } from "../testUtilities";

describe("CoprocessorRepository", function () {
  it("should initialize with an empty map", function () {
    const repository = new CoprocessorRepository();
    assert(repository.getCoprocessorsByTopics().size === 0);
  });

  it("should add a coprocessor to the repository", function () {
    const repository = new CoprocessorRepository();
    repository.add(createHandle());
    assert(repository.getCoprocessorsByTopics().size === 1);
  });

  it("should add a coprocessor for each topic in coprocessor", function () {
    const repository = new CoprocessorRepository();
    const topics = ["topicA", "topicB"];
    repository.add(createHandle({ inputTopics: topics }));
    assert(repository.getCoprocessorsByTopics().size === 2);
    topics.forEach((topic) => {
      assert(repository.getCoprocessorsByTopics().has(topic));
    });
  });

  it(
    "should replace a coprocessor if a new one with the same globalId " +
      "is added.",
    function (done) {
      const topicA = "topicA";
      const topicB = "topicB";
      const coprocessorA = createMockCoprocessor(1, [topicA]);
      const coprocessorB = createMockCoprocessor(1, [topicB]);
      const repository = new CoprocessorRepository();
      repository.add(createHandle(coprocessorA));
      const result1 = repository.getCoprocessorsByTopics();
      assert(result1.get(topicA).size() === 1);
      assert(
        result1
          .get(topicA)
          .findHandleByCoprocessor(coprocessorA)
          .coprocessor.inputTopics.includes(topicA)
      );
      repository.add(createHandle(coprocessorB)).then(() => {
        const result2 = repository.getCoprocessorsByTopics();
        assert(!!result2.get(topicA));
        assert(result2.get(topicB).size() === 1);
        assert(
          result2
            .get(topicB)
            .findHandleByCoprocessor(coprocessorB)
            .coprocessor.inputTopics.includes(topicB)
        );
        done();
      });
    }
  );

  it("should remove a coprocessor from all topics", function (done) {
    const topicA = "topicA";
    const topicB = "topicB";
    const topicC = "topicC";
    const repository = new CoprocessorRepository();
    const handleCoprocessorA = createHandle({
      inputTopics: [topicA, topicB, topicC],
    });
    const handleCoprocessorB = createHandle({
      globalId: 2,
      inputTopics: [topicC],
    });
    repository.add(handleCoprocessorA);
    repository.add(handleCoprocessorB);

    const expect1: [string, number][] = [
      [topicA, 1],
      [topicB, 1],
      [topicC, 2],
    ];
    expect1.forEach(([topic, coprocessorNumber]) => {
      assert(
        repository.getCoprocessorsByTopics().get(topic).size() ===
          coprocessorNumber
      );
    });
    repository.remove(handleCoprocessorA).then(() => {
      const expect2: [string, number][] = [
        [topicA, 0],
        [topicB, 0],
        [topicC, 1],
      ];
      expect2.forEach(([topic, coprocessorNumber]) => {
        assert(
          repository.getCoprocessorsByTopics().get(topic).size() ===
            coprocessorNumber
        );
      });
      done();
    });
  });

  it("should find a coprocessor by another CoprocessorHandle", function () {
    const repository = new CoprocessorRepository();
    const coprocessorHandleA = createHandle();
    const coprocessorHandleB = createHandle({
      globalId: 2,
      inputTopics: ["topicB"],
    });
    repository.add(coprocessorHandleA);
    assert(repository.findByGlobalId(coprocessorHandleA));
    assert(!repository.findByGlobalId(coprocessorHandleB));
  });

  it("should find a coprocessor by another Coprocessor", function () {
    const repository = new CoprocessorRepository();
    const coprocessorHandleA = createHandle();
    const coprocessorHandleB = createHandle({
      globalId: 2,
      inputTopics: ["topicB"],
    });
    repository.add(coprocessorHandleA);
    assert(repository.findByCoprocessor(coprocessorHandleA.coprocessor));
    assert(!repository.findByCoprocessor(coprocessorHandleB.coprocessor));
  });
});
