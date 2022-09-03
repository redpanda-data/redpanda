class BadLogLines(Exception):
    def __init__(self, node_to_lines):
        self.node_to_lines = node_to_lines

    def __str__(self):
        # Pick the first line from the first node as an example, and include it
        # in the string output so that for single line failures, it isn't necessary
        # for folks to search back in the log to find the culprit.
        example_lines = next(iter(self.node_to_lines.items()))[1]
        example = next(iter(example_lines))

        summary = ','.join([
            f'{i[0].account.hostname}({len(i[1])})'
            for i in self.node_to_lines.items()
        ])
        return f"<BadLogLines nodes={summary} example=\"{example}\">"

    def __repr__(self):
        return self.__str__()


class NodeCrash(Exception):
    def __init__(self, crashes):
        self.crashes = crashes

        # Not legal to construct empty
        assert len(crashes)

    def __str__(self):
        example = f"{self.crashes[0][0].name}: {self.crashes[0][1]}"
        if len(self.crashes) == 1:
            return f"<NodeCrash {example}>"
        else:
            names = ",".join([c[0].name for c in self.crashes])
            return f"<NodeCrash ({names}) {example}>"

    def __repr__(self):
        return self.__str__()
