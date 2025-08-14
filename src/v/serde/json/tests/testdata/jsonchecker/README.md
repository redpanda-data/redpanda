Test suite from http://json.org/JSON_checker/.

If the JSON_checker is working correctly, it must accept all of the pass*.json files and reject all of the fail*.json files.

Changes:
- The json_checker_test uses a depth limit of 19 to ensure fail18.json (20 levels) fails while pass2.json (19 levels) succeeds.
- fail1.json: REMOVED - Originally rejected top-level strings, but updated JSON spec allows any value at top level.
