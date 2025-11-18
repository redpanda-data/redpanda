#!/usr/bin/env python3
"""
Split a full Swagger/OpenAPI JSON file into component files for Seastar tooling.

This script takes a complete Swagger 2.0 JSON file and splits it into:
1. A header file containing the swagger version, info, host, basePath, and schemes
2. A definitions file containing just the definitions section
3. A paths file containing just the paths section

The output files can be concatenated (with proper JSON structure) to reconstruct
the original file for use with Seastar's json2code tool.
"""

import json
import sys
from pathlib import Path


def split_swagger_json(input_file: Path, output_dir: Path, output_basename: str = None):
    """Split a Swagger JSON file into component files.

    Args:
        input_file: Path to the input JSON file
        output_dir: Directory to write output files to
        output_basename: Base name for output files (without extension).
                        If None, uses input_file.stem
    """
    if output_basename is None:
        output_basename = input_file.stem

    # Read the input file
    with open(input_file, "r") as f:
        swagger_data = json.load(f)

    # Extract the header (everything except paths and definitions)
    header_data = {
        k: v for k, v in swagger_data.items() if k not in ["paths", "definitions"]
    }
    # Add opening for paths
    header_content = json.dumps(header_data, indent=2)
    # Remove the trailing brace and add the paths key opening
    header_content = header_content.rstrip("\n}") + ',\n  "paths": {\n'

    # Extract the definitions (just the content, not the key)
    definitions_content = ""
    if "definitions" in swagger_data:
        definitions_json = json.dumps(swagger_data["definitions"], indent=2)
        # Remove the outer braces and add proper spacing with leading newline
        lines = definitions_json.split("\n")[1:-1]  # Skip first { and last }
        # Adjust indentation from 2 spaces to 4 spaces
        adjusted_lines = [
            "    " + line[2:] if line.startswith("  ") else line for line in lines
        ]
        definitions_content = "\n" + "\n".join(adjusted_lines) + "\n"

    # Extract the paths (just the content, not the key)
    paths_content = ""
    if "paths" in swagger_data:
        paths_json = json.dumps(swagger_data["paths"], indent=2)
        # Remove the outer braces and add leading newline
        lines = paths_json.split("\n")[1:-1]  # Skip first { and last }
        # Adjust indentation from 2 spaces to 4 spaces
        adjusted_lines = [
            "    " + line[2:] if line.startswith("  ") else line for line in lines
        ]
        paths_content = "\n" + "\n".join(adjusted_lines) + "\n"

    # Write the output files
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write header file
    header_file = output_dir / f"{output_basename}_header.json"
    with open(header_file, "w") as f:
        f.write(header_content)

    # Write definitions file
    definitions_file = output_dir / f"{output_basename}_definitions.def.json"
    with open(definitions_file, "w") as f:
        f.write(definitions_content)

    # Write paths file
    paths_file = output_dir / f"{output_basename}.json"
    with open(paths_file, "w") as f:
        f.write(paths_content)

    return header_file, definitions_file, paths_file


def main():
    if len(sys.argv) not in [3, 4]:
        print(f"Usage: {sys.argv[0]} <input.json> <output_dir> [output_basename]")
        sys.exit(1)

    input_file = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    output_basename = sys.argv[3] if len(sys.argv) == 4 else None

    if not input_file.exists():
        print(f"Error: Input file {input_file} does not exist")
        sys.exit(1)

    try:
        header, definitions, paths = split_swagger_json(
            input_file, output_dir, output_basename
        )
        print(f"Successfully split {input_file} into:")
        print(f"  - {header}")
        print(f"  - {definitions}")
        print(f"  - {paths}")
    except Exception as e:
        print(f"Error splitting JSON: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
