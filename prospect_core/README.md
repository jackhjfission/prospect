# Template Core

Core functionality package for the template project.

## Overview

This package contains the fundamental building blocks and core logic for the project. It provides the essential functionality that other packages can build upon.

## Structure

```
prospect_core/
├── prospect_core/     # Core package source code
│   ├── cli/          # Command-line interface
│   └── core/         # Core functionality modules
├── tests/            # Test suite
├── prototyping/      # Experimental code and prototypes
└── scratchpads/      # Working files (gitignored)
```

## Development

### Installation

From the package directory:
```bash
poetry install --all-extras --all-groups
```

### Running Tests

```bash
poetry run pytest -v
```

### Code Quality

Pre-commit hooks will automatically run on commit. To run manually:
```bash
pre-commit run --files $(find . -type f)
```

## Usage

The prototyping directory is available for experimental development and testing new ideas before integrating them into the main codebase. The scratchpads directory is gitignored and can be used for temporary work files.
