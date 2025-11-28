# Template Tools

Shared utilities and tools package for the template project.

## Overview

This package provides common tools, utilities, and helper functions that can be shared across multiple packages in the project. It serves as a centralized location for reusable components that don't belong in the core functionality.

## Structure

```
prospect_tools/
├── prospect_tools/    # Tools package source code
│   └── cli/          # Command-line interface utilities
├── tests/            # Test suite
├── prototyping/      # Experimental tools and utilities
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

This package is designed to be imported and used by other packages in the monorepo. Tools developed here should be general-purpose and reusable across different contexts.

The prototyping directory is available for developing and testing new tools before they're ready for production use. The scratchpads directory is gitignored and can be used for temporary work files.
