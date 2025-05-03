# MAI Data Utilities

A collection of utilities for managing and validating AI model data in the Maida AI ecosystem.

## Features

- File size validation to ensure repository constraints
- Utilities for managing AI model data
- CI/CD integration with size checks

## Installation

```bash
pip install mai_data
```

For development:

```bash
pip install -e ".[dev]"
pre-commit install
```

## Usage

### Size Guard

The size guard utility helps ensure that no files in the repository exceed size limits:

```python
from mai_data.size_guard import check_repo_size

# Check if any files exceed 200MB
if check_repo_size():
    print("All files are within size limits")
else:
    print("Found files exceeding size limits")
```

## Development

### Running Tests

```bash
pytest
```

### Linting

```bash
ruff check .
black .
isort .
```

## License

Apache-2.0
