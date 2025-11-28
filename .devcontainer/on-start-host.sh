#!/usr/bin/env bash

echo copying git pre-commit hook script
cp /home/prospector/workspace/.devcontainer/.pre-commit /home/prospector/workspace/.git/hooks/pre-commit

# keep poetry envs up to date
echo "Installing poetry environments..."
find /home/prospector/workspace -name "pyproject.toml" -type f | while read -r lockfile; do
    poetry_dir=$(dirname "$lockfile")
    echo "Installing dependencies in: $poetry_dir"
    cd "$poetry_dir" && poetry install --all-extras --all-groups
    if [ $? -eq 0 ]; then
        echo "✓ Successfully installed dependencies in $poetry_dir"
    else
        echo "✗ Failed to install dependencies in $poetry_dir"
    fi
done

# Generate mypy.ini files for packages with pre-commit configs
echo "Generating mypy.ini files..."
find /home/prospector/workspace -maxdepth 2 -name "pyproject.toml" -type f | while read -r pyproject; do
    pkg_dir=$(dirname "$pyproject")
    # Only generate for packages with .pre-commit-config.yaml
    if [ -f "$pkg_dir/.pre-commit-config.yaml" ]; then
        echo "Generating mypy.ini in: $pkg_dir"
        cd "$pkg_dir"
        python_path=$(poetry run which python)
        cat > mypy.ini << EOF
[mypy]
python_executable = $python_path
EOF
        echo "✓ Generated mypy.ini with python_executable=$python_path"
    fi
done
