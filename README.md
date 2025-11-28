# Prospect

A Python monorepo for Prospect with automated CI/CD workflows and development container support.

## Contributing

This project uses a DevContainer to provide a consistent, fully-configured development environment. All required tools and dependencies are automatically set up when you open the project in the container.

### Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Visual Studio Code](https://code.visualstudio.com/)
- [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

### Getting Started

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd prospect
   ```

2. **Build the Docker image**
   ```bash
   cd .devcontainer
   docker compose build
   cd ..
   ```
   This builds the development container image locally. The image is not stored in a repository, so this step is required before first use.

3. **Open in DevContainer**
   - Open the project folder in VS Code
   - When prompted, click "Reopen in Container" (or use Command Palette: `Dev Containers: Reopen in Container`)
   - The container will start automatically

4. **Start developing!**
   - All dependencies are automatically installed on container startup
   - Pre-commit hooks are automatically configured
   - **mypy.ini files are automatically generated** for each package
   - Your SSH keys are mounted for git operations

### DevContainer Features

#### 🐍 Python Environment
- **Python 3.12-slim** base image
- **Poetry** for dependency management (installed globally via pipx)
- **Pre-commit** for code quality checks (installed globally via pipx)

#### 🔧 Automatic Setup
On container startup, the following happens automatically:
- **Poetry dependencies** are installed for all packages in the monorepo
- **Pre-commit hooks** are installed in the git repository
- **Git configuration** is ready with your mounted SSH keys

#### 📦 VS Code Extensions
The container comes pre-configured with essential extensions:
- **Python Development**: Pylance, Python, debugpy for debugging
- **Jupyter Notebooks**: Full notebook support with renderers and extensions
- **Code Quality**: isort for import organization
- **Version Control**: Git Graph for repository visualization
- **AI Assistant**: Claude Dev for AI-powered development support

#### ⚙️ Container Configuration
- **Memory**: 8GB allocated (configurable in `devcontainer.json`)
- **User**: Non-root user (`prospector`) for security
- **SSH Keys**: Automatically mounted from `~/.ssh` on host
- **Workspace**: Mounted at `/home/prospector/workspace`

### Working with Multiple Packages

This monorepo contains multiple Python packages. Each package has its own:
- `pyproject.toml` - Poetry configuration and dependencies
- `.pre-commit-config.yaml` - Pre-commit hooks configuration
- `poetry.lock` - Locked dependency versions

To work on a specific package:

```bash
cd prospect_core  # or prospect_tools
poetry install    # Install/update dependencies
poetry run pytest # Run tests
```

### Pre-commit Hooks

This project uses a **custom pre-commit hook** (`.devcontainer/pre-commit`) that intelligently runs checks independently for each package that has modifications. When you commit changes:

1. The hook automatically detects which packages contain staged files
2. For each affected package, it runs pre-commit with that package's own configuration
3. Only files within each package directory are checked, using that package's Poetry environment

This ensures:
- Each package's pre-commit hooks run in isolation with the correct dependencies
- You don't need to run hooks manually for each package
- Commits are fast - only modified packages are checked

#### Available Hooks (per package)

- **Black** - Code formatting
- **Ruff** - Linting and style checks
- **isort** - Import sorting
- **mypy** - Type checking with package-specific Python environments
- **Standard checks** - Trailing whitespace, end-of-file fixes, etc.
- **Notebook tools** - nbstripout, nbqa-black, nbqa-ruff, nbqa-isort

#### Manual Execution

To run pre-commit manually on a specific package:
```bash
cd prospect_core  # Run from package directory
pre-commit run --files $(find . -type f)
```

Note that the use of `find` here is required to only include files in the subdirectory as `pre-commit` runs at the top level of the repo.

### CI/CD Workflows

The project includes automated GitHub Actions workflows:

- **Pre-commit Checks** (`.github/workflows/run-pre-commit-hooks.yml`)
  - Runs pre-commit hooks on changed packages only
  - Uses separate Poetry environments per package
  - Dynamically discovers packages (no hardcoded names)

- **Tests** (`.github/workflows/run-tests.yml`)
  - Runs pytest on changed packages only
  - Parallel execution with Poetry environment caching
  - Automatically discovers new packages

Both workflows use smart change detection to only process packages with modifications, optimizing CI time.

### Environment Configuration

The `.devcontainer/.env` file contains configuration variables:
```env
IMAGE_NAME=prospect          # Docker image name
IMAGE_VERSION=v0.0.0        # Docker image version
USER_NAME=prospector         # Container user name
```

Modify these if you need to customize the container setup.

### Troubleshooting

**Container won't start:**
- Ensure Docker is running
- Check Docker has enough resources allocated (8GB+ recommended)
- Try rebuilding: Command Palette → `Dev Containers: Rebuild Container`

**Dependencies not installing:**
- Check Poetry is in PATH: `poetry --version`
- Manually trigger installation: Run the on-start script from the workspace root

**Pre-commit hooks not working:**
- Verify hooks are installed: `ls .git/hooks/pre-commit`
- Manually reinstall: `pre-commit install`

**SSH keys not working:**
- Ensure your SSH keys are in `~/.ssh` on your host machine
- Check file permissions on your host (`chmod 600 ~/.ssh/id_*`)
