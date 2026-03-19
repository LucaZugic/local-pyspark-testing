# Local PySpark Testing with Databricks

A demonstration project for the Manchester Databricks User Group showing how to build a local development environment using official Databricks Docker images, enabling local unit testing before deployment.

## Project Goals

- Build local dev environment from official Databricks Docker image
- Enable local unit testing of PySpark code
- Push image on deployment and create cluster from it
- Demonstrate TDD workflow for Spark development

## Tech Stack

- Python with PySpark
- pytest for testing
- Docker (Databricks runtime image)
- uv for environment management
- Ruff for linting

## Architecture

- Clean, testable code structure
- Medallion architecture for data layers where applicable
- Modular design for easy testing

## Development

Open the project in VS Code and use the "Reopen in Container" command to start the devcontainer. This runs the Databricks 17.3 LTS image with all dependencies installed.

The devcontainer is named `local-pyspark-testing`.

```bash
# Run tests (inside devcontainer)
pytest

# Run tests from host via docker
docker exec -w /workspaces/local-pyspark-testing local-pyspark-testing pytest

# Lint
ruff check .
ruff format .
```

## Development Workflow

### TDD with Claude Code

When asked to implement a new feature or fix a bug, follow strict TDD:

1. **Write the failing test first** - Propose a test that captures the expected behaviour
2. **Run the test to confirm it fails**
3. **Implement the minimal code** to make the test pass
4. **Run the test to confirm it passes**
5. **Refactor if needed** - with the safety net of passing tests

Always run tests locally before suggesting the work is complete.

### Testing Commands

Tests must run inside the devcontainer. If the container is not running, start it first:

```bash
# Start the devcontainer if not running
docker start local-pyspark-testing

# Run tests via devcontainer (from host)
docker exec -w /workspaces/local-pyspark-testing local-pyspark-testing pytest -v

# Run specific test file
docker exec -w /workspaces/local-pyspark-testing local-pyspark-testing pytest tests/test_transforms.py -v

# Run tests matching a pattern
docker exec -w /workspaces/local-pyspark-testing local-pyspark-testing pytest -v -k "country"
```
