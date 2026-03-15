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
