# Local PySpark Testing with Databricks Runtime

A demonstration project for the Manchester Databricks User Group showing how to run local unit tests using the official Databricks Runtime Docker image.

## Why?

Testing PySpark code locally with the exact same runtime as production ensures:
- No surprises from version mismatches
- Fast feedback loops during development
- CI pipelines that catch issues before deployment

## Quick Start

### Prerequisites
- Docker
- VS Code with Dev Containers extension

### Local Development

1. Clone the repo
2. Open in VS Code
3. Click "Reopen in Container" when prompted
4. Run tests:
   ```bash
   pytest -v
   ```

That's it. You're running tests against Databricks Runtime 17.3 LTS (Spark 4.0.0) locally.

## Project Structure

```
├── src/local_pyspark_testing/
│   ├── environment.py      # Spark session factory (local vs Databricks)
│   ├── transforms.py       # UDFs using pycountry
│   └── jobs/               # Pipeline entry points
│       ├── bronze_to_silver.py
│       └── silver_to_gold.py
├── tests/
│   └── test_transforms.py  # DataFrame-based unit tests
├── resources/              # Databricks Asset Bundle configs
│   ├── clusters.yml
│   └── jobs.yml
├── Dockerfile              # Dual-mode: CI wheel install or dev editable
└── .devcontainer/          # VS Code devcontainer config
```

## How It Works

### Dual-Mode Dockerfile

The same Dockerfile serves both CI and local development:

```dockerfile
ARG INSTALL_MODE=wheel

# CI: installs pre-built wheel
# Dev: skips, postCreateCommand handles editable install
RUN if [ "$INSTALL_MODE" = "wheel" ]; then \
      uv pip install --system --break-system-packages *.whl pytest; \
    fi
```

### Spark Session Factory

`environment.py` creates the right Spark session based on context:

```python
def get_spark() -> SparkSession:
    if os.environ.get("LOCAL_SPARK") == "true":
        return _create_local_spark()  # Optimized for testing
    return _create_databricks_spark()  # Uses existing cluster session
```

### CI/CD Pipeline

1. **Build** - `uv build --wheel` creates the package
2. **Test** - Docker runs pytest against the wheel
3. **Push** - Image pushed to GitHub Container Registry
4. **Deploy** - Databricks Asset Bundles updates clusters and jobs

## Testing

Tests use `assertDataFrameEqual` for DataFrame comparisons:

```python
def test_converts_valid_codes(self, spark_session):
    df = spark_session.createDataFrame([("GB",), ("DE",)], ["country_code"])
    result = df.withColumn("country_name", country_code_to_name("country_code"))

    expected = spark_session.createDataFrame(
        [("GB", "United Kingdom"), ("DE", "Germany")],
        ["country_code", "country_name"],
    )
    assertDataFrameEqual(result, expected)
```

## Configuration

### GitHub Secrets Required

- `DATABRICKS_HOST` - Workspace URL
- `DATABRICKS_TOKEN` - Personal access token

### Databricks Workspace

Enable **Container Services** in Admin Settings (or via CLI):
```bash
databricks workspace-conf set-status --json '{"enableDcs": "true"}'
```

## License

MIT
