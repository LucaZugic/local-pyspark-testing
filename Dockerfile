FROM databricksruntime/standard:17.3-LTS

ARG INSTALL_MODE=wheel

# Install uv for fast Python package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy wheel if present (CI build)
COPY dist/*.whl* ./

# Install based on mode:
# - wheel: install pre-built wheel + pytest for CI testing
# - dev: skip, postCreateCommand handles it
RUN if [ "$INSTALL_MODE" = "wheel" ]; then \
      uv pip install --system --break-system-packages *.whl pytest && rm -f *.whl; \
    fi

# Copy test files and config (needed for CI test runs)
COPY tests ./tests
COPY pyproject.toml ./
