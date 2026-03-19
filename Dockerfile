FROM databricksruntime/standard:17.3-LTS

ARG INSTALL_MODE=wheel

# Install uv for fast Python package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy project files (dist/ created by initializeCommand in dev, by CI in wheel mode)
COPY pyproject.toml ./
COPY dist/ ./dist/

# Install based on mode:
# - wheel: install pre-built wheel + pytest for CI testing
# - dev: skip, postCreateCommand handles editable install
RUN if [ "$INSTALL_MODE" = "wheel" ]; then \
      uv pip install --system --break-system-packages ./dist/*.whl pytest && rm -rf dist/; \
    fi

# Copy test files (needed for CI test runs)
COPY tests ./tests
