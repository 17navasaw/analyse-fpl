# Use the official uv image for the build stage
FROM astral-sh/uv:python3.12-slim AS builder

# Set the working directory
WORKDIR /app

# Enable bytecode compilation and copy dependency files
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Install dependencies first (better caching)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Final Runtime Stage
FROM python:3.12-slim

WORKDIR /app

# Copy the environment from the builder
COPY --from=builder /app/.venv /app/.venv

# Ensure the app uses the virtualenv
ENV PATH="/app/.venv/bin:$PATH"

# Copy your source code
COPY . .

# Expose the FastAPI port
EXPOSE 8000

# Start FastAPI using uvicorn
CMD ["fastapi", "run", "main.py", "--port", "8000"]
