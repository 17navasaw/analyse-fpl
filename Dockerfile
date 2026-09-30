# Use the official uv image for the build stage
FROM ghcr.io/astral-sh/uv:0.10.11-python3.12-trixie AS builder

# Set the working directory
WORKDIR /app

# Enable bytecode compilation and copy dependency files
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Install dependencies first (better caching)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=analyse-fpl/uv.lock,target=analyse-fpl/uv.lock \
    --mount=type=bind,source=analyse-fpl/pyproject.toml,target=analyse-fpl/pyproject.toml \
    cd analyse-fpl && uv sync --frozen --no-install-project --no-dev

# Final Runtime Stage
FROM python:3.12-slim

WORKDIR /app

# Copy the environment from the builder
COPY --from=builder /app/analyse-fpl/.venv /app/analyse-fpl/.venv

# Ensure the app uses the virtualenv
ENV PATH="/app/analyse-fpl/.venv/bin:$PATH"

# Copy your source code
COPY . .

RUN mkdir -p /app/analyse-fpl/log
RUN touch /app/analyse-fpl/log/info.log

# Expose the FastAPI port
EXPOSE 8000

WORKDIR /app/analyse-fpl

# Start FastAPI using uvicorn
CMD ["fastapi", "run", "main.py", "--port", "8000"]
