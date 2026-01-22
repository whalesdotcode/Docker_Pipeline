# Start from slim Python image
FROM python:3.13-slim

# Set working directory inside container
WORKDIR /code

# Install uv
RUN pip install uv

# Copy dependency files first
COPY pyproject.toml uv.lock ./

# Create uv environment & install dependencies
RUN uv sync --locked

# Copy the rest of your project
COPY . .

# Default command: run pipeline.py with argument passed to container
# You can override the argument when running
ENTRYPOINT ["uv", "run", "python", "ingest_data.py"]