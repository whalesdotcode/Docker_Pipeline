FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

RUN pip install pyarrow pandas 

WORKDIR /code

COPY ".python-version" "uv.lock" "pyproject.toml" ./

COPY pipeline/pipeline.py .

# ENTRYPOINT [ "python", "pipeline.py" ]