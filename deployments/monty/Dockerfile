FROM ghcr.io/astral-sh/uv:0.12.19 AS uv
FROM python:3.12-slim

COPY --from=uv /uv /usr/local/bin/uv
ADD https://github.com/temporal-community/temporal-agent-harness/archive/ceb6ef3fdf3f31b32f571c8460d4fd545e117977.tar.gz /tmp/harness.tar.gz
RUN mkdir /app && tar -xzf /tmp/harness.tar.gz -C /app --strip-components=1 && rm /tmp/harness.tar.gz
WORKDIR /app
ENV UV_LINK_MODE=copy PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN uv sync --frozen --no-default-groups --extra genai --extra code-mode --extra ui --extra s3
COPY launcher.py /opt/monty/launcher.py
ENV PATH="/app/.venv/bin:$PATH" PYTHONPATH=/app
USER 10001:10001
EXPOSE 8000
ENTRYPOINT ["python", "/opt/monty/launcher.py"]
