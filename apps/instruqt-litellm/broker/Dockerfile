FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
COPY broker /app/broker

RUN pip install --no-cache-dir .

USER nobody

EXPOSE 8080

CMD ["uvicorn", "broker.main:app", "--host", "0.0.0.0", "--port", "8080"]
