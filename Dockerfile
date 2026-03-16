FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

ENV PYTHONPATH=/app

CMD ["python", "main.py"]
