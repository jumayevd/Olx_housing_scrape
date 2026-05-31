# Official Playwright Python image — includes Chromium + all system deps
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scraper.py .

# Default output dir — mount a Railway Volume here to persist files
ENV OUTPUT_DIR=/data
ENV PYTHONUNBUFFERED=1

CMD ["python", "scraper.py"]
