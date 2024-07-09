FROM python:3.10.12-slim-bookworm

RUN mkdir /app && pip install fastapi uvicorn[standard]

COPY app.py /app/app.py

WORKDIR /app

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
