FROM python:3.12-alpine
LABEL authors="Filip Dobrovolny"

RUN apk add --no-cache git
COPY . /app
WORKDIR /app
RUN pip install requests && pip install -r /app/requirements.txt

ENTRYPOINT ["python", "/app/main.py"]