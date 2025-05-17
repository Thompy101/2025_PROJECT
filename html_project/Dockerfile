FROM python:3.9-alpine

WORKDIR /usr/share/nginx/html


COPY . .


EXPOSE 8000


CMD ["python", "-m", "http.server", "8000"]
