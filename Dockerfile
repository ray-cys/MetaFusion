FROM python:3.11-slim
WORKDIR /config
COPY . /config
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y cron
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
CMD ["/entrypoint.sh"]