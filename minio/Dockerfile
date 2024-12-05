FROM quay.io/minio/minio

COPY config.env /etc/config.env

EXPOSE 9000 9001

CMD ["server", "/data", "--console-address", ":9001"]