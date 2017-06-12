FROM alpine:3.4

RUN mkdir /app

COPY kafka-http-config /app/

ENV PATH=$PATH:/app

CMD ["./kafka-http-config"]
