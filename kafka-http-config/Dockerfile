FROM alpine:3.4

ADD kafka-http-config /
ADD config /

ENV PATH=$PATH:/

WORKDIR /

CMD ["kafka-http-config"]
