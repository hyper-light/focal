FROM alpine:3.20
RUN apk add --no-cache iproute2
ENTRYPOINT ["tc"]
