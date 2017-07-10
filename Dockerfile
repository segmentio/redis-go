FROM alpine
COPY red /usr/local/bin/red
ENTRYPOINT ["red"]
