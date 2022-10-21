FROM python:3.9.7-alpine
WORKDIR /clickhouse-executor
COPY . /clickhouse-executor
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.ustc.edu.cn/g' /etc/apk/repositories \
 && apk add --update gcc musl-dev python3-dev libffi-dev openssl-dev build-base \
 && pip3 install --upgrade pip \
 && pip3 install --no-cache-dir -r requirements.txt
ENTRYPOINT ["python", "excute.py"]