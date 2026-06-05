FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH

WORKDIR /workspace

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    bash \
    tini \
    && rm -rf /var/lib/apt/lists/*

COPY docker/base/requirements.txt /tmp/docker-requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/docker-requirements.txt

COPY . /workspace

RUN mkdir -p /workspace/logs /workspace/artifacts /workspace/data

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]
