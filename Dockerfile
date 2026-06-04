FROM python:3.12

WORKDIR /processor

RUN apt clean && apt-get update && apt-get -y install libhdf5-dev

COPY processor/requirements.txt /processor/requirements.txt

RUN pip install -r /processor/requirements.txt

COPY processor/ /processor

ENV PYTHONPATH="/"

# pynwb creates a cache dir at import time. On Fargate the task runs as a
# non-root user with HOME unset, so ~/.cache resolves to /.cache (not writable).
# Point platformdirs at /tmp (world-writable, UID-agnostic).
ENV XDG_CACHE_HOME="/tmp/.cache"

CMD ["python3.12", "-m", "processor.main"]
