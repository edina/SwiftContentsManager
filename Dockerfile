FROM jupyter/base-notebook

MAINTAINER NaaS Project <edina@ed.ac.uk>

USER root

RUN apt-get update && apt-get install -yq --no-install-recommends \
    vim \
    build-essential \
    python3-dev \
    less \ 
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*
RUN pip install nose pytest pytest-cov python-coveralls

COPY . SwiftContentsManager/
WORKDIR SwiftContentsManager
RUN pip install -r requirements.txt
RUN pip install .

CMD py.test swiftcontents/tests/test_swiftmanager.py
