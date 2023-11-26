FROM rocker/r-base:4.2.3

ENV DEBIAN_FRONTEND=noninteractive

# Must install python 3.10 for the client
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    python3 \
    python3-pip \
    python3-setuptools \
    python3-venv \
    python3-dev \
    # R client dep
    dpkg-dev \
    zlib1g-dev \
    libssl-dev \
    libffi-dev \
    libxml2-dev \
    # procps is required for nextflow tower
    curl \
    libcurl4-openssl-dev \
    procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# RUN pip3 install --upgrade pip

ENV RENV_VERSION 0.17.3
RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cloud.r-project.org'))"
RUN R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')"

WORKDIR /project
# Only copy the renv lock here so that we make use of the caching
COPY renv.lock renv.lock
# Set the RENV_PATHS_LIBRARY environment variable to a
# writable path within your Docker container
ENV RENV_PATHS_LIBRARY renv/library

RUN R -e "renv::restore()"

COPY . .
