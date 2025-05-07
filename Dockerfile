FROM ghcr.io/osgeo/gdal:ubuntu-full-3.10.2 AS base

RUN apt update --fix-missing && \
    apt install -y wget bzip2 ca-certificates curl git binutils vim make build-essential python3-pip && \
    apt clean

RUN mkdir -p /working/software
RUN mkdir -p /working/software/pip

FROM base AS prerequirements

WORKDIR /working/software
RUN  git clone --depth 1 --branch rios-2.0.6 https://github.com/ubarsc/rios.git
WORKDIR /working/software/rios
RUN pip install . --break-system-packages

RUN pip install scipy==1.15.1 --break-system-packages
RUN GDAL_CONFIG=/usr/bin/gdal-config pip install --no-binary rasterio rasterio==1.4.3 --break-system-packages

FROM prerequirements AS software

WORKDIR /working/software
RUN git clone --depth 1 --branch pythonfmask-0.5.10 https://github.com/ubarsc/python-fmask.git
WORKDIR /working/software/python-fmask
RUN pip install . --break-system-packages

# HOTFIX
#RUN sed -i "s|osr.UseExceptions()|#osr.UseExceptions()|g" /working/software/miniforge/lib/python3.12/site-packages/fmask/sen2meta.py

RUN pip install s2cloudless==1.7.3 --break-system-packages
RUN pip install rio-cogeo==5.4.1 --break-system-packages

FROM software AS workflow

RUN pip install luigi==3.6.0 --break-system-packages

COPY ./workflows /working/software/workflows
RUN cp /working/software/workflows/luigi.cfg.template /working/software/workflows/luigi.cfg
RUN chmod +x /working/software/workflows/cloudmask/container/exec.sh

RUN mkdir -p /input
RUN mkdir -p /work
RUN mkdir -p /state
RUN mkdir -p /output

WORKDIR /work

ENTRYPOINT [ "/working/software/workflows/cloudmask/container/exec.sh" ]
