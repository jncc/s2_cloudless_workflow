FROM ubuntu:22.04 AS base

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/miniconda/bin:$PATH

RUN apt update --fix-missing && \
    apt install -y wget bzip2 ca-certificates curl git binutils vim make build-essential && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

FROM base as prerequirements

RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/miniconda && \
    rm ~/miniconda.sh && \
    ln -s /opt/miniconda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/miniconda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate base" >> ~/.bashrc

RUN conda install --yes -c conda-forge python=3.12 gdal=3.9 && conda update --yes -n base conda

WORKDIR /working/software
RUN  git clone --depth 1 --branch rios-1.4.17 https://github.com/ubarsc/rios.git
WORKDIR /working/software/rios
RUN pip install .

RUN pip install scipy~=1.13.1

FROM prerequirements AS software

WORKDIR /working/software
RUN git clone --depth 1 --branch pythonfmask-0.5.9 https://github.com/ubarsc/python-fmask.git
WORKDIR /working/software/python-fmask
RUN pip install .

# HOTFIX
RUN sed -i "s|osr.UseExceptions()|#osr.UseExceptions()|g" /opt/miniconda/lib/python3.12/site-packages/fmask/sen2meta.py

RUN mkdir /working/data

WORKDIR /working
