FROM ubuntu:jammy

RUN echo 'APT::Install-Suggests "0";' >> /etc/apt/apt.conf.d/00-docker
RUN echo 'APT::Install-Recommends "0";' >> /etc/apt/apt.conf.d/00-docker
RUN DEBIAN_FRONTEND=noninteractive \
  apt-get update && apt-get install -y bc openssh-server wget mpich libglib2.0-dev pkg-config build-essential git vim cmake && rm -rf /var/lib/apt/lists/*

# Modify `sshd_config`
RUN sed -ri 's/PermitEmptyPasswords no/PermitEmptyPasswords yes/' /etc/ssh/sshd_config
RUN sed -ri 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -ri 's/^UsePAM yes/UsePAM no/' /etc/ssh/sshd_config
RUN echo "    StrictHostKeyChecking no" > /etc/ssh/ssh_config

WORKDIR /tmp

RUN wget --no-check-certificate https://github.com/openucx/ucx/releases/download/v1.15.0/ucx-1.15.0.tar.gz && tar xzf ucx-1.15.0.tar.gz
WORKDIR /tmp/ucx-1.15.0
RUN mkdir build
WORKDIR /tmp/ucx-1.15.0/build
RUN ../configure && make -j && make install

RUN rm -rf /tmp/ucx-1.15.0*

WORKDIR /
RUN mkdir /hercules

ENV GIT_SSL_NO_VERIFY=1
WORKDIR /hercules
RUN git clone https://gitlab.arcos.inf.uc3m.es/admire/hercules.git code
WORKDIR /hercules/code
RUN mkdir build
WORKDIR /hercules/code/build
RUN cmake .. && make -j


#hercules start -m /hercules/metadata -d /hercules/data -f /hercules/code/conf/hercules.conf.sample

WORKDIR /hercules
RUN echo "localhost" > data
RUN echo "localhost" > metadata
RUN mkdir conf
RUN cp /hercules/code/conf/hercules.conf.sample /hercules/conf/hercules.conf
RUN sed -ri 's|/home/hercules|/hercules/code|g' /hercules/conf/hercules.conf
RUN sed -ri 's|DATA_HOSTFILE = /home/user/data_hostfile|DATA_HOSTFILE = /hercules/data|g' /hercules/conf/hercules.conf
RUN sed -ri 's|METADATA_HOSTFILE = /home/user/meta_hostfile|METADATA_HOSTFILE = /hercules/metadata|g' /hercules/conf/hercules.conf
RUN cp /hercules/conf/hercules.conf /etc/
ENV H_PATH=/hercules/code
ENV H_BUILD_PATH=/hercules/code/build
ENV H_BASH_PATH=/hercules/code/bash


ENV PATH="$PATH:/hercules/code/build:/hercules/code/bash"

RUN ssh-keygen -t rsa -q -f "/root/.ssh/id_rsa" -N ""
RUN cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
RUN mkdir /var/run/sshd && chmod 0755 /var/run/sshd

EXPOSE 22 7500 8500
ENTRYPOINT service ssh restart && hercules start -m /hercules/metadata -d /hercules/data -f /hercules/conf/hercules.conf && tail -f /dev/null

