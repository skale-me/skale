FROM debian:jessie

COPY *.c /root/

# C, C++, make, curl, ssh, python
RUN apt-get update && apt-get -y install \
 build-essential \
 busybox \
 curl \
 git \
 openssh-client \
 openssh-server \
 python \
 python-numpy \
 && curl -s http://nodejs.org/dist/v4.1.2/node-v4.1.2-linux-x64.tar.xz \
    | tar -xJ -C /usr/local --strip-components=1

# Build a simple sudo, create ugrid user
RUN cc -o /bin/sudo /root/sudo.c \
 && strip /bin/sudo \
 && chmod u+s /bin/sudo \
 && useradd -m -u 1000 ugrid \
 && mkdir /home/ugrid/.ssh

# SSH setup for git access (read-only)
COPY id_rsa* config /home/ugrid/.ssh/
RUN cat /home/ugrid/.ssh/id_rsa.pub > /home/ugrid/.ssh/authorized_keys \
 && chown -R ugrid:ugrid /home/ugrid/.ssh \
 && chmod 0600 /home/ugrid/.ssh/id_rsa

USER ugrid
WORKDIR /home/ugrid

## SSH setup
#RUN ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa \
# && cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys \
# && echo "StrictHostKeyChecking no\nUserKnownHostsFile /dev/null\n" > /root/.ssh/config
#

RUN echo "alias vi='busybox vi'" >> /home/ugrid/.bash_aliases \
 && echo "alias netstat='busybox netstat'" >> /home/ugrid/.bash_aliases \
 && mkdir ugrid && cd ugrid \
 && git archive --format=tar --remote=ssh://git@gitlab.ugrid.net:10022/ugrid/ugrid.git master \
    | tar xf - \
 && npm install

COPY start-demo.sh /home/ugrid/

EXPOSE 22 8000 12346 12348

CMD ["/home/ugrid/start-demo.sh"]
