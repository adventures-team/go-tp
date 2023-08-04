FROM centos:centos7
RUN yum-config-manager --add-repo http://pkg.example.com/centos/7/x86_64/ --add-repo http://pkg.example.com/centos/7/noarch/
RUN yum -y install --nogpgcheck queued-default.x86_64
COPY queued1.conf /etc/queued1.conf
CMD /usr/local/bin/queued -c /etc/queued1.conf

# Master port
EXPOSE 20000

# Slave port
EXPOSE 20020

# Admin port
EXPOSE 20010
