FROM openjdk:8
RUN apt-get update
RUN apt-get install -y apt-transport-https ca-certificates
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
RUN apt-get update
RUN apt-get install -y sbt
RUN apt-get install -y python3-pip
ENV HOME /home/developer
WORKDIR /home/developer
COPY . .
ARG VERSION
RUN sbt clean coverage test coverageReport
RUN sbt publishLocal
RUN chmod -R 775 /home/developer
WORKDIR /home/developer/python
RUN pip3 install -r requirements.txt
RUN PYSPARK_PYTHON=python3 spark-submit --packages ml.dhs:modelmonitor_2.11:$VERSION-SNAPSHOT setup.py test