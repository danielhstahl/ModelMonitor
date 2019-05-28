#FROM docker.repository.cloudera.com/cdsw/engine:6
## do I need sbt?  I guess I need to install SBT so I can compile, but I need cdsw so I can run a spart environment for testing

FROM hseeberger/scala-sbt
COPY . .
RUN sbt clean coverage test coverageReport