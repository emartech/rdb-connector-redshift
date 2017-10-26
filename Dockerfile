FROM hseeberger/scala-sbt:8u141_2.12.4_1.0.2

ADD build.sbt /rdb-connector-redshift/build.sbt
ADD project /rdb-connector-redshift/project
ADD src /rdb-connector-redshift/src

WORKDIR /rdb-connector-redshift

RUN sbt clean compile
