FROM hseeberger/scala-sbt:8u141-jdk_2.12.3_0.13.16

ADD build.sbt /rdb-connector-redshift/build.sbt
ADD project /rdb-connector-redshift/project
ADD src /rdb-connector-redshift/src

WORKDIR /rdb-connector-redshift

RUN sbt clean compile
