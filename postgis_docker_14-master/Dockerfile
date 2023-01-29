FROM postgis/postgis:14-3.2
#citusdata/citus:11.0.3
RUN apt-get update
RUN apt-get install -y pgxnclient make cmake git postgresql-server-dev-all clang-format clang-tidy g++ zstd
RUN pgxn install h3

RUN apt-get update && apt-get install -y parallel

ADD docker-entrypoint-initdb.d /docker-entrypoint-initdb.d
#https://hub.docker.com/layers/citus/citusdata/citus/11.0.3/images/sha256-ac0f30d1def274c23e7741e02ba242f58a6df95a8eb54530c03261d4df160587?context=explore

#https://github.com/pgspider/parquet_s3_fdw

RUN apt-get install -y openjdk-11-jdk
RUN git clone https://github.com/pgspider/jdbc_fdw.git /home/jdbc_fwd --branch v0.2.0
RUN cd /home/jdbc_fwd/ && PATH=/usr/lib/postgresql/14/bin/:$PATH make USE_PGXS=1 clean
RUN mkdir /usr/lib64
RUN ln -s /usr/lib/jvm/java-11-openjdk-amd64/lib/server/libjvm.so /usr/lib64/libjvm.so
RUN cd /home/jdbc_fwd/ && PATH=/usr/lib/postgresql/14/bin/:$PATH make USE_PGXS=1 install
RUN cd / && rm -rf /home/jdbc_fwd/

#LD_LIBRARY_PATH=/usr/lib/jvm/java-11-openjdk-amd64/lib/server/
RUN cd /home && git clone https://github.com/heterodb/pg-strom.git
RUN cd /home/pg-strom/arrow-tools && make PG_CONFIG=/usr/lib/postgresql/14/bin/pg_config PROG=pg2arrow install-pg2arrow

RUN apt-get update && apt-get install -y maven gcc libgssapi-krb5-2 libkrb5-dev libsasl2-modules-gssapi-mit
RUN cd /home && git clone https://github.com/tada/pljava.git
RUN cd /home/pljava && mvn clean install -Dpgsql.pgconfig=/usr/lib/postgresql/14/bin/pg_config
RUN java -Dorg.postgresql.pljava.policy.enforcement=none -jar /home/pljava/pljava-packaging/target/pljava-pg14.jar
ADD pljava.policy /etc/postgresql-common/pljava.policy
ADD mvn-classloader-1.11.jar /usr/share/postgresql/14/pljava/mvn-classloader-1.11.jar

#docker build -t postgis_java .

RUN apt-get install -y curl
RUN curl https://install.citusdata.com/community/deb.sh | bash
RUN apt-get purge -y --auto-remove curl
RUN apt-get -y install apt-utils postgresql-14-citus-11.0
RUN echo "shared_preload_libraries='citus'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "shared_preload_libraries='citus'" >> /var/lib/postgresql/data/postgresql.conf

#https://github.com/adjust/parquet_fdw
RUN apt-get install -y wget && wget https://apache.jfrog.io/artifactory/arrow/debian/apache-arrow-apt-source-latest-bullseye.deb && apt-get install -y -V ./apache-arrow-apt-source-latest-bullseye.deb && apt-get update
RUN apt install -y libarrow-dev
RUN apt-get install -y libparquet-dev
RUN cd /home && git clone https://github.com/adjust/parquet_fdw.git && cd /home/parquet_fdw && make install