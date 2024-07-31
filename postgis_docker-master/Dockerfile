FROM postgis/postgis:16-master
#docker build -t postgres16_postgis .
RUN apt-get update && apt-get install -y --fix-missing pgxnclient make pip git postgresql-server-dev-16  apt-utils clang-format clang-tidy g++ zstd
RUN apt-get update && apt-get install -y python3-requests postgresql-plpython3-${PG_MAJOR}
RUN pip install cmake --upgrade

RUN apt-get update && apt-get install -y parallel

ADD docker-entrypoint-initdb.d /docker-entrypoint-initdb.d

#https://github.com/pgspider/parquet_s3_fdw

RUN apt-get install -y openjdk-11-jdk
RUN git clone https://github.com/pgspider/jdbc_fdw.git /home/jdbc_fwd
RUN cd /home/jdbc_fwd/ && PATH=/usr/lib/postgresql/16/bin/:$PATH make USE_PGXS=1 clean
RUN mkdir /usr/lib64
RUN ln -s /usr/lib/jvm/java-11-openjdk-amd64/lib/server/libjvm.so /usr/lib/x86_64-linux-gnu/libjvm.so
RUN cd /home/jdbc_fwd/ && PATH=/usr/lib/postgresql/16/bin/:$PATH make USE_PGXS=1 install
RUN cd / && rm -rf /home/jdbc_fwd/


RUN apt-get update
RUN apt-get install -y curl
RUN curl https://install.citusdata.com/community/deb.sh | bash
RUN apt-get purge -y --auto-remove curl
RUN apt-get -y install apt-utils postgresql-16-citus-12.1
RUN echo "shared_preload_libraries='citus'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "shared_preload_libraries='citus'" >> /var/lib/postgresql/data/postgresql.conf

RUN apt-get install -y wget unzip
RUN cd /home && git clone https://github.com/alitrack/duckdb_fdw
RUN cd /home/duckdb_fdw && wget -c https://github.com/duckdb/duckdb/releases/download/v1.0.0/libduckdb-linux-amd64.zip && unzip -o -d . libduckdb-linux-amd64.zip && cp libduckdb.so $(/usr/lib/postgresql/16/bin/pg_config --libdir)
RUN cd /home/duckdb_fdw &&  make PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config USE_PGXS=1 && make PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config install USE_PGXS=1
RUN apt-get purge -y --auto-remove wget unzip

RUN pgxn install h3
