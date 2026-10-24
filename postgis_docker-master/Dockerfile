FROM postgis/postgis:19rc1-master
#docker build -t postgres19_postgis .
RUN apt-get update && apt-get install -y --no-install-recommends parallel && rm -rf /var/lib/apt/lists/*

ADD docker-entrypoint-initdb.d /docker-entrypoint-initdb.d
