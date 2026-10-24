FROM adoptopenjdk/maven-openjdk11

#build with: docker build -t openstreetmap_h3 .
#and run it with following command: docker run -it --rm -w $(pwd) -v $(pwd):/$(pwd) openstreetmap_h3:latest -source_pbf $(pwd)/your.pbf -result_in_tsv true

RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common

RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - && \
    add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" && \
    apt-get update && apt-get install -y docker-ce-cli

ADD pom.xml /openstreetmap_h3/pom.xml
ADD extension-api/pom.xml /openstreetmap_h3/extension-api/pom.xml
ADD extension-api/src/main /openstreetmap_h3/extension-api/src/main
ADD loader/pom.xml /openstreetmap_h3/loader/pom.xml
ADD loader/src/main /openstreetmap_h3/loader/src/main
ADD poi-organicmaps/pom.xml /openstreetmap_h3/poi-organicmaps/pom.xml
ADD poi-organicmaps/src/main /openstreetmap_h3/poi-organicmaps/src/main
RUN cd /openstreetmap_h3 && mvn package -DskipTests
RUN mv /openstreetmap_h3/loader/target/osm-to-pgsnapshot-schema-ng-1.0-SNAPSHOT.jar /openstreetmap_h3.jar
RUN mkdir /plugins && mv /openstreetmap_h3/poi-organicmaps/target/osm-to-pgsnapshot-poi-organicmaps-1.0-SNAPSHOT.jar /plugins/
ENV LOADER_PATH=/plugins
RUN rm -rf /root/.m2/repository && rm -rf /openstreetmap_h3

COPY --from=mschilde/osmium-tool /usr/bin/osmium /usr/bin/osmium
COPY --from=mschilde/osmium-tool /usr/lib/x86_64-linux-gnu/* /usr/lib/x86_64-linux-gnu/

ENTRYPOINT ["java","-jar","/openstreetmap_h3.jar","-osmium_docker","false"]
CMD ["--help"]