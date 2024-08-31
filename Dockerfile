FROM adoptopenjdk/maven-openjdk11

#build with: docker build -t openstreetmap_h3 .
#and run it with following command: docker run -it --rm -w $(pwd) -v $(pwd):/$(pwd) -v /var/run/docker.sock:/var/run/docker.sock openstreetmap_h3:latest -source_pbf $(pwd)/your.pbf -result_in_tsv true

RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common

RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - && \
    add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" && \
    apt-get update && apt-get install -y docker-ce-cli

ADD pom.xml /openstreetmap_h3/pom.xml
ADD src/main /openstreetmap_h3/src/main
RUN cd /openstreetmap_h3 && mvn package -DskipTests
RUN mv /openstreetmap_h3/target/osm-to-pgsnapshot-schema-ng-1.0-SNAPSHOT.jar /openstreetmap_h3.jar
RUN rm -rf /root/.m2/repository && rm -rf /openstreetmap_h3

COPY --from=mschilde/osmium-tool /usr/bin/osmium /usr/bin/osmium
COPY --from=mschilde/osmium-tool /usr/lib/x86_64-linux-gnu/* /usr/lib/x86_64-linux-gnu/

ENTRYPOINT ["java","-jar","/openstreetmap_h3.jar"]
CMD ["--help"]