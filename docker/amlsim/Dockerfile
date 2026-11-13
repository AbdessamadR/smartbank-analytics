FROM eclipse-temurin:8-jdk

ENV DEBIAN_FRONTEND=noninteractive
ENV MAVEN_VERSION=3.9.9
ENV MINIFORGE_VERSION=24.11.3-0

WORKDIR /opt/AMLSim

# Outils système
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    tar \
    gzip \
    && rm -rf /var/lib/apt/lists/*

# Installation Maven
RUN curl -fsSL \
    https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
    -o /tmp/maven.tar.gz \
    && tar -xzf /tmp/maven.tar.gz -C /opt \
    && ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/local/bin/mvn \
    && rm /tmp/maven.tar.gz

# Installation Miniforge
RUN curl -fsSL \
    https://github.com/conda-forge/miniforge/releases/download/${MINIFORGE_VERSION}/Miniforge3-Linux-x86_64.sh \
    -o /tmp/miniforge.sh \
    && bash /tmp/miniforge.sh -b -p /opt/conda \
    && rm /tmp/miniforge.sh

ENV PATH="/opt/conda/bin:${PATH}"

# Environnement Python 3.7 pour AMLSim
RUN conda create -y -n amlsim python=3.7 \
    && conda clean -afy

ENV PATH="/opt/conda/envs/amlsim/bin:${PATH}"

# Dépendances Python AMLSim
RUN pip install --no-cache-dir \
    numpy==1.16.6 \
    networkx==1.11 \
    python-dateutil \
    Faker

# Copie du dépôt officiel AMLSim
COPY external/AMLSim/ /opt/AMLSim/

# Installation de MASON 20 dans le dépôt Maven local
RUN mvn -B install:install-file \
    -Dfile=/opt/AMLSim/jars/mason.20.jar \
    -DgroupId=mason \
    -DartifactId=mason \
    -Dversion=20 \
    -Dpackaging=jar \
    -DgeneratePom=true

# Compilation Java
RUN mvn -B -DskipTests package

CMD ["bash"]