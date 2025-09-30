#!/bin/bash

CORRETO_JDK_VERSION=${CORRETO_JDK_VERSION:-21}

install_java_if_missing() {
#  if ! command -v java &> /dev/null; then
    echo "Java não encontrado. Instalando o Amazon Corretto ${CORRETO_JDK_VERSION}..."

    wget "https://corretto.aws/downloads/latest/amazon-corretto-${CORRETO_JDK_VERSION}-x64-linux-jdk.tar.gz" -O /tmp/amazon-corretto.tar.gz
    sudo mkdir -p "/opt/jdk/amazon-corretto-${CORRETO_JDK_VERSION}"
    sudo tar -xzf /tmp/amazon-corretto.tar.gz -C "/opt/jdk/amazon-corretto-${CORRETO_JDK_VERSION}" --strip-components=1

    export PATH="/opt/jdk/amazon-corretto-${CORRETO_JDK_VERSION}/bin:$PATH"
    echo "export JAVA_HOME=/opt/jdk/amazon-corretto-${CORRETO_JDK_VERSION}" >> ~/.zshrc
    echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.zshrc

    rm /tmp/amazon-corretto.tar.gz

    echo "Amazon Corretto ${CORRETO_JDK_VERSION} instalado com sucesso."
    echo "Versão do Java: $(java -version 2>&1 | head -n 1)"
#  else
#    echo "Java já está instalado. Versão: $(java -version 2>&1 | head -n 1)"
#  fi
}

install_java_if_missing