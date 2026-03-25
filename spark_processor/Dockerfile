FROM apache/spark:3.5.0

USER root

# 1. Instala o curl (necessário para baixar os Jars abaixo)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# 2. Instala dependências Python
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    redis==5.0.1 \
    structlog==23.2.0

# 3. Jars necessários para Kafka + PostgreSQL (Caminhos corrigidos para /opt/spark)
RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL -o /opt/spark/jars/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -fsSL -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fsSL -o /opt/spark/jars/postgresql-42.6.0.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

WORKDIR /app
COPY processor.py .

# Volta para o usuário spark por segurança (opcional, mas recomendado)
USER spark

CMD ["/opt/spark/bin/spark-submit", \
    "--master", "local[2]", \
    "--conf", "spark.sql.shuffle.partitions=4", \
    "--conf", "spark.streaming.stopGracefullyOnShutdown=true", \
    "processor.py"]