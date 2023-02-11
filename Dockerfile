FROM bitnami/spark:latest

WORKDIR /model_conversion
USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
RUN export PYSPARK_SUBMIT_ARGS="--master spark://127.0.0.1:6666"
RUN pip install py4j
RUN pip install notebook
RUN pip install pandas
RUN pip install numpy
RUN pip install matplotlib
RUN pip install scikit-learn
RUN pip install seaborn
RUN pip install torch==1.8.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
RUN pip install sparktorch
RUN pip install joblibspark
RUN pip install vnstock
RUN pip install prophet
RUN echo "spark.sql.autoBroadcastJoinThreshold=-1" >> /opt/bitnami/spark/conf/spark-defaults.conf.template
# set java.arg.2=-Xms512m
RUN echo "spark.driver.memory=10g" >> /opt/bitnami/spark/conf/spark-defaults.conf
RUN echo "spark.executor.memory=10g" >> /opt/bitnami/spark/conf/spark-defaults.conf.template
ENTRYPOINT [ "/bin/bash" ]
