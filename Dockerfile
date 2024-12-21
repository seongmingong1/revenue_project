FROM apache/airflow:2.10.2
USER root
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev gcc \
    && apt-get clean
USER airflow
RUN pip install mysqlclient pymysql mysql-connector-python \
    pandas \
    numpy \
    python-dotenv \
    forex-python \
    requests \
    pendulum \
    openpyxl