FROM quay.io/astronomer/astro-runtime:8.10.0

RUN git clone https://github.com/The-Academic-Observatory/observatory-platform.git
RUN pip install -e ./observatory-platform/observatory-api --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-no-providers-3.10.txt
RUN pip install -e ./observatory-platform/observatory-platform --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-no-providers-3.10.txt

RUN cd ..
COPY academic-observatory-workflows ./academic-observatory-workflows
RUN pip install -e ./academic-observatory-workflows --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-no-providers-3.10.txt