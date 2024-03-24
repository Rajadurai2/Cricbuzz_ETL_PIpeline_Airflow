FROM apache/airflow:2.7.3

COPY requirements.txt /requirements.txt

RUN pip install --user --upgrade pip


RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" --user -r /requirements.txt

COPY ./dags/ /opt/airflow/dags

COPY get_schedule.py .

RUN mkdir Transformed_files

RUN mkdir Scraped__raw_files

RUN mkdir Loaded_files

RUN mkdir Loaded_files/ball_data

RUN mkdir Loaded_files/match_data





