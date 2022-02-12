FROM python:3.7

COPY . /app
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir /project
COPY scripts_airflow/ /project/scripts/

RUN chmod +x /project/scripts/init.sh
ENTRYPOINT [ "/project/scripts/init.sh" ]