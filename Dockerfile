FROM apache/beam_python3.7_sdk:2.25.0

WORKDIR /src

COPY apache_basic.py /src/
COPY bigquery-demo-385800-0deb753c8487.json /src/

CMD [ "python3", "/src/apache_basic.py" ]
