FROM apache/beam_python3.7_sdk:2.25.0

WORKDIR /src

COPY apache_basic.py /src/

CMD [ "python3", "/src/apache_basic.py" ]
