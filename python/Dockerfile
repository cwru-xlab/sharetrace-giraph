FROM amazon/aws-lambda-python:3.8

RUN /var/lang/bin/python3.8 -m pip install --upgrade pip

COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

COPY sharetrace/ /var/task

CMD ["app.handle"]
