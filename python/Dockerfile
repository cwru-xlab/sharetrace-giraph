FROM public.ecr.aws/lambda/python:3.8

WORKDIR /code

COPY sharetrace/app.py requirements.txt ./

RUN python3.8 -m pip install -r requirements.txt -t .

COPY sharetrace/ .

CMD ["app.handle"]
