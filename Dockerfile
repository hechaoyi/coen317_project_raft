FROM tiangolo/meinheld-gunicorn-flask:python3.7-alpine3.8
COPY ./Pipfile ./Pipfile.lock /
RUN pip install pipenv && pipenv install --system --deploy
COPY ./src .
