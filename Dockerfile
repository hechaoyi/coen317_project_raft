FROM python:alpine
WORKDIR /app
COPY ./Pipfile ./Pipfile.lock /
RUN pip install pipenv && pipenv install --system --deploy
COPY ./src .
EXPOSE 80
CMD [ "python", "main.py" ]
