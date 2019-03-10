FROM python:3
WORKDIR /app
COPY ./Pipfile ./Pipfile.lock /
RUN pip install pipenv && pipenv install --system --deploy
COPY ./src .
EXPOSE 80
CMD [ "python", "main.py" ]
