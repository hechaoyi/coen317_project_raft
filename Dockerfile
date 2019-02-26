FROM python:alpine
WORKDIR /app
COPY ./Pipfile ./Pipfile.lock /
RUN pip install pipenv && pipenv install --system --deploy
COPY ./src .
ENV FLASK_APP=main
ENV FLASK_ENV=development
EXPOSE 80
CMD [ "flask", "run", "--host=0.0.0.0", "--port=80", "--no-reload", "--with-threads" ]
