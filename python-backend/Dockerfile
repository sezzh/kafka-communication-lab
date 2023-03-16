FROM python:3.9.16-bullseye

WORKDIR /usr/src/app

COPY ./python-backend/requirements.txt .

RUN pip install -r requirements.txt

COPY ./python-backend .

EXPOSE 3000

# CMD [ "python", "app.py" ]

CMD [ "flask", "--app", "app", "run", "--host=0.0.0.0" ]