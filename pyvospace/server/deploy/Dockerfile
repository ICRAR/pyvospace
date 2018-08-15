FROM library/postgres:10
COPY *.sql /docker-entrypoint-initdb.d/
RUN apt-get update && apt-get upgrade && apt-get install -y postgresql-contrib