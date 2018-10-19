FROM library/postgres:11
COPY *.sql /docker-entrypoint-initdb.d/
RUN apt-get update && apt-get upgrade -y && apt-get install -y postgresql-contrib
