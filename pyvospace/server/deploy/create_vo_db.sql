CREATE USER "vos_user";
ALTER USER "vos_user" WITH PASSWORD 'vos_user';
CREATE DATABASE vospace WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'en_US.utf8' LC_CTYPE = 'en_US.utf8';
ALTER DATABASE vospace OWNER TO "vos_user";
GRANT ALL PRIVILEGES ON DATABASE vospace TO "vos_user";