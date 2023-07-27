create database dev;

create user airflow with encrypted password 'airflow';

grant all privileges on database dev to airflow;

\c dev airflow
create schema stock AUTHORIZATION airflow;