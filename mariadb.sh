docker run --name mariadb -d -p 3306:3306 --restart=always -e MYSQL_ROOT_PASSWORD=root mariadb

docker exec -it dd4f752ab3bf /bin/bash

mariadb -u root -p
create database stock
create user 'airflow'@'%' identified by 'airflow';
grant all privileges on stock.* to 'airflow'@'%';
flush privileges;