#!/bin/bash
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg 9.4" >> /etc/apt/sources.list.d/pgdg.list'
sudo apt-get update
sudo service postgresql stop
sudo apt-get remove postgresql postgresql-9.3 --purge
sudo apt-get install postgresql-9.4 postgresql-client-9.4 postgresql-server-dev-9.4 postgresql-contrib-9.4
sudo sed -i s/peer/trust/g /etc/postgresql/9.4/main/pg_hba.conf
sudo service postgresql restart
python psqlgraph/setup_psql_graph.py