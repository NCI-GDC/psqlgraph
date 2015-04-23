import os
import json
import argparse
import subprocess
import shutil
from psqlgraph import psqlgraph2neo4j


def export():
    '''
    Export process which convert psqlgraph database to csv files, then use java
    batch importer to convert csv to neo4j data files.
    '''
    try:
        import gdcdatamodel
    except:
        print "psqlgraph_to_neo need gdcdatamodel package to run"
        return
    cur_dir = os.getcwd()
    bin_dir = os.path.dirname(__file__)

    parser = argparse.ArgumentParser(description="psqlgraph to neo4j")
    parser.add_argument("--host", default=os.getenv("GDC_PG_HOST"))
    parser.add_argument("--user", default=os.getenv("GDC_PG_USER"))
    parser.add_argument("--password", default=os.getenv("GDC_PG_PASSWORD"))
    parser.add_argument("--name", default=os.getenv("GDC_PG_DBNAME"))
    parser.add_argument("--out", default=os.path.join(cur_dir, 'data'))
    args = parser.parse_args()
    if not (args.host and args.user and args.password and args.name):
        print ("please provide psqlgraph credentials with"
               "--host, --user, --password, --name"
               "or set GDC_PG_HOST, GDC_PG_USER, GDC_PG_PASSWORD, "
               "GDC_PB_DBNAME environment variables.")
        return

    driver = psqlgraph2neo4j.PsqlGraph2Neo4j()
    driver.connect_to_psql(args.host, args.user, args.password, args.name)
    csv_dir = os.path.join(cur_dir, 'csv')
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    with open(os.path.join(
            gdcdatamodel.schema_src_dir, 'node_properties.avsc'), 'r') as f:
        schema = json.load(f)
    driver.export(csv_dir, schema)

    data_dir = args.out
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    print "Exporting data file to %s" % data_dir
    script = os.path.join(bin_dir, 'csv_to_neo.sh')
    batch_importer = get_batch_importer(script, cur_dir)
    subprocess.call(
        ["bash", script, "-s", csv_dir, "-d",
         data_dir, "-b", batch_importer, "convert"])
    shutil.rmtree(csv_dir)
    shutil.rmtree(batch_importer)


def get_batch_importer(script, cur_dir):
    importer = os.path.join(cur_dir, 'batch_importer')
    if os.path.exists(importer):
        return importer
    else:
        subprocess.call(["bash", script, "setup"])
        return importer

if __name__ == "__main__":
    export()
