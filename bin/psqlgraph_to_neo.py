import sys
sys.path.extend(['..', '../../gdcdatamodel'])
import re
import requests
import os.path
import os,  argparse, subprocess, shutil
from psqlgraph import psqlgraph2neo4j


def export():
    '''
    Export process which convert psqlgraph database to csv files,
    then use Neo's java batch importer to convert csv to neo4j data files.
    '''
    try:
        from gdcdatamodel.models import *
    except:
        print("psqlgraph_to_neo needs gdcdatamodel package to run")
        return
    cur_dir = os.getcwd()

    parser = argparse.ArgumentParser(description="psqlgraph to neo4j")
    parser.add_argument("--host", default=os.getenv("GDC_PG_HOST"))
    parser.add_argument("--user", default=os.getenv("GDC_PG_USER"))
    parser.add_argument("--password", default=os.getenv("GDC_PG_PASSWORD"))
    parser.add_argument("--name", default=os.getenv("GDC_PG_DBNAME"))
    parser.add_argument("--out", default=os.path.join(cur_dir, 'data'))
    parser.add_argument("--importer_url", default="")
    parser.add_argument("--export_only", action='store_true')
    parser.add_argument("--convert_only", action='store_true')
    parser.add_argument("--cleanup", default=False)
    parser.add_argument("--index", action="store_true")
    args = parser.parse_args()
    if args.index:
        print("create index")
        psqlgraph2neo4j.create_index()
        return 
    if not args.convert_only and not (args.user and args.name):
        print('''please provide psqlgraph credentials with --host, --user, --password, --name
or set GDC_PG_HOST, GDC_PG_USER, GDC_PG_PASSWORD,
GDC_PB_DBNAME environment variables.''')
        return
    driver = psqlgraph2neo4j.PsqlGraph2Neo4j()
    driver.connect_to_psql(args.host, args.user, args.password, args.name)
    csv_dir = os.path.join(cur_dir, 'csv')
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
    if args.export_only or not (args.export_only or args.convert_only):
        print("Exporting psqlgraph to csv")
        with driver.psqlgraphDriver.session_scope():
            driver.export(csv_dir)
        print("-Done")
    data_dir = args.out
    if args.convert_only or not (args.export_only or args.convert_only):
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        batch_importer = get_batch_importer(cur_dir, args.importer_url)
        convert_csv(csv_dir, data_dir, batch_importer)

    if args.cleanup:
        if (not args.export_only):
            shutil.rmtree(csv_dir)
        shutil.rmtree(batch_importer)


def get_batch_importer(cur_dir, url=''):
    importer = os.path.join(cur_dir, 'batch_importer')
    if os.path.exists(importer):
        return importer
    else:
        os.makedirs(importer)
        if url:
            print("Downloading Neo4j batch importer")
            r = requests.get(url, stream=True)
            zipfile = os.path.basename(url)
            with open(zipfile, "wb") as zipf:
                for chunk in r.iter_content(100*1024):
                    zipf.write(chunk)

        else:
            bin_dir = os.path.dirname(__file__)
            zipfile = os.path.join(bin_dir, 'batch_importer.zip')

        subprocess.check_call(['unzip', zipfile, '-d', importer])
        if os.path.exists(os.path.join(importer, 'import.sh')):
            if url:
                os.remove(zipfile)
            return importer
        else:
            raise RuntimeError(
                'Problem with batch importer retrieval or unzip')
        print("-Done")


def convert_csv(csv_dir, data_dir, importer_dir):
    if not os.path.exists(csv_dir):
        raise RuntimeError("Can't find directory '%s'" % csv_dir)
    node_files = [x for x in os.listdir(csv_dir) if re.match('nodes.*\.csv$', x)]
    edge_files = [x for x in os.listdir(csv_dir) if re.match('rels.*\.csv$', x)]
    pwd = os.getcwd()
    os.chdir(importer_dir)
    cmd = ['bash', './import.sh', data_dir,
           ','.join([os.path.join(csv_dir, i) for i in node_files]),
           ','.join([os.path.join(csv_dir, i) for i in edge_files])]
    subprocess.check_call(cmd)
    os.chdir(pwd)


if __name__ == "__main__":
    export()
