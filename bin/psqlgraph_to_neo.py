import sys
sys.path.extend(['..','../../gdcdatamodel'])
import re
import requests
import os.path
import os,json,argparse, subprocess,shutil
from pdb import set_trace
from psqlgraph import psqlgraph2neo4j
    
def export():
    '''
    Export process which convert psqlgraph database to csv files, then use Neo's 
    java batch importer to convert csv to neo4j data files.
    '''
    try: 
        import gdcdatamodel
    except Exception as e:
        print "psqlgraph_to_neo needs gdcdatamodel package to run"
        return
    cur_dir = os.getcwd()
    bin_dir = os.path.dirname(__file__)

    parser = argparse.ArgumentParser(description="psqlgraph to neo4j")
    parser.add_argument("--host",default = os.getenv("GDC_PG_HOST"))
    parser.add_argument("--user",default = os.getenv("GDC_PG_USER"))
    parser.add_argument("--password",default = os.getenv("GDC_PG_PASSWORD"))
    parser.add_argument("--name",default = os.getenv("GDC_PG_DBNAME"))
    parser.add_argument("--out",default = os.path.join(cur_dir,'data'))
    parser.add_argument("--importer_url",default="https://dl.dropboxusercontent.com/u/14493611/batch_importer_20.zip")
    parser.add_argument("--export_only", action='store_true')
    parser.add_argument("--convert_only", action='store_true')
    parser.add_argument("--cleanup",default = False)
    args = parser.parse_args()
    if not (args.host and args.user and args.name):
        print '''please provide psqlgraph credentials with --host, --user, --password, --name
or set GDC_PG_HOST, GDC_PG_USER, GDC_PG_PASSWORD, GDC_PB_DBNAME environment variables.'''
        return
    driver = psqlgraph2neo4j.PsqlGraph2Neo4j()
    driver.connect_to_psql(args.host,args.user,args.password,args.name)
    csv_dir = os.path.join(cur_dir,'csv')
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
    if args.export_only or not (args.export_only or args.convert_only):
        with open(os.path.join(
            gdcdatamodel.schema_src_dir,'node_properties.avsc'),'r') as f:
            schema = json.load(f)
        print "Exporting psqlgraph to csv"
        try:
            driver.export(csv_dir,schema)
        except Exception as e:
            set_trace()
            pass
        print "-Done"
    data_dir = args.out 
    if args.convert_only or not (args.export_only or args.convert_only):
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        batch_importer = get_batch_importer(cur_dir,args.importer_url)
        convert_csv(csv_dir,data_dir,os.path.dirname(batch_importer))
    
    if args.cleanup:
        if (not args.export_only):
            shutil.rmtree(csv_dir)
        shutil.rmtree(batch_importer)
    
def get_batch_importer(cur_dir,url):
    importer = os.path.join(cur_dir,'batch_importer')
    if os.path.exists(importer):
        return importer
    else:
        print "Downloading Neo4j batch importer"
        r = requests.get(url,stream=True)
        zipfile = os.path.basename(url)
        with open(zipfile, "wb") as zipf:
            for chunk in r.iter_content(100*1024):
                zipf.write(chunk)

        subprocess.check_call(['unzip',zipf])
        if os.path.exists( os.path.join(cur_dir,'batch_importer','import.sh') ):
            os.remove(zipfile)
            return importer
        else:
            raise RuntimeError('Problem with batch importer retrieval or unzip')
        print "-Done"
def convert_csv(csv_dir,data_dir,importer_dir):
    if not os.path.exists(csv_dir):
        raise RuntimeError("Can't find directory '%s'" % csv_dir)
    files=filter(re.match('(nodes.*|rels.*)\.csv$', os.listdir(csv_dir)))
    pwd=os.getcwd()
    chdir(importer_dir)
    cmd = ['./import.sh',data_dir]
    cmd.extend(files)
    subprocess.check_call(cmd)
    chdir(pwd)
    
if __name__ == "__main__":
    export()

