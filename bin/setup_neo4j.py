#!/user/bin/python
import argparse
from subprocess import call
import re
import os

default_neo4j_url = 'http://neo4j.com/artifact.php?name=neo4j-community-2.1.6-unix.tar.gz'
p_tar = re.compile('(.*)\?name=(.*)')
p_dir = re.compile('(.*)\?name=(.*)(-unix\.tar\.gz)')


def download_and_extract(url):
    tar_path = p_tar.match(url).group(2)
    dir_path = p_dir.match(url).group(2)
    print 'tar_path', tar_path
    print 'dir_path', dir_path
    if not os.path.exists(tar_path):
        call(['wget','-O',tar_path, url])
    if not os.path.exists(dir_path):
        call(['tar', '-zxf', tar_path])


def start_neo4j(url):
    dir_path = p_dir.match(url).group(2)
    conf = os.path.join(dir_path, 'conf', 'neo4j.properties')
    call(['sed','-i','s/^#allow/allow/g', conf])

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--url", type=str, action="store",
                        default=default_neo4j_url, help="neo4j source url")

    args = parser.parse_args()
    download_and_extract(args.url)
    start_neo4j(args.url)
