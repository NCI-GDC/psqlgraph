import argparse
import getpass
import psqlgraph
from psqlgraph import Node, VoidedNode, PolyNode
from sqlalchemy import func

try:
    import IPython
    ipython = True
except Exception as e:
    print '{}, using standard interactiv console'.format(e)
    import code
    ipython = False

message = """
Entering psqlgraph console:
    database : {}
    host     : {}
    user     : {}

NOTE:
    PsqlGraphDriver stored in local variable `g`.
    `g.session_scope` is aliased as `ss`.
"""

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', default='test', type=str,
                        help='name of the database to connect to')
    parser.add_argument('-i', '--host', default='localhost', type=str,
                        help='host of the postgres server')
    parser.add_argument('-u', '--user', default='test', type=str,
                        help='user to connect to postgres as')
    parser.add_argument('-p', '--password', default=None, type=str,
                        help='password for given user. If no '
                        'password given, one will be prompted.')

    args = parser.parse_args()
    print(message.format(args.database, args.host, args.user))
    if not args.password:
        args.password = getpass.getpass()

    g = psqlgraph.PsqlGraphDriver(**args.__dict__)
    ss = g.session_scope

    with g.session_scope() as s:
        rb = s.rollback
        if ipython:
            IPython.embed()
        else:
            code.InteractiveConsole(locals=globals()).interact()
