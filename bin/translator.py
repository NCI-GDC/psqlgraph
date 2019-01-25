#!/usr/bin/env python
import logging
import argparse
from gdcdatamodel import models
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy import Column, Integer, Text, DateTime
from sqlalchemy.orm import relationship, joinedload
from psqlgraph import Base
from sqlalchemy import ForeignKey
from multiprocessing import Pool
import sys

from psqlgraph import PsqlGraphDriver, PolyNode

logging.root.setLevel(level=logging.ERROR)

BLOCK = 4096
driver_kwargs = dict()


class OldEdge(Base):
    __tablename__ = 'edges'
    __table_args__ = None
    key = Column(Integer, primary_key=True)
    edge_id = Column(Text, nullable=False)
    src_id = Column(Text, ForeignKey(
        'nodes.node_id', deferrable=True,
        initially="DEFERRED"), nullable=False)
    dst_id = Column(Text, ForeignKey(
        'nodes.node_id', deferrable=True,
        initially="DEFERRED"), nullable=False)
    created = Column(DateTime(timezone=True), nullable=False)
    system_annotations = Column(JSONB, default={})
    label = Column(Text, nullable=False)
    properties = Column(JSONB, default={})
    src = relationship("OldNode", foreign_keys=[src_id])
    dst = relationship("OldNode", foreign_keys=[dst_id])


class OldNode(Base):
    __tablename__ = 'nodes'
    __table_args__ = (UniqueConstraint('node_id', name='_node_id_uc'),)
    key = Column(Integer, primary_key=True)
    node_id = Column(Text, nullable=False)
    label = Column(Text, nullable=False)
    created = Column(DateTime(timezone=True), nullable=False)
    acl = Column(ARRAY(Text))
    system_annotations = Column(JSONB, default={})
    properties = Column(JSONB, default={})
    edges_out = relationship("OldEdge", foreign_keys=[OldEdge.src_id])
    edges_in = relationship("OldEdge", foreign_keys=[OldEdge.dst_id])

    def get_edges(self):
        for edge_in in self.edges_in:
            yield edge_in
        for edge_out in self.edges_out:
            yield edge_out


def translate_node_range(_args):
    args, offset = _args
    src = PsqlGraphDriver(
        args.source_host, args.source_user, args.source_password,
        args.source, **driver_kwargs)
    dst = PsqlGraphDriver(
        args.dest_host, args.dest_user, args.dest_password,
        args.dest, **driver_kwargs)
    with src.session_scope() as session:
        with dst.session_scope() as session:
            for old in src.nodes(OldNode).order_by(OldNode.node_id)\
                                         .offset(offset)\
                                         .limit(BLOCK)\
                                         .yield_per(BLOCK):

                try:
                    new = PolyNode(
                        node_id=old.node_id,
                        properties=old.properties,
                        system_annotations=old.system_annotations,
                        acl=old.acl,
                        label=old.label,
                    )
                    new.created = old.created
                    session.merge(new)
                except Exception as e:
                    logging.error("unable to add node {}, {}".format(
                        old.label, old.node_id))
                    logging.error(e)

            try:
                session.commit()
            except Exception as e:
                logging.error(e)


def translate_nodes(args):
    src = PsqlGraphDriver(
        args.source_host, args.source_user, args.source_password,
        args.source, **driver_kwargs)
    with src.session_scope():
        count = src.nodes(OldNode).count()
    offsets = [i*BLOCK for i in range(count/BLOCK+1)]
    pool = Pool(args.nprocs)
    args = [(args, offset) for offset in offsets]
    pool.map_async(translate_node_range, args).get(int(1e9))


def translate_edge_range(_args):
    args, offset = _args
    src = PsqlGraphDriver(
        args.source_host, args.source_user, args.source_password,
        args.source, **driver_kwargs)
    dst = PsqlGraphDriver(
        args.dest_host, args.dest_user, args.dest_password,
        args.dest, **driver_kwargs)

    print('{}-{}'.format(offset, offset+BLOCK))
    sys.stdout.flush()
    with src.session_scope() as session:
        with dst.session_scope() as session:
            for old in src.edges(OldEdge)\
                          .order_by((OldEdge.src_id),
                                    (OldEdge.dst_id),
                                    (OldEdge.label))\
                          .options(joinedload(OldEdge.src))\
                          .options(joinedload(OldEdge.dst))\
                          .offset(offset)\
                          .limit(BLOCK).all():
                try:
                    Type = dst.get_edge_by_labels(
                        old.src.label, old.label, old.dst.label)
                    print(Type.__name__)
                    new = Type(
                        src_id=old.src_id,
                        dst_id=old.dst_id,
                        properties=old.properties,
                        system_annotations=old.system_annotations,
                        label=old.label,
                    )
                    new.created = old.created
                    session.merge(new)
                except Exception as e:
                    logging.error("unable to add edge {}, {}".format(
                        old.label, old.src_id, old.dst_id))
                    logging.error(e)

            try:
                session.commit()
            except Exception as e:
                logging.error(e)


def translate_edges(args):
    src = PsqlGraphDriver(
        args.source_host, args.source_user, args.source_password,
        args.source, **driver_kwargs)
    with src.session_scope():
        count = src.nodes(OldEdge).count()
    src.engine.dispose()
    offsets = [i*BLOCK for i in range(count/BLOCK+1)]
    pool = Pool(args.nprocs)
    args = [(args, offset) for offset in offsets]
    pool.map_async(translate_edge_range, args).get(int(1e9))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--nprocs', default=16, type=int,
                        help='number of processes')

    # ======== Destination options ========
    parser.add_argument('--dest', required=True, type=str,
                        help='the database to import to')
    parser.add_argument('--dest-user', default='test', type=str,
                        help='the user to import as')
    parser.add_argument('--dest-password', default='test', type=str,
                        help='the password for import user')
    parser.add_argument('--dest-host', default='localhost', type=str,
                        help='the postgres server host')

    # ======== Source options ========
    parser.add_argument('--source', required=True, type=str,
                        help='the database to import from')
    parser.add_argument('--source-user', default='test', type=str,
                        help='the user to import as')
    parser.add_argument('--source-password', default='test', type=str,
                        help='the password for import user')
    parser.add_argument('--source-host', default='localhost', type=str,
                        help='the postgres server host')
    args = parser.parse_args()
    translate_nodes(args)
    translate_edges(args)
