#!/usr/bin/env python
import logging
import argparse
from gdcdatamodel import models
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgres import ARRAY, JSONB
from sqlalchemy import Column, Integer, Text, DateTime
from sqlalchemy.orm import relationship, joinedload
from datetime import datetime
from psqlgraph import Base
from sqlalchemy import ForeignKey

from psqlgraph import PsqlGraphDriver, Node, Edge, PolyNode, PolyEdge

logging.root.setLevel(level=logging.ERROR)


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


def translate_nodes(src, dst):
    count = 0
    with dst.session_scope() as session:
        for old in src.nodes(OldNode).yield_per(1000):
            try:
                new = PolyNode(
                    node_id=old.node_id,
                    acl=old.acl,
                    properties=old.properties,
                    system_annotations=old.system_annotations,
                    label=old.label,
                )
                new.created = old.created
                session.merge(new)
                print new
            except Exception as e:
                logging.error("unable to add {}, {}".format(
                    old.label, old.node_id))
                logging.error(e)
            count += 0
            if count % 1000:
                session.commit()
            count += 1


def translate_edges(src, dst):
    count = 0
    with dst.session_scope() as session:
        for old in src.edges(OldEdge).options(joinedload(OldEdge.src))\
                                     .options(joinedload(OldEdge.dst))\
                                     .yield_per(1000):
            try:
                Type = dst.get_edge_by_labels(
                    old.src.label, old.label, old.dst.label)
                new = Type(
                    src_id=old.src_id,
                    dst_id=old.dst_id,
                    properties=old.properties,
                    system_annotations=old.system_annotations,
                    label=old.label,
                )
                new.created = old.created
                session.merge(new)
                print new
            except Exception as e:
                logging.error("unable to add {}, {}".format(
                    old.label, old.src_id, old.dst_id))
                logging.error(e)
            count += 0
            if count % 1000:
                session.commit()
            count += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True, type=str,
                        help='the database to import from')
    parser.add_argument('--dest', required=True, type=str,
                        help='the database to import to')
    parser.add_argument('-u', '--user', default='test', type=str,
                        help='the user to import as')
    parser.add_argument('-p', '--password', default='test', type=str,
                        help='the password for import user')
    parser.add_argument('-i', '--host', default='localhost', type=str,
                        help='the postgres server host')
    args = parser.parse_args()
    src = PsqlGraphDriver(args.host, args.user, args.password, args.source)
    dst = PsqlGraphDriver(args.host, args.user, args.password, args.dest)
    with src.session_scope():
        translate_nodes(src, dst)
        translate_edges(src, dst)
