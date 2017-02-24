# -*- coding: utf-8 -*-
"""
tests.postgres.data
----------------------------------

Test data for postgres tests
"""

import random
import string
import uuid

from models import StateMemberOfCity, City, State

def random_string(length=6):
    return ''.join([
        random.choice(
            string.ascii_lowercase + string.digits
        ) for _ in range(length)
    ])


def fuzzed(node_class, node_id=None, **kwargs):
    if node_id is None:
        node_id = str(uuid.uuid4())

    kwargs['key0'] = random_string()
    kwargs['key1'] = random_string()

    return node_class(node_id, **kwargs)


nodes = [
    fuzzed(State, 'b45d5379-97e2-4d67-a72c-d8debeabae8f'),
    fuzzed(State, '5d769874-21f4-4a50-8588-aa3df3e95633'),
    fuzzed(State, 'f45d91e6-78de-4e57-a509-9b122f67aeed'),
    fuzzed(State, '9c911974-bb2e-4dfa-9835-5aff3f806014'),
    fuzzed(State, '36a79a86-dacd-4168-89ef-8a1d07438aa2'),
    fuzzed(State, '3cae58b5-4643-41a5-84da-c9eceea1769d'),
    fuzzed(State, 'dddbca5a-f212-42a2-9a4b-c494ae73cda9'),
    fuzzed(State, '737a64cd-ae81-4d8a-a909-60c181b92fd7'),
    fuzzed(State, '19b44636-3fb4-4565-852d-13f042603ff4'),
    fuzzed(State, '0bc4070e-d505-41d9-bda5-7eae41196d23'),
    fuzzed(State, 'ad6ad4f8-0bad-4a3b-9a33-9d54515718b0'),
    fuzzed(State, 'eb7c332f-1df6-48aa-921f-b354313176ba'),
    fuzzed(State, '9d80bdbc-914f-42cb-b5fd-9b199ee2ef54'),
    fuzzed(State, 'cbd355d1-8f18-4dbd-88aa-e39e204fc317'),
    fuzzed(State, '83baeed6-e658-4656-a334-a118aac1c67d'),
    fuzzed(State, '8c026560-dbc6-47bf-8602-e8bcaecc1931'),
    fuzzed(State, 'e865d8cc-97c7-4329-8e00-919a48b53f1b'),

    fuzzed(City, 'cfe7b1c4-2169-4328-b26c-84ed521e7612'),
]


edges = [
    StateMemberOfCity(
        'b45d5379-97e2-4d67-a72c-d8debeabae8f',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        '5d769874-21f4-4a50-8588-aa3df3e95633',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        'f45d91e6-78de-4e57-a509-9b122f67aeed',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        '9c911974-bb2e-4dfa-9835-5aff3f806014',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        '36a79a86-dacd-4168-89ef-8a1d07438aa2',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        '3cae58b5-4643-41a5-84da-c9eceea1769d',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        'dddbca5a-f212-42a2-9a4b-c494ae73cda9',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        '737a64cd-ae81-4d8a-a909-60c181b92fd7',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        '19b44636-3fb4-4565-852d-13f042603ff4',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        '0bc4070e-d505-41d9-bda5-7eae41196d23',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        'ad6ad4f8-0bad-4a3b-9a33-9d54515718b0',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        'eb7c332f-1df6-48aa-921f-b354313176ba',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        '9d80bdbc-914f-42cb-b5fd-9b199ee2ef54',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        'cbd355d1-8f18-4dbd-88aa-e39e204fc317',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        '83baeed6-e658-4656-a334-a118aac1c67d',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        '8c026560-dbc6-47bf-8602-e8bcaecc1931',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
    StateMemberOfCity(
        'e865d8cc-97c7-4329-8e00-919a48b53f1b',
        'cfe7b1c4-2169-4328-b26c-84ed521e7612'
    ),
]



def insert(g):
    with g.session_scope() as session:
        for node in nodes:
            session.merge(node)
        for edge in edges:
            session.merge(edge)
