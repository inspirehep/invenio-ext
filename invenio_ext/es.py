# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015 CERN.
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""Simplified Elastic Search integration."""

from __future__ import absolute_import

import json

from elasticsearch import Elasticsearch
from elasticsearch.connection import RequestsHttpConnection

from invenio.base.globals import cfg
from invenio_search.registry import mappings

es = None


def create_index(sender, **kwargs):
    """Create or recreate the elasticsearch index for records."""
    indices = set(cfg["SEARCH_ELASTIC_COLLECTION_INDEX_MAPPING"].values())
    indices.add(cfg['SEARCH_ELASTIC_DEFAULT_INDEX'])
    for index in indices:
        mapping = {}
        mapping_filename = index + ".json"
        if mapping_filename in mappings:
            mapping = json.load(open(mappings[mapping_filename], "r"))
        es.indices.delete(index=index, ignore=404)
        es.indices.create(index=index, body=mapping)


def delete_index(sender, **kwargs):
    """Create the elasticsearch index for records."""
    indices = set(cfg["SEARCH_ELASTIC_COLLECTION_INDEX_MAPPING"].values())
    indices.add(cfg['SEARCH_ELASTIC_DEFAULT_INDEX'])
    for index in indices:
        es.indices.delete(index=index, ignore=404)


def setup_app(app):
    """Set up the extension for the given app."""
    from invenio_base import signals
    from invenio_base.scripts.database import recreate, drop, create

    global es

    es = Elasticsearch(
        app.config.get('ES_HOSTS', None),
        connection_class=RequestsHttpConnection,
        sniff_on_start=True,
        sniff_on_connection_fail=True,
        sniffer_timeout=60,
        retry_on_timeout=True,
    )

    signals.pre_command.connect(delete_index, sender=drop)
    signals.pre_command.connect(create_index, sender=create)
    signals.pre_command.connect(delete_index, sender=recreate)
    signals.pre_command.connect(create_index, sender=recreate)
