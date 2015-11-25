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

from elasticsearch import Elasticsearch
from elasticsearch.connection import RequestsHttpConnection

es = None


def setup_app(app):
    """Set up the extension for the given app."""
    global es

    hosts = app.config.get('ES_HOSTS', None)

    sniff_timeout = app.config.get('ES_SNIFF_TIMEOUT', 10)

    def get_host_info(node_info, host):
        """Simple callback that takes the node info from `/_cluster/nodes` and a
        parsed connection information and return the connection information.

        If `None` is returned this node will be skipped.

        By default master only nodes are filtered out since they shouldn't
        typically be used for API operations.

        :arg node_info: node information from `/_cluster/nodes`
        :arg host: connection information (host, port) extracted from the node info
        """
        # Only allow nodes from ES_HOSTS
        if hosts:
            found = False
            for allowed_node in hosts:
                if node_info.get('host') in allowed_node:
                    found = True
            if not found:
                return None
        return host

    es = Elasticsearch(
        hosts,
        connection_class=RequestsHttpConnection,
        sniff_on_connection_fail=True,
        sniffer_timeout=60,
        sniff_timeout=sniff_timeout,
        retry_on_timeout=True,
        host_info_callback=get_host_info
    )
