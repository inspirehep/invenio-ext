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
import six

from collections import OrderedDict
from urlparse import urlparse

from elasticsearch import Elasticsearch
from elasticsearch.connection import RequestsHttpConnection
from elasticsearch.serializer import JSONSerializer
from elasticsearch.exceptions import SerializationError

es = None


class OrderedJSONSerializer(JSONSerializer):

    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, six.string_types):
            return data

        try:
            if 'size' in data:
                data = OrderedDict([
                    ('from', data.get('from', 0)),
                    ('size', data.get('size')),
                ] + [(k, v) for k, v in data.items()
                     if k not in ('size', 'from')])
            return json.dumps(data, default=self.default)
        except (ValueError, TypeError) as e:
            raise SerializationError(data, e)


# Extracted from elasticsearch library
def normalize_hosts(hosts):
    """
    Helper function to transform hosts argument to
    :class:`~elasticsearch.Elasticsearch` to a list of dicts.
    """
    # if hosts are empty, just defer to defaults down the line
    if hosts is None:
        return [{}]

    # passed in just one string
    if isinstance(hosts, six.string_types):
        hosts = [hosts]

    out = []
    # normalize hosts to dicts
    for host in hosts:
        if isinstance(host, six.string_types):
            if '://' not in host:
                host = "//%s" % host

            parsed_url = urlparse(host)
            h = {"host": parsed_url.hostname}

            if parsed_url.port:
                h["port"] = parsed_url.port

            if parsed_url.scheme == "https":
                h['port'] = parsed_url.port or 443
                h['use_ssl'] = True
                h['scheme'] = 'http'
            elif parsed_url.scheme:
                h['scheme'] = parsed_url.scheme

            if parsed_url.username or parsed_url.password:
                h['http_auth'] = '%s:%s' % (parsed_url.username, parsed_url.password)

            if parsed_url.path and parsed_url.path != '/':
                h['url_prefix'] = parsed_url.path

            out.append(h)
        else:
            out.append(host)
    return out


def setup_app(app):
    """Set up the extension for the given app."""
    global es

    hosts = app.config.get('ES_HOSTS', None)

    sniff_timeout = app.config.get('ES_SNIFF_TIMEOUT', 10)

    def get_host_info(node_info, host):
        """Simple callback that takes the node info from `/_nodes/_all/clear` and a
        parsed connection information and return the connection information.

        If `None` is returned this node will be skipped.

        By default master only nodes are filtered out since they shouldn't
        typically be used for API operations.

        :arg node_info: node information from `/_nodes/_all/clear`
        :arg host: connection information (host, port) extracted from the node info
        """
        # Only allow nodes from ES_HOSTS
        if hosts:
            accepted_node = None
            for allowed_hostname in hosts:
                if node_info.get('name') in allowed_hostname:
                    # E.g. 'nodename' in 'http://*:*@nodename.example.com:80'
                    accepted_node = allowed_hostname
                    break
            if accepted_node is not None:
                # Need to use the same internal function used upon parsing
                # ES_HOSTS so that we connect the same way. E.g. proxy support
                # and http_auth.
                host = normalize_hosts(accepted_node)[0]
            else:
                return None
        return host

    es = Elasticsearch(
        hosts,
        connection_class=RequestsHttpConnection,
        sniff_on_connection_fail=True,
        sniffer_timeout=60,
        sniff_timeout=sniff_timeout,
        retry_on_timeout=True,
        host_info_callback=get_host_info,
        timeout=60,
        serializer=OrderedJSONSerializer(),
    )
