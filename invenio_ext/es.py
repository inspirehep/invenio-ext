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

SEARCH_RECORD_MAPPING = {
    "settings": {
        "analysis": {
            "filter": {
                "asciifold_with_orig": {
                    "type": "asciifolding",
                    "preserve_original": True
                },

                "synonyms_kbr": {
                    "type": "synonym",
                    "synonyms": [
                        "production => creation"
                    ]
                }
            },
            "analyzer": {
                "natural_text": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "asciifold_with_orig",
                        "lowercase",
                        "synonyms_kbr"
                    ]
                },
                "basic_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "asciifold_with_orig",
                        "lowercase"
                    ]
                }
            }
        },
        "index.percolator.map_unmapped_fields_as_string": True,
    },
    "mappings": {
        "record": {
            "_all": {"enabled": False},
            "date_detection": False,
            "numeric_detection": False,
            "dynamic_templates": [
                {"default": {
                    "match_mapping_type": "string",
                    "mapping": {
                        "analyzer": "basic_analyzer",
                        "type": "string",
                        "copy_to": "global_default"
                    }
                }
                }
            ],
            "properties": {
                "global_fulltext": {
                    "type": "string",
                    "analyzer": "natural_text"
                },
                "global_default": {
                    "type": "string",
                    "analyzer": "basic_analyzer"
                },
                "_collections": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "collections": {
                    "properties": {
                        "primary": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "secondary": {
                            "type": "string",
                            "index": "not_analyzed"
                        }
                    }
                },
                "authors": {
                    "type": "string",
                    "fields": {
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed"
                        }
                    }
                },
                "main_entry_personal_name": {
                    "type": "object",
                    "properties": {
                        "personal_name": {
                            "type": "string",
                            "copy_to": ["authors"],
                            "analyzer": "natural_text"
                        }
                    }
                },
                "added_entry_personal_name": {
                    "type": "object",
                    "properties": {
                        "personal_name": {
                            "type": "string",
                            "copy_to": ["authors"],
                            "analyzer": "natural_text"
                        }
                    }
                },
                "abstract": {
                    "type": "string",
                    "analyzer": "natural_text"
                },
                "title": {
                    "type": "string",
                    "analyzer": "natural_text"
                },
                "title_statement": {
                    "type": "object",
                    "properties": {
                        "title": {
                            "type": "string", "copy_to": ["title", "global_fulltext"],
                            "analyzer": "natural_text"
                        }
                    }
                },
                "division": {
                    "type": "string"
                },
                "experiment": {
                    "type": "string"
                },
                "varying_form_of_title": {
                    "type": "object",
                    "properties": {
                        "title_proper_short_title": {
                            "type": "string", "copy_to": ["title", "global_fulltext"],
                            "analyzer": "natural_text"
                        }
                    }
                },
                "summary_": {
                    "type": "object",
                    "properties": {
                        "summary_": {
                            "type": "string", "copy_to": ["abstract", "global_fulltext"],
                            "analyzer": "natural_text"
                        }
                    }
                },
                "date_and_time_of_latest_transaction": {
                    "type": "date",
                    "format": "yyyy||yyyyMM||yyyyMMdd||yyyyMMddHHmmss||yyyyMMddHHmmss.S",
                },
                "publication_date": {
                    "type": "date",
                    "format": "yyyy||yyyyMM||yyyyMMdd||yyyyMMddHHmmss||yyyyMMddHHmmss.S||dd MM yyyy||dd MMM yyyy||MMM yyyy||MMM yyyy?||yyyy ('repr'.1964.)",
                },
                "publication_distribution_imprint": {
                    "type": "object",
                    "properties": {
                        "date_of_publication_distribution": {
                            "type": "date",
                            "format": "yyyy||yyyyMM||yyyyMMdd||yyyyMMddHHmmss||yyyyMMddHHmmss.S||dd MM yyyy||dd MMM yyyy||MMM yyyy||MMM yyyy?||yyyy ('repr'.1964.)",
                            "copy_to": ["publication_date"]
                        },
                        "name_of_publisher_distributor": {
                            "type": "string",
                            "analyzer": "basic_analyzer"

                        },
                        "place_of_publication_distribution": {
                            "type": "string",
                            "analyzer": "basic_analyzer"
                        }
                    }
                }
            }
        }
    }
}


def create_index(sender, **kwargs):
    """Create or recreate the elasticsearch index for records."""
    es.indices.delete(index='records', ignore=404)
    es.indices.create(index='records', body=SEARCH_RECORD_MAPPING)


def delete_index(sender, **kwargs):
    """Create the elasticsearch index for records."""
    es.indices.delete(index='records', ignore=404)


def setup_app(app):
    """Set up the extension for the given app."""
    from invenio_base import signals
    from invenio_base.scripts.database import recreate, drop, create

    global es

    es = Elasticsearch(
        app.config.get('ES_HOSTS', None),
        connection_class=RequestsHttpConnection
    )

    signals.pre_command.connect(delete_index, sender=drop)
    signals.pre_command.connect(create_index, sender=create)
    signals.pre_command.connect(delete_index, sender=recreate)
    signals.pre_command.connect(create_index, sender=recreate)
