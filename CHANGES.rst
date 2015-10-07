..
    This file is part of Invenio.
    Copyright (C) 2015 CERN.

    Invenio is free software; you can redistribute it
    and/or modify it under the terms of the GNU General Public License as
    published by the Free Software Foundation; either version 2 of the
    License, or (at your option) any later version.

    Invenio is distributed in the hope that it will be
    useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Invenio; if not, write to the
    Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
    MA 02111-1307, USA.

    In applying this license, CERN does not
    waive the privileges and immunities granted to it by virtue of its status
    as an Intergovernmental Organization or submit itself to any jurisdiction.

Changes
=======

Version 0.3.2 (released 2015-10-07)
-----------------------------------

- Code style fixes and version bump of required packages.

Version 0.3.1 (released 2015-10-05)
-----------------------------------

Improved features
~~~~~~~~~~~~~~~~~

- Uses CFG_VERSION config variable to generate `bower.json`.

Bug fixes
~~~~~~~~~

- Allows legacy UserInfo object creation without Invenio-Access
  package.

Version 0.3.0 (released 2015-10-02)
-----------------------------------

Incompatible changes
~~~~~~~~~~~~~~~~~~~~

- Removes record related tasks in favor of `invenio-records`.

Bug fixes
~~~~~~~~~

- Adds missing dependency to invenio-collections>=0.1.2.
- Removes references to invenio.config and replaces them with
  invenio_base.globals.cfg.
- Adds missing dependency to invenio-testing.
- Replaces if statement by try...except block to check if a model has
  a mixer associated with it.

Version 0.2.1 (released 2015-09-23)
-----------------------------------

Incompatible changes
~~~~~~~~~~~~~~~~~~~~

- Removes previously disabled legacy handlers.

Bug fixes
~~~~~~~~~

- Adds missing MySQL-python>=1.2.5 dependency.

Version 0.2.0 (released 2015-09-22)
-----------------------------------

Incompatible changes
~~~~~~~~~~~~~~~~~~~~

- Removes `get_record` from global Jinja context.
- Removes possibility to import config as invenio package attribute.
  Replace `from invenio import config` by using `current_app.config`.
- Removes endpoints serving legacy webinterfaces and legacy admin
  pages.
- Removes bibdocfile dependency.

Bug fixes
~~~~~~~~~

- Adds missing invenio-base, raven and redis dependencies.
- Adds missing dependencies to SQLAlchemy-Utils and intbitset.
- Adds missing invenio-celery>=0.1.0 dependency.
- Removes dependency on legacy WebUser module.

Version 0.1.0 (released 2015-09-17)
-----------------------------------

- Initial public release.
