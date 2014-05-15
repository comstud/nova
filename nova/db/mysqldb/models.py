# Copyright (c) 2013 Rackspace Hosting
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
MySQLdb models
"""
import sys

from nova.db.mysqldb import sql
from nova.openstack.common.gettextutils import _
from nova.openstack.common import importutils
from nova.openstack.common import log as logging
from nova.openstack.common import timeutils


LOG = logging.getLogger(__name__)
_OUR_MODULE = sys.modules[__name__]
_SCHEMA_INFO = {'version': None}


class Join(object):
    def __init__(self, table_name, join_str, join_kwargs=None,
                 join_type=None, use_list=True, use_dict=False,
                 prereq_join_names=None, hidden=False):
        if join_kwargs is None:
            join_kwargs = {}
        if join_type is None:
            join_type = 'LEFT OUTER JOIN'
        if prereq_join_names is None:
            prereq_join_names = []
        # target will be set automatically by _create_models()
        self.target = None
        self.table_name = table_name
        self.join_type = join_type
        self.join_str = join_str
        self.join_kwargs = join_kwargs
        self.use_list = use_list
        self.use_dict = use_dict
        self.prereq_join_names = prereq_join_names
        self.hidden = hidden


class _BaseModel(dict):
    """Base Model.  This is essentially a dictionary with some extra
    methods.  To access values for columns, access this object as a
    dictionary.
    """

    _default_joins = []
    # These will be set automatically in _create_models() below.
    __joins__ = []
    columns = []

    @classmethod
    def get_model(cls, name):
        return getattr(cls.__all_models__, name)

    @classmethod
    def from_response(cls, col_iter):
        obj = cls()
        for column in cls.columns:
            obj[column] = col_iter.next()
        if not obj['id']:
            return None
        # Swap out the joins
        for join_name in cls.__joins__:
            join = getattr(cls, join_name)
            if join.use_list:
                obj[join_name] = []
            elif join.use_dict:
                obj[join_name] = {}
            else:
                obj[join_name] = None
        return obj

    def to_dict(self):
        """Return dictionary representation of ourselves, including
        anything that we joined.
        """
        # 'copy' only creates a new dictionary, not a new model object.
        d = self.copy()
        # Recurse into joins
        for j in self.__joins__:
            val = d[j]
            if val is None:
                continue
            if isinstance(val, dict):
                d[j] = val.copy()
            elif isinstance(val, list):
                d[j] = [x.to_dict() for x in val]
            else:
                d[j] = val.to_dict()
        return d

    @classmethod
    def soft_delete(cls, conn, where_str, **where_kwargs):
        now = timeutils.utcnow()
        query = sql.UpdateQuery(cls, values=dict(deleted_at=now),
                raw_values=dict(deleted='`id`'))
        query = query.where(where_str, **where_kwargs)
        return query.update(conn)


class Models(object):
    """This will have attributes for every model.  Ie, 'Instance'.
    This gets setattr'd every time we update the schema, so it's an
    atomic swap.  This is here just so pylint, etc is happy.
    """
    pass


def _table_to_base_model_mapping():
    """Create a table name to base model mapping."""
    mapping = {}
    for obj_name in dir(_OUR_MODULE):
        obj = getattr(_OUR_MODULE, obj_name)
        try:
            if issubclass(obj, _BaseModel) and obj_name != '_BaseModel':
                mapping[obj.__table__] = obj
        except TypeError:
            continue
    return mapping


def _create_models(schema):
    tbl_to_base_model = _table_to_base_model_mapping()
    version = schema['version']
    # Create a new Models class.  This will end up with an attribute
    # for every model we create.

    models_obj = type('Models', (object, ), {})
    table_to_model = {}
    for table, table_info in schema['tables'].iteritems():
        # Find the base model for this mapping based on the table name.
        base_model = tbl_to_base_model.get(table)
        if not base_model:
            # Just skip it if we've not defined one yet.
            continue
        model_name = base_model.__model__

        # Create a new class like Instance_v<version>
        vers_model_name = '%s_v%s' % (base_model.__model__, str(version))
        # Find the Mixin class for this model.
        mixin_cls = _mixin_cls(model_name)

        # Do the actual class creation here.  We'll subclass the base
        # model as as from the mixin.  Populate some useful attributes.
        vers_model = type(vers_model_name, (mixin_cls, base_model),
                {'__repo_version__': version,
                 '__all_models__': models_obj,
                 'columns': table_info['columns']})
        # Update '__joins__' on the model and set each Join()'s target
        # to the 'column name'.
        joins = []
        for obj_name in dir(vers_model):
            obj = getattr(vers_model, obj_name)
            try:
                if isinstance(obj, Join):
                    obj.target = obj_name
                    # Skip adding Joins to __joins__ that should remain
                    # hidden
                    if obj.hidden:
                        continue
                    joins.append(obj_name)
            except TypeError:
                continue
        setattr(vers_model, '__joins__', joins)

        # Set this model in our 'Models' object.
        setattr(models_obj, model_name, vers_model)
        table_to_model[table] = vers_model
    # Currently not used
    setattr(models_obj, '__table_model_map__', table_to_model)
    # Update our 'Models' object within this module.
    setattr(_OUR_MODULE, 'Models', models_obj)


class _DefaultMixin(object):
    pass


def _mixin_cls(model_name):
    """Fix the Mixin class for the model.  Each Mixin will be
    in a module named by the model.  Ie, the Instance model Mixin
    will be in mysqldb/instance.py.
    """
    pkg = _OUR_MODULE.__package__
    module_name = ''
    for c in model_name:
        if c.isupper():
            if module_name:
                module_name += '_'
            c = c.lower()
        module_name += c
    mixin_str = ('%(pkg)s.%(name)s.Mixin' % {'pkg': pkg, 'name':
        module_name})
    try:
        return importutils.import_class(mixin_str)
    except ImportError:
        LOG.warn(_("Couldn't load mixin class: %s") % mixin_str)
        return _DefaultMixin


def set_schema(schema):
    """Update our schema and regenerate our models if the version changed."""
    if schema['version'] != _SCHEMA_INFO['version']:
        _SCHEMA_INFO['version'] = schema['version']
        _create_models(schema)
