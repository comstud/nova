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
Tests For MySQLDB API.
"""
import inspect

from nova.db.mysqldb import api as mysqldb_api
from nova.db.sqlalchemy import api as sqla_api
from nova import test
from nova import utils


class MysqlDBAPITestCase(test.TestCase):
    def setUp(self):
        super(MysqlDBAPITestCase, self).setUp()

        def _fake_monitor(*args, **kwargs):
            pass

        self.stubs.Set(mysqldb_api.API, '_launch_monitor', _fake_monitor)
        self.api = mysqldb_api.API()

    @staticmethod
    def _compare_methods(mysqldb_fn, sqla_fn):
        # Drill down to the real methods
        mysqldb_fn = utils.get_wrapped_function(mysqldb_fn)
        sqla_fn = utils.get_wrapped_function(sqla_fn)
        mysqldb_args = list(inspect.getargspec(mysqldb_fn))
        sqla_args = list(inspect.getargspec(sqla_fn))
        # Fix up mysqldb because it has a 'self' for the class instance and
        # also uses 'ctxt' instead of 'context'
        mysqldb_args[0] = mysqldb_args[0][1:]
        if mysqldb_args[0][0] == 'ctxt':
            mysqldb_args[0][0] = 'context'
        return mysqldb_args == sqla_args

    def test_method_signatures_against_sqlalchemy(self):
        for fn_name in dir(self.api):
            if fn_name.startswith('_'):
                continue
            mysqldb_fn = getattr(self.api, fn_name)
            if not inspect.ismethod(mysqldb_fn):
                continue
            sqla_fn = getattr(sqla_api, fn_name, None)
            if sqla_fn is None:
                self.fail("%s is gone from sqlalchemy" % fn_name)
            if not self._compare_methods(mysqldb_fn, sqla_fn):
                self.fail("%s method signature doesn't match sqlalchemy" %
                          fn_name)
