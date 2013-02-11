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
Tests For MySQLDB Connection.
"""
from MySQLdb.constants import CLIENT as mysql_client_constants

from nova.db.mysqldb import connection
from nova.openstack.common.db import exception as db_exc
from nova import test


class MysqlDBConnTestCase(test.NoDBTestCase):
    def setUp(self):
        super(MysqlDBConnTestCase, self).setUp()

    def _test_sql_connect_helper(self, conn_str, expected_result):
        self.flags(connection=conn_str, group='database')
        # Make sure we have no cached connection dict
        connection._CONNECTION_DICT = None
        if expected_result is None:
            self.assertRaises(db_exc.DBError, connection.get_connection_args)
            return
        result = connection.get_connection_args()
        self.assertEqual(expected_result, result)

    def test_sql_connection_conf_parsing_success(self):
        def _expected_dict(**kwargs):
            d = {'user': 'user',
                 'passwd': 'passwd',
                 'host': 'localhost.localdomain',
                 'port': 3306,
                 'db': 'thedbyo',
                 'client_flag': mysql_client_constants.FOUND_ROWS,
                 'charset': 'utf8'}
            d.update(kwargs)
            return d

        self._test_sql_connect_helper(
                'mysql://user:passwd@localhost.localdomain/thedbyo?'
                'charset=utf8', _expected_dict())
        self._test_sql_connect_helper(
                'mysql://user:passwd@localhost.localdomain:5432/thedbyo?'
                'charset=utf8', _expected_dict(port=5432))
        self._test_sql_connect_helper(
                'mysql+foo://user:passwd@localhost.localdomain/thedbyo?'
                'charset=utf8', _expected_dict())
        self._test_sql_connect_helper(
                'mysql+foo://user:passwd@localhost.localdomain:5432/thedbyo?'
                'charset=utf8&beer=mandatory', _expected_dict(port=5432))
        self._test_sql_connect_helper(
                'mysql://user@localhost.localdomain:5432/thedbyo?charset=utf8',
                _expected_dict(passwd='', port=5432))
        # Should fail
        self._test_sql_connect_helper(
                'foo://user:passwd@localhost.localdomain:5432/thedbyo?'
                'tacos=yummy&charset=utf8', None)
