# Copyright (c) 2012 OpenStack, LLC.
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

from nova import compute
from nova.compute import rpcapi as compute_rpcapi
from nova import context
from nova.openstack.common import rpc
from nova import test


class ComputeHostAPITestCase(test.TestCase):
    def setUp(self):
        super(ComputeHostAPITestCase, self).setUp()
        self.host_api = compute.HostAPI()
        self.ctxt = context.get_admin_context()

    def _mock_rpc_call(self, expected_message, result=None):
        if result is None:
            result = 'fake-result'
        self.mox.StubOutWithMock(rpc, 'call')
        rpc.call(self.ctxt, 'compute.fake_host',
                 expected_message, None).AndReturn(result)

    def _mock_assert_host_exists(self):
        """Sets it so that the host API always thinks that 'fake_host'
        exists.
        """
        self.mox.StubOutWithMock(self.host_api, '_assert_host_exists')
        self.host_api._assert_host_exists(self.ctxt, 'fake_host')

    def test_set_host_enabled(self):
        self._mock_assert_host_exists()
        self._mock_rpc_call(
                {'method': 'set_host_enabled',
                 'args': {'enabled': 'fake_enabled'},
                 'version': compute_rpcapi.ComputeAPI.BASE_RPC_API_VERSION})

        self.mox.ReplayAll()
        result = self.host_api.set_host_enabled(self.ctxt, 'fake_host',
                                                'fake_enabled')
        self.assertEqual('fake-result', result)

    def test_get_host_uptime(self):
        self._mock_assert_host_exists()
        self._mock_rpc_call(
                {'method': 'get_host_uptime',
                 'args': {},
                 'version': compute_rpcapi.ComputeAPI.BASE_RPC_API_VERSION})
        self.mox.ReplayAll()
        result = self.host_api.get_host_uptime(self.ctxt, 'fake_host')
        self.assertEqual('fake-result', result)

    def test_host_power_action(self):
        self._mock_assert_host_exists()
        self._mock_rpc_call(
                {'method': 'host_power_action',
                 'args': {'action': 'fake_action'},
                 'version': compute_rpcapi.ComputeAPI.BASE_RPC_API_VERSION})
        self.mox.ReplayAll()
        result = self.host_api.host_power_action(self.ctxt, 'fake_host',
                                                 'fake_action')
        self.assertEqual('fake-result', result)

    def test_set_host_maintenance(self):
        self._mock_assert_host_exists()
        self._mock_rpc_call(
                {'method': 'host_maintenance_mode',
                 'args': {'host': 'fake_host', 'mode': 'fake_mode'},
                 'version': compute_rpcapi.ComputeAPI.BASE_RPC_API_VERSION})
        self.mox.ReplayAll()
        result = self.host_api.set_host_maintenance(self.ctxt, 'fake_host',
                                                    'fake_mode')
        self.assertEqual('fake-result', result)

    def test_service_get_all(self):
        services = [dict(id=1, key1='val1', key2='val2', topic='compute',
                         host='host1'),
                    dict(id=2, key1='val2', key3='val3', topic='compute',
                         host='host2')]
        exp_services = []
        for service in services:
            exp_service = {}
            exp_service.update(availability_zone='nova', **service)
            exp_services.append(exp_service)

        self.mox.StubOutWithMock(self.host_api.db,
                                 'service_get_all')

        # Test no filters
        self.host_api.db.service_get_all(self.ctxt, False).AndReturn(
                services)
        self.mox.ReplayAll()
        result = self.host_api.service_get_all(self.ctxt)
        self.mox.VerifyAll()
        self.assertEqual(exp_services, result)

        # Test no filters #2
        self.mox.ResetAll()
        self.host_api.db.service_get_all(self.ctxt, False).AndReturn(
                services)
        self.mox.ReplayAll()
        result = self.host_api.service_get_all(self.ctxt, filters={})
        self.mox.VerifyAll()
        self.assertEqual(exp_services, result)

        # Test w/ filter
        self.mox.ResetAll()
        self.host_api.db.service_get_all(self.ctxt, False).AndReturn(
                services)
        self.mox.ReplayAll()
        result = self.host_api.service_get_all(self.ctxt,
                                               filters=dict(key1='val2'))
        self.mox.VerifyAll()
        self.assertEqual([exp_services[1]], result)


class ComputeHostAPICellsTestCase(ComputeHostAPITestCase):
    def setUp(self):
        self.flags(compute_api_class='nova.compute.cells_api.ComputeCellsAPI')
        super(ComputeHostAPICellsTestCase, self).setUp()

    def _mock_rpc_call(self, expected_message, result=None):
        if result is None:
            result = 'fake-result'
        # Wrapped with cells call
        expected_message = {'method': 'proxy_rpc_to_manager',
                            'args': {'topic': 'compute.fake_host',
                                     'rpc_message': expected_message,
                                     'call': True,
                                     'timeout': None},
                            'version': '1.2'}
        self.mox.StubOutWithMock(rpc, 'call')
        rpc.call(self.ctxt, 'cells', expected_message,
                 None).AndReturn(result)

    def test_service_get_all(self):
        fake_filters = dict(key1='val2')
        self.mox.StubOutWithMock(self.host_api.cells_rpcapi,
                                 'service_get_all')
        self.host_api.cells_rpcapi.service_get_all(self.ctxt,
                include_disabled=False,
                filters=fake_filters).AndReturn('fake-result')
        self.mox.ReplayAll()
        result = self.host_api.service_get_all(self.ctxt,
                                               filters=dict(key1='val2'))
        self.assertEqual('fake-result', result)
