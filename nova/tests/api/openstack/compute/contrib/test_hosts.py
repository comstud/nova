# Copyright (c) 2011 OpenStack, LLC.
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

from lxml import etree
import webob.exc

from nova.api.openstack.compute.contrib import hosts as os_hosts
from nova.cells import rpcapi as cells_rpcapi
from nova.compute import power_state
from nova.compute import vm_states
from nova import context
from nova import db
from nova.openstack.common import log as logging
from nova import test
from nova.tests import matchers

LOG = logging.getLogger(__name__)
HOST_LIST = {"hosts": [
        {"host_name": "host_c1", "service": "compute", "zone": "nova"},
        {"host_name": "host_c2", "service": "compute", "zone": "nonova"},
        {"host_name": "host_v1", "service": "volume", "zone": "nova"},
        {"host_name": "host_v2", "service": "volume", "zone": "nonova"}]}
HOST_LIST_NOVA_ZONE = [
        {"host_name": "host_c1", "service": "compute", "zone": "nova"},
        {"host_name": "host_v1", "service": "volume", "zone": "nova"}]
SERVICES_LIST = [
        {"host": "host_c1", "topic": "compute", "availability_zone": "nova"},
        {"host": "host_c2", "topic": "compute", "availability_zone": "nonova"},
        {"host": "host_v1", "topic": "volume", "availability_zone": "nova"},
        {"host": "host_v2", "topic": "volume", "availability_zone": "nonova"}]


def stub_service_get_all(self, req):
    return SERVICES_LIST


def stub_set_host_enabled(context, host, enabled):
    if host == "notimplemented":
        raise NotImplementedError()
    # We'll simulate success and failure by assuming
    # that 'host_c1' always succeeds, and 'host_c2'
    # always fails
    fail = (host == "host_c2")
    status = "enabled" if (enabled != fail) else "disabled"
    return status


def stub_set_host_maintenance(context, host, mode):
    if host == "notimplemented":
        raise NotImplementedError()
    # We'll simulate success and failure by assuming
    # that 'host_c1' always succeeds, and 'host_c2'
    # always fails
    fail = (host == "host_c2")
    maintenance = "on_maintenance" if (mode != fail) else "off_maintenance"
    return maintenance


def stub_host_power_action(context, host, action):
    if host == "notimplemented":
        raise NotImplementedError()
    return action


def _create_instance(**kwargs):
    """Create a test instance"""
    ctxt = context.get_admin_context()
    return db.instance_create(ctxt, _create_instance_dict(**kwargs))


def _create_instance_dict(**kwargs):
    """Create a dictionary for a test instance"""
    inst = {}
    inst['image_ref'] = 'cedef40a-ed67-4d10-800e-17455edce175'
    inst['reservation_id'] = 'r-fakeres'
    inst['user_id'] = kwargs.get('user_id', 'admin')
    inst['project_id'] = kwargs.get('project_id', 'fake')
    inst['instance_type_id'] = '1'
    if 'host' in kwargs:
        inst['host'] = kwargs.get('host')
    inst['vcpus'] = kwargs.get('vcpus', 1)
    inst['memory_mb'] = kwargs.get('memory_mb', 20)
    inst['root_gb'] = kwargs.get('root_gb', 30)
    inst['ephemeral_gb'] = kwargs.get('ephemeral_gb', 30)
    inst['vm_state'] = kwargs.get('vm_state', vm_states.ACTIVE)
    inst['power_state'] = kwargs.get('power_state', power_state.RUNNING)
    inst['task_state'] = kwargs.get('task_state', None)
    inst['availability_zone'] = kwargs.get('availability_zone', None)
    inst['ami_launch_index'] = 0
    inst['launched_on'] = kwargs.get('launched_on', 'dummy')
    return inst


class FakeRequest(object):
    environ = {"nova.context": context.get_admin_context()}
    GET = {}


class FakeRequestWithNovaZone(object):
    environ = {"nova.context": context.get_admin_context()}
    GET = {"zone": "nova"}


class HostTestCase(test.TestCase):
    """Test Case for hosts."""

    def setUp(self):
        super(HostTestCase, self).setUp()
        self.controller = os_hosts.HostController()
        self.req = FakeRequest()
        self.stubs.Set(db, 'service_get_all',
                       stub_service_get_all)
        self.stubs.Set(self.controller.api, 'set_host_enabled',
                       stub_set_host_enabled)
        self.stubs.Set(self.controller.api, 'set_host_maintenance',
                       stub_set_host_maintenance)
        self.stubs.Set(self.controller.api, 'host_power_action',
                       stub_host_power_action)

    def _test_host_update(self, host, key, val, expected_value):
        body = {key: val}
        result = self.controller.update(self.req, host, body)
        self.assertEqual(result[key], expected_value)

    def test_list_hosts(self):
        """Verify that the compute hosts are returned."""
        hosts = os_hosts._list_hosts(self.req)
        self.assertEqual(hosts, HOST_LIST['hosts'])

    def test_list_hosts_with_zone(self):
        req = FakeRequestWithNovaZone()
        hosts = os_hosts._list_hosts(req)
        self.assertEqual(hosts, HOST_LIST_NOVA_ZONE)

    def test_disable_host(self):
        self._test_host_update('host_c1', 'status', 'disable', 'disabled')
        self._test_host_update('host_c2', 'status', 'disable', 'enabled')

    def test_enable_host(self):
        self._test_host_update('host_c1', 'status', 'enable', 'enabled')
        self._test_host_update('host_c2', 'status', 'enable', 'disabled')

    def test_enable_maintenance(self):
        self._test_host_update('host_c1', 'maintenance_mode',
                               'enable', 'on_maintenance')

    def test_disable_maintenance(self):
        self._test_host_update('host_c1', 'maintenance_mode',
                               'disable', 'off_maintenance')

    def _test_host_update_notimpl(self, key, val):
        def stub_service_get_all_notimpl(self, req):
            return [{'host': 'notimplemented', 'topic': None,
                     'availability_zone': None}]
        self.stubs.Set(db, 'service_get_all',
                       stub_service_get_all_notimpl)
        body = {key: val}
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          self.controller.update,
                          self.req, 'notimplemented', body=body)

    def test_disable_host_notimpl(self):
        self._test_host_update_notimpl('status', 'disable')

    def test_enable_maintenance_notimpl(self):
        self._test_host_update_notimpl('maintenance_mode', 'enable')

    def test_host_startup(self):
        result = self.controller.startup(self.req, "host_c1")
        self.assertEqual(result["power_action"], "startup")

    def test_host_shutdown(self):
        result = self.controller.shutdown(self.req, "host_c1")
        self.assertEqual(result["power_action"], "shutdown")

    def test_host_reboot(self):
        result = self.controller.reboot(self.req, "host_c1")
        self.assertEqual(result["power_action"], "reboot")

    def _test_host_power_action_notimpl(self, method):
        self.assertRaises(webob.exc.HTTPNotImplemented,
                          method, self.req, "notimplemented")

    def test_host_startup_notimpl(self):
        self._test_host_power_action_notimpl(self.controller.startup)

    def test_host_shutdown_notimpl(self):
        self._test_host_power_action_notimpl(self.controller.shutdown)

    def test_host_reboot_notimpl(self):
        self._test_host_power_action_notimpl(self.controller.reboot)

    def test_bad_status_value(self):
        bad_body = {"status": "bad"}
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.update,
                self.req, "host_c1", bad_body)
        bad_body2 = {"status": "disablabc"}
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.update,
                self.req, "host_c1", bad_body2)

    def test_bad_update_key(self):
        bad_body = {"crazy": "bad"}
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.update,
                self.req, "host_c1", bad_body)

    def test_bad_update_key_and_correct_udpate_key(self):
        bad_body = {"status": "disable", "crazy": "bad"}
        self.assertRaises(webob.exc.HTTPBadRequest, self.controller.update,
                self.req, "host_c1", bad_body)

    def test_good_udpate_keys(self):
        body = {"status": "disable", "maintenance_mode": "enable"}
        result = self.controller.update(self.req, 'host_c1', body)
        self.assertEqual(result["host"], "host_c1")
        self.assertEqual(result["status"], "disabled")
        self.assertEqual(result["maintenance_mode"], "on_maintenance")

    def test_bad_host(self):
        self.assertRaises(webob.exc.HTTPNotFound, self.controller.update,
                self.req, "bogus_host_name", {"status": "disable"})

    def test_show_forbidden(self):
        self.req.environ["nova.context"].is_admin = False
        dest = 'dummydest'
        self.assertRaises(webob.exc.HTTPForbidden,
                          self.controller.show,
                          self.req, dest)
        self.req.environ["nova.context"].is_admin = True

    def test_show_host_not_exist(self):
        """A host given as an argument does not exists."""
        self.req.environ["nova.context"].is_admin = True
        dest = 'dummydest'
        self.assertRaises(webob.exc.HTTPNotFound,
                          self.controller.show,
                          self.req, dest)

    def _create_compute_service(self):
        """Create compute-manager(ComputeNode and Service record)."""
        ctxt = context.get_admin_context()
        dic = {'host': 'dummy', 'binary': 'nova-compute', 'topic': 'compute',
               'report_count': 0, 'availability_zone': 'dummyzone'}
        s_ref = db.service_create(ctxt, dic)

        dic = {'service_id': s_ref['id'],
               'vcpus': 16, 'memory_mb': 32, 'local_gb': 100,
               'vcpus_used': 16, 'memory_mb_used': 32, 'local_gb_used': 10,
               'hypervisor_type': 'qemu', 'hypervisor_version': 12003,
               'cpu_info': '', 'stats': {}}
        db.compute_node_create(ctxt, dic)

        return db.service_get(ctxt, s_ref['id'])

    def test_show_no_project(self):
        """No instance are running on the given host."""
        ctxt = context.get_admin_context()
        s_ref = self._create_compute_service()

        result = self.controller.show(self.req, s_ref['host'])

        proj = ['(total)', '(used_now)', '(used_max)']
        column = ['host', 'project', 'cpu', 'memory_mb', 'disk_gb']
        self.assertEqual(len(result['host']), 3)
        for resource in result['host']:
            self.assertTrue(resource['resource']['project'] in proj)
            self.assertEqual(len(resource['resource']), 5)
            self.assertTrue(set(resource['resource'].keys()) == set(column))
        db.service_destroy(ctxt, s_ref['id'])

    def test_show_works_correctly(self):
        """show() works correctly as expected."""
        ctxt = context.get_admin_context()
        s_ref = self._create_compute_service()
        i_ref1 = _create_instance(project_id='p-01', host=s_ref['host'])
        i_ref2 = _create_instance(project_id='p-02', vcpus=3,
                                       host=s_ref['host'])

        result = self.controller.show(self.req, s_ref['host'])

        proj = ['(total)', '(used_now)', '(used_max)', 'p-01', 'p-02']
        column = ['host', 'project', 'cpu', 'memory_mb', 'disk_gb']
        self.assertEqual(len(result['host']), 5)
        for resource in result['host']:
            self.assertTrue(resource['resource']['project'] in proj)
            self.assertEqual(len(resource['resource']), 5)
            self.assertTrue(set(resource['resource'].keys()) == set(column))
        db.service_destroy(ctxt, s_ref['id'])
        db.instance_destroy(ctxt, i_ref1['uuid'])
        db.instance_destroy(ctxt, i_ref2['uuid'])


class HostSerializerTest(test.TestCase):
    def setUp(self):
        super(HostSerializerTest, self).setUp()
        self.deserializer = os_hosts.HostUpdateDeserializer()

    def test_index_serializer(self):
        serializer = os_hosts.HostIndexTemplate()
        text = serializer.serialize(HOST_LIST)

        tree = etree.fromstring(text)

        self.assertEqual('hosts', tree.tag)
        self.assertEqual(len(HOST_LIST['hosts']), len(tree))
        for i in range(len(HOST_LIST)):
            self.assertEqual('host', tree[i].tag)
            self.assertEqual(HOST_LIST['hosts'][i]['host_name'],
                             tree[i].get('host_name'))
            self.assertEqual(HOST_LIST['hosts'][i]['service'],
                             tree[i].get('service'))

    def test_update_serializer_with_status(self):
        exemplar = dict(host='host_c1', status='enabled')
        serializer = os_hosts.HostUpdateTemplate()
        text = serializer.serialize(exemplar)

        tree = etree.fromstring(text)

        self.assertEqual('host', tree.tag)
        for key, value in exemplar.items():
            self.assertEqual(value, tree.get(key))

    def test_update_serializer_with_maintainance_mode(self):
        exemplar = dict(host='host_c1', maintenance_mode='enabled')
        serializer = os_hosts.HostUpdateTemplate()
        text = serializer.serialize(exemplar)

        tree = etree.fromstring(text)

        self.assertEqual('host', tree.tag)
        for key, value in exemplar.items():
            self.assertEqual(value, tree.get(key))

    def test_update_serializer_with_maintainance_mode_and_status(self):
        exemplar = dict(host='host_c1',
                        maintenance_mode='enabled',
                        status='enabled')
        serializer = os_hosts.HostUpdateTemplate()
        text = serializer.serialize(exemplar)

        tree = etree.fromstring(text)

        self.assertEqual('host', tree.tag)
        for key, value in exemplar.items():
            self.assertEqual(value, tree.get(key))

    def test_action_serializer(self):
        exemplar = dict(host='host_c1', power_action='reboot')
        serializer = os_hosts.HostActionTemplate()
        text = serializer.serialize(exemplar)

        tree = etree.fromstring(text)

        self.assertEqual('host', tree.tag)
        for key, value in exemplar.items():
            self.assertEqual(value, tree.get(key))

    def test_update_deserializer(self):
        exemplar = dict(status='enabled', maintenance_mode='disable')
        intext = """<?xml version='1.0' encoding='UTF-8'?>
    <updates>
        <status>enabled</status>
        <maintenance_mode>disable</maintenance_mode>
    </updates>"""
        result = self.deserializer.deserialize(intext)

        self.assertEqual(dict(body=exemplar), result)


class HostTestCaseCells(test.TestCase):
    """
    Tests that _list_hosts calls the child cells when CONF.cells.enable is on
    """

    fakeHosts = [
        {'host': 'cells1.host.com',
         'topic': 'cells'},
        {'host': 'cells2.host.com',
         'topic': 'cells'},
        {'host': 'cells3.host.com',
         'topic': 'cells'},
        {'host': 'compute.host.com',
         'topic': 'compute'},
        {'host': 'console.host.com',
         'topic': 'consoleauth'},
        {'host': 'network1.host.com',
         'topic': 'network'},
        {'host': 'network1.host.com',
         'topic': 'network'},
        {'host': 'network1.host.com',
         'topic': 'network'},
        {'host': 'scheduler1.host.com',
         'topic': 'scheduler'},
        {'host': 'scheduler2.host.com',
         'topic': 'scheduler'},
        {'host': 'scheduler3.host.com',
         'topic': 'volume'},
        {'host': 'compute1.host.com',
         'topic': 'compute'},
        {'host': 'compute2.host.com',
         'topic': 'compute'},
        {'host': 'compute3.host.com',
         'topic': 'compute'},
    ]

    def setUp(self):
        super(HostTestCaseCells, self).setUp()
        self.flags(enable=True, group='cells')
        self.fake_req = FakeRequest()
        self.controller = os_hosts.HostController()

    def _mock_broadcast_call(self, ret_val, info=None):
        def _fake_cell_broadcast_call(_self, *args, **kwargs):
            if info is not None:
                info['args'] = args
                info['kwargs'] = kwargs
            return ret_val

        self.stubs.Set(cells_rpcapi.CellsAPI, 'cell_broadcast_call',
                _fake_cell_broadcast_call)

    def test_empty_list(self):
        self._mock_broadcast_call([([], "c0001")])
        response = self.controller.index(self.fake_req)
        self.assertEqual({"hosts": []}, response)

    def test_empty_list_from_two_cells(self):
        self._mock_broadcast_call([([], "c0001"), ([], "c0002")])
        response = self.controller.index(self.fake_req)
        self.assertEqual({"hosts": []}, response)

    def test_cell_name_prepending(self):
        self._mock_broadcast_call([(self.fakeHosts, "c0001")])
        response = self.controller.index(self.fake_req)
        responseHosts = response.get('hosts', [])
        for responseHost, fakeHost in zip(responseHosts, self.fakeHosts):
            self.assertEqual(responseHost['host_name'],
                             'c0001-%s' % fakeHost['host'])
            self.assertEqual(responseHost['service'], fakeHost['topic'])

    def test_cell_name_prepending_with_two_cells(self):
        self._mock_broadcast_call([(self.fakeHosts, "c0001"),
                                   (self.fakeHosts, "c0002")])
        response = self.controller.index(self.fake_req)
        responseHosts = response.get('hosts', [])
        ln = len(self.fakeHosts)
        self.assertEqual(len(responseHosts), ln * 2)
        for responseHost, fakeHost in zip(responseHosts, self.fakeHosts):
            self.assertEqual(responseHost['host_name'],
                             'c0001-%s' % fakeHost['host'])
            self.assertEqual(responseHost['service'], fakeHost['topic'])
        for responseHost, fakeHost in zip(responseHosts[ln:], self.fakeHosts):
            self.assertEqual(responseHost['host_name'],
                             'c0002-%s' % fakeHost['host'])
            self.assertEqual(responseHost['service'], fakeHost['topic'])

    def test_param_names_passed_correctly(self):
        """Test correct args passed to cell_broadcast_call."""
        info = {}
        self._mock_broadcast_call([], info=info)
        os_hosts._cells_list_hosts(self.fake_req, 'compute')

        fake_context = self.fake_req.environ['nova.context']
        expected = {'args': (fake_context, 'down', 'list_services'),
                    'kwargs': {'include_disabled': False}}
        self.assertThat(info, matchers.DictMatches(expected))

    def test_ignoring_non_compute_ervices(self):
        half = len(self.fakeHosts)
        half1 = self.fakeHosts[:half]
        half2 = self.fakeHosts[half + 1:]
        half1Filtered = [
            host for host in half1 if host['topic'] == 'compute']
        half2Filtered = [
            host for host in half2 if host['topic'] == 'compute']
        self._mock_broadcast_call([(half1, "c0001"),
                                   (half2, "c0002")])
        output = os_hosts._cells_list_hosts(self.fake_req, 'compute')
        self.assertEqual(len(half1Filtered) + len(half2Filtered), len(output))
        for in_row, out_row in zip(half1Filtered, output):
            self.assertEqual(out_row['host_name'],
                             'c0001-%s' % in_row['host'])
            self.assertEqual(in_row['topic'], out_row['service'])

        for in_row, out_row in zip(half2Filtered, output[len(half1) + 1:]):
            self.assertEqual(out_row['host_name'],
                             'c0002-%s' % in_row['host'])
            self.assertEqual(in_row['topic'], out_row['service'])
