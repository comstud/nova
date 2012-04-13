# Copyright (c) 2012 Rackspace Hosting # All Rights Reserved.
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
Tests For Cells Messaging module
"""

from nova.cells import messaging
from nova import context
from nova import exception
from nova.openstack.common import cfg
from nova import test
from nova.tests.cells import fakes
from nova import utils as nova_utils


CONF = cfg.CONF
CONF.import_opt('host', 'nova.config')
CONF.import_opt('name', 'nova.cells.opts', group='cells')
CONF.import_opt('allowed_rpc_exception_modules',
                'nova.openstack.common.rpc')


class CellsMessageClassesTestCase(test.TestCase):
    """Test case for the main Cells Message classes."""
    def setUp(self):
        super(CellsMessageClassesTestCase, self).setUp()
        fakes.init(self)
        self.ctxt = 'fake_context'
        # Need to be able to deserialize test.TestingException.
        allowed_modules = CONF.allowed_rpc_exception_modules
        allowed_modules.append('nova.test')
        self.flags(allowed_rpc_exception_modules=allowed_modules)

    def test_reverse_path(self):
        path = 'a!b!c!d'
        expected = 'd!c!b!a'
        rev_path = messaging._reverse_path(path)
        self.assertEqual(rev_path, expected)

    def test_response_cell_name_from_path(self):
        # test array with tuples of inputs/expected outputs
        test_paths = [('cell1', 'cell1'),
                      ('cell1!cell2', 'cell2!cell1'),
                      ('cell1!cell2!cell3', 'cell3!cell2!cell1')]

        for test_input, expected_output in test_paths:
            self.assertEqual(expected_output,
                    messaging._response_cell_name_from_path(test_input))

    def test_response_cell_name_from_path_neighbor_only(self):
        # test array with tuples of inputs/expected outputs
        test_paths = [('cell1', 'cell1'),
                      ('cell1!cell2', 'cell2!cell1'),
                      ('cell1!cell2!cell3', 'cell3!cell2')]

        for test_input, expected_output in test_paths:
            self.assertEqual(expected_output,
                    messaging._response_cell_name_from_path(test_input,
                            neighbor_only=True))

    def test_targetted_message(self):
        self.flags(max_hop_count=99, group='cells')
        our_name = 'api-cell'
        target_cell = 'api-cell!child-cell2!grandchild-cell1'
        msg_handler = fakes.get_message_handler(our_name)
        method = 'fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        tgt_message = msg_handler.create_targetted_message(self.ctxt,
                method, method_kwargs, direction, target_cell)
        self.assertEqual(self.ctxt, tgt_message.ctxt)
        self.assertEqual(method, tgt_message.method_name)
        self.assertEqual(method_kwargs, tgt_message.method_kwargs)
        self.assertEqual(direction, tgt_message.direction)
        self.assertEqual(target_cell, target_cell)
        self.assertFalse(tgt_message.fanout)
        self.assertFalse(tgt_message.need_response)
        self.assertEqual(our_name, tgt_message.routing_path)
        self.assertEqual(1, tgt_message.hop_count)
        self.assertEqual(99, tgt_message.max_hop_count)
        self.assertFalse(tgt_message.is_broadcast)
        # Correct next hop?
        next_hop = tgt_message._get_next_hop()
        child_cell = msg_handler.state_manager.get_child_cell('child-cell2')
        self.assertEqual(child_cell, next_hop)

    def test_create_targetted_message_with_response(self):
        self.flags(max_hop_count=99, group='cells')
        our_name = 'child-cell1'
        target_cell = 'child-cell1!api-cell'
        msg_handler = fakes.get_message_handler(our_name)
        method = 'fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'up'
        tgt_message = msg_handler.create_targetted_message(self.ctxt,
                method, method_kwargs, direction, target_cell,
                need_response=True)
        self.assertEqual(self.ctxt, tgt_message.ctxt)
        self.assertEqual(method, tgt_message.method_name)
        self.assertEqual(method_kwargs, tgt_message.method_kwargs)
        self.assertEqual(direction, tgt_message.direction)
        self.assertEqual(target_cell, target_cell)
        self.assertFalse(tgt_message.fanout)
        self.assertTrue(tgt_message.need_response)
        self.assertEqual(our_name, tgt_message.routing_path)
        self.assertEqual(1, tgt_message.hop_count)
        self.assertEqual(99, tgt_message.max_hop_count)
        self.assertFalse(tgt_message.is_broadcast)
        # Correct next hop?
        next_hop = tgt_message._get_next_hop()
        parent_cell = msg_handler.state_manager.get_parent_cell('api-cell')
        self.assertEqual(parent_cell, next_hop)

    def test_create_broadcast_message(self):
        self.flags(max_hop_count=99, group='cells')
        our_name = 'api-cell'
        self.flags(name='api-cell', max_hop_count=99, group='cells')
        msg_handler = fakes.get_message_handler(our_name)
        method = 'fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        bcast_message = msg_handler.create_broadcast_message(self.ctxt,
                method, method_kwargs, direction)
        self.assertEqual(self.ctxt, bcast_message.ctxt)
        self.assertEqual(method, bcast_message.method_name)
        self.assertEqual(method_kwargs, bcast_message.method_kwargs)
        self.assertEqual(direction, bcast_message.direction)
        self.assertFalse(bcast_message.fanout)
        self.assertFalse(bcast_message.need_response)
        self.assertEqual(our_name, bcast_message.routing_path)
        self.assertEqual(1, bcast_message.hop_count)
        self.assertEqual(99, bcast_message.max_hop_count)
        self.assertTrue(bcast_message.is_broadcast)
        # Correct next hops?
        next_hops = bcast_message._get_next_hops()
        child_cells = msg_handler.state_manager.get_child_cells()
        self.assertEqual(child_cells, next_hops)

    def test_create_broadcast_message_with_response(self):
        self.flags(max_hop_count=99, group='cells')
        our_name = 'child-cell1'
        msg_handler = fakes.get_message_handler(our_name)
        method = 'fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'up'
        bcast_message = msg_handler.create_broadcast_message(self.ctxt,
                method, method_kwargs, direction, need_response=True)
        self.assertEqual(self.ctxt, bcast_message.ctxt)
        self.assertEqual(method, bcast_message.method_name)
        self.assertEqual(method_kwargs, bcast_message.method_kwargs)
        self.assertEqual(direction, bcast_message.direction)
        self.assertFalse(bcast_message.fanout)
        self.assertTrue(bcast_message.need_response)
        self.assertEqual(our_name, bcast_message.routing_path)
        self.assertEqual(1, bcast_message.hop_count)
        self.assertEqual(99, bcast_message.max_hop_count)
        self.assertTrue(bcast_message.is_broadcast)
        # Correct next hops?
        next_hops = bcast_message._get_next_hops()
        parent_cells = msg_handler.state_manager.get_parent_cells()
        self.assertEqual(parent_cells, next_hops)

    def test_self_targetted_message(self):
        target_cell = 'api-cell'
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        call_info = {}

        def our_fake_method(message, **kwargs):
            call_info['context'] = message.ctxt
            call_info['routing_path'] = message.routing_path
            call_info['kwargs'] = kwargs

        fakes.stub_tgt_method(self, 'api-cell', 'our_fake_method',
                our_fake_method)

        tgt_message = msg_handler.create_targetted_message(self.ctxt,
                method, method_kwargs, direction, target_cell)
        tgt_message.process()

        self.assertEqual(self.ctxt, call_info['context'])
        self.assertEqual(method_kwargs, call_info['kwargs'])
        self.assertEqual(target_cell, call_info['routing_path'])

    def test_child_targetted_message(self):
        target_cell = 'api-cell!child-cell1'
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        call_info = {}

        def our_fake_method(message, **kwargs):
            call_info['context'] = message.ctxt
            call_info['routing_path'] = message.routing_path
            call_info['kwargs'] = kwargs

        fakes.stub_tgt_method(self, 'child-cell1', 'our_fake_method',
                our_fake_method)

        tgt_message = msg_handler.create_targetted_message(self.ctxt,
                method, method_kwargs, direction, target_cell)
        tgt_message.process()

        self.assertEqual(self.ctxt, call_info['context'])
        self.assertEqual(method_kwargs, call_info['kwargs'])
        self.assertEqual(target_cell, call_info['routing_path'])

    def test_grandchild_targetted_message(self):
        target_cell = 'api-cell!child-cell2!grandchild-cell1'
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        call_info = {}

        def our_fake_method(message, **kwargs):
            call_info['context'] = message.ctxt
            call_info['routing_path'] = message.routing_path
            call_info['kwargs'] = kwargs

        fakes.stub_tgt_method(self, 'grandchild-cell1', 'our_fake_method',
                our_fake_method)

        tgt_message = msg_handler.create_targetted_message(self.ctxt,
                method, method_kwargs, direction, target_cell)
        tgt_message.process()

        self.assertEqual(self.ctxt, call_info['context'])
        self.assertEqual(method_kwargs, call_info['kwargs'])
        self.assertEqual(target_cell, call_info['routing_path'])

    def test_grandchild_targetted_message_with_response(self):
        target_cell = 'api-cell!child-cell2!grandchild-cell1'
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        call_info = {}

        def our_fake_method(message, **kwargs):
            call_info['context'] = message.ctxt
            call_info['routing_path'] = message.routing_path
            call_info['kwargs'] = kwargs
            return 'our_fake_response'

        fakes.stub_tgt_method(self, 'grandchild-cell1', 'our_fake_method',
                our_fake_method)

        tgt_message = msg_handler.create_targetted_message(self.ctxt,
                method, method_kwargs, direction, target_cell,
                need_response=True)
        response = tgt_message.process()

        self.assertEqual(self.ctxt, call_info['context'])
        self.assertEqual(method_kwargs, call_info['kwargs'])
        self.assertEqual(target_cell, call_info['routing_path'])
        self.assertFalse(response.failure)
        self.assertTrue(response.value_or_raise(), 'our_fake_response')

    def test_grandchild_targetted_message_with_error(self):
        target_cell = 'api-cell!child-cell2!grandchild-cell1'
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        def our_fake_method(message, **kwargs):
            raise test.TestingException('this should be returned')

        fakes.stub_tgt_method(self, 'grandchild-cell1', 'our_fake_method',
                our_fake_method)

        tgt_message = msg_handler.create_targetted_message(self.ctxt,
                method, method_kwargs, direction, target_cell,
                need_response=True)
        response = tgt_message.process()
        self.assertTrue(response.failure)
        self.assertRaises(test.TestingException, response.value_or_raise)

    def test_grandchild_targetted_message_max_hops(self):
        self.flags(max_hop_count=2, group='cells')
        target_cell = 'api-cell!child-cell2!grandchild-cell1'
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        def our_fake_method(message, **kwargs):
            raise test.TestingException('should not be reached')

        fakes.stub_tgt_method(self, 'grandchild-cell1', 'our_fake_method',
                our_fake_method)

        tgt_message = msg_handler.create_targetted_message(self.ctxt,
                method, method_kwargs, direction, target_cell,
                need_response=True)
        response = tgt_message.process()
        self.assertTrue(response.failure)
        self.assertRaises(exception.CellMaxHopCountReached,
                response.value_or_raise)

    def test_targetted_message_invalid_cell(self):
        target_cell = 'api-cell!child-cell2!grandchild-cell4'
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        tgt_message = msg_handler.create_targetted_message(self.ctxt,
                method, method_kwargs, direction, target_cell,
                need_response=True)
        response = tgt_message.process()
        self.assertTrue(response.failure)
        self.assertRaises(exception.CellRoutingInconsistency,
                response.value_or_raise)

    def test_targetted_message_invalid_cell2(self):
        target_cell = 'unknown-cell!child-cell2'
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        tgt_message = msg_handler.create_targetted_message(self.ctxt,
                method, method_kwargs, direction, target_cell,
                need_response=True)
        response = tgt_message.process()
        self.assertTrue(response.failure)
        self.assertRaises(exception.CellRoutingInconsistency,
                response.value_or_raise)

    def test_broadcast_routing(self):
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        cells = set()

        def our_fake_method(message, **kwargs):
            cells.add(message.routing_path)

        fakes.stub_bcast_methods(self, 'our_fake_method', our_fake_method)

        bcast_message = msg_handler.create_broadcast_message(self.ctxt,
                method, method_kwargs, direction, run_locally=True)
        bcast_message.process()
        # fakes creates 8 cells (including ourself).
        self.assertEqual(len(cells), 8)

    def test_broadcast_routing_up(self):
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'up'
        msg_handler = fakes.get_message_handler('grandchild-cell3')

        cells = set()

        def our_fake_method(message, **kwargs):
            cells.add(message.routing_path)

        fakes.stub_bcast_methods(self, 'our_fake_method', our_fake_method)

        bcast_message = msg_handler.create_broadcast_message(self.ctxt,
                method, method_kwargs, direction, run_locally=True)
        bcast_message.process()
        # Paths are reversed, since going 'up'
        expected = set(['grandchild-cell3', 'grandchild-cell3!child-cell3',
                        'grandchild-cell3!child-cell3!api-cell'])
        self.assertEqual(expected, cells)

    def test_broadcast_routing_without_ourselves(self):
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        cells = set()

        def our_fake_method(message, **kwargs):
            cells.add(message.routing_path)

        fakes.stub_bcast_methods(self, 'our_fake_method', our_fake_method)

        bcast_message = msg_handler.create_broadcast_message(self.ctxt,
                method, method_kwargs, direction, run_locally=False)
        bcast_message.process()
        # fakes creates 8 cells (including ourself).  So we should see
        # only 7 here.
        self.assertEqual(len(cells), 7)

    def test_broadcast_routing_with_response(self):
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        def our_fake_method(message, **kwargs):
            return 'response-%s' % message.routing_path

        fakes.stub_bcast_methods(self, 'our_fake_method', our_fake_method)

        bcast_message = msg_handler.create_broadcast_message(self.ctxt,
                method, method_kwargs, direction, run_locally=True,
                need_response=True)
        responses = bcast_message.process()
        self.assertEqual(len(responses), 8)
        for response in responses:
            self.assertFalse(response.failure)
            self.assertEqual('response-%s' % response.cell_name,
                    response.value_or_raise())

    def test_broadcast_routing_with_response_max_hops(self):
        self.flags(max_hop_count=2, group='cells')
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        def our_fake_method(message, **kwargs):
            return 'response-%s' % message.routing_path

        fakes.stub_bcast_methods(self, 'our_fake_method', our_fake_method)

        bcast_message = msg_handler.create_broadcast_message(self.ctxt,
                method, method_kwargs, direction, run_locally=True,
                need_response=True)
        responses = bcast_message.process()
        # Should only get responses from our immediate children (and
        # ourselves)
        self.assertEqual(len(responses), 5)
        for response in responses:
            self.assertFalse(response.failure)
            self.assertEqual('response-%s' % response.cell_name,
                    response.value_or_raise())

    def test_broadcast_routing_with_all_erroring(self):
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        def our_fake_method(message, **kwargs):
            raise test.TestingException('fake failure')

        fakes.stub_bcast_methods(self, 'our_fake_method', our_fake_method)

        bcast_message = msg_handler.create_broadcast_message(self.ctxt,
                method, method_kwargs, direction, run_locally=True,
                need_response=True)
        responses = bcast_message.process()
        self.assertEqual(len(responses), 8)
        for response in responses:
            self.assertTrue(response.failure)
            self.assertRaises(test.TestingException, response.value_or_raise)

    def test_broadcast_routing_with_two_erroring(self):
        method = 'our_fake_method'
        method_kwargs = dict(arg1=1, arg2=2)
        direction = 'down'
        msg_handler = fakes.get_message_handler('api-cell')

        def our_fake_method_failing(message, **kwargs):
            raise test.TestingException('fake failure')

        def our_fake_method(message, **kwargs):
            return 'response-%s' % message.routing_path

        fakes.stub_bcast_methods(self, 'our_fake_method', our_fake_method)
        fakes.stub_bcast_method(self, 'child-cell2', 'our_fake_method',
                                our_fake_method_failing)
        fakes.stub_bcast_method(self, 'grandchild-cell3', 'our_fake_method',
                                our_fake_method_failing)

        bcast_message = msg_handler.create_broadcast_message(self.ctxt,
                method, method_kwargs, direction, run_locally=True,
                need_response=True)
        responses = bcast_message.process()
        self.assertEqual(len(responses), 8)
        failure_responses = [resp for resp in responses if resp.failure]
        success_responses = [resp for resp in responses if not resp.failure]
        self.assertEqual(len(failure_responses), 2)
        self.assertEqual(len(success_responses), 6)

        for response in success_responses:
            self.assertFalse(response.failure)
            self.assertEqual('response-%s' % response.cell_name,
                    response.value_or_raise())

        for response in failure_responses:
            self.assertIn(response.cell_name, ['api-cell!child-cell2',
                    'api-cell!child-cell3!grandchild-cell3'])
            self.assertTrue(response.failure)
            self.assertRaises(test.TestingException, response.value_or_raise)


class CellsTargettedMethodsTestCase(test.TestCase):
    """Test case for _TargettedMessageMethods class."""

    def setUp(self):
        super(CellsTargettedMethodsTestCase, self).setUp()
        fakes.init(self)
        self.msg_handler = fakes.get_message_handler('api-cell')
        self.methods_cls = self.msg_handler.methods_by_type['targetted']
        self.ctxt = 'fake_context'
        self.message = self.msg_handler.create_targetted_message(
                self.ctxt, 'fake_method', 'fake_kwargs',
                'down', 'target_cell')
        self.state_manager = self.msg_handler.state_manager
        self.db = self.methods_cls.db

    def test_schedule_run_instance(self):
        host_sched_kwargs = {'filter_properties': {},
                             'key1': 'value1',
                             'key2': 'value2'}
        scheduler = self.msg_handler.scheduler
        self.mox.StubOutWithMock(scheduler, 'run_instance')
        scheduler.run_instance(self.ctxt, **host_sched_kwargs)
        self.mox.ReplayAll()
        self.methods_cls.schedule_run_instance(self.message,
                **host_sched_kwargs)

    def test_call_compute_api_method(self):
        compute_api = self.methods_cls.compute_api
        instance_uuid = 'fake_instance_uuid'
        method_info = {'method': 'reboot',
                       'method_args': (instance_uuid, 2, 3),
                       'method_kwargs': {'arg1': 'val1', 'arg2': 'val2'}}
        self.mox.StubOutWithMock(compute_api, 'reboot')
        self.mox.StubOutWithMock(self.db, 'instance_get_by_uuid')

        self.db.instance_get_by_uuid(self.ctxt,
                                     instance_uuid).AndReturn(
                                             'fake_instance')
        compute_api.reboot(self.ctxt, 'fake_instance', 2, 3,
                arg1='val1', arg2='val2').AndReturn('fake_result')
        self.mox.ReplayAll()
        result = self.methods_cls.call_compute_api_method(self.message,
                method_info)
        self.assertEqual('fake_result', result)

    def test_call_compute_api_method_unknown_instance(self):
        # Unknown instance should send a broadcast up that instance
        # is gone.
        instance_uuid = 'fake_instance_uuid'
        method_info = {'method': 'reboot',
                       'method_args': (instance_uuid, 2, 3),
                       'method_kwargs': {'arg1': 'val1', 'arg2': 'val2'}}

        class FakeMessage(object):
            def process(self):
                pass

        fake_msg = FakeMessage()

        self.mox.StubOutWithMock(self.db, 'instance_get_by_uuid')
        self.mox.StubOutWithMock(self.msg_handler,
                                 'create_broadcast_message')
        self.mox.StubOutWithMock(fake_msg, 'process')

        self.db.instance_get_by_uuid(self.ctxt,
                                     'fake_instance_uuid').AndRaise(
                                             exception.InstanceNotFound)
        kwargs = {'instance': {'uuid': instance_uuid}}
        self.msg_handler.create_broadcast_message(self.ctxt,
                'instance_destroy_at_top', kwargs, 'up',
                run_locally=False).AndReturn(fake_msg)
        fake_msg.process()

        self.mox.ReplayAll()
        self.assertRaises(exception.InstanceNotFound,
                          self.methods_cls.call_compute_api_method,
                          self.message, method_info)

    def test_update_capabilities(self):
        cell_name = 'fake_cell_name'
        capabs = 'fake_capabilities'
        self.mox.StubOutWithMock(self.state_manager,
                                 'update_cell_capabilities')
        self.mox.StubOutWithMock(self.msg_handler,
                                 'tell_parents_our_capabilities')
        self.state_manager.update_cell_capabilities(cell_name, capabs)
        self.msg_handler.tell_parents_our_capabilities(self.ctxt)
        self.mox.ReplayAll()
        self.methods_cls.update_capabilities(self.message, cell_name,
                capabs)

    def test_update_capacities(self):
        cell_name = 'fake_cell_name'
        capacs = 'fake_capacities'
        self.mox.StubOutWithMock(self.state_manager,
                                 'update_cell_capacities')
        self.mox.StubOutWithMock(self.msg_handler,
                                 'tell_parents_our_capacities')
        self.state_manager.update_cell_capacities(cell_name, capacs)
        self.msg_handler.tell_parents_our_capacities(self.ctxt)
        self.mox.ReplayAll()
        self.methods_cls.update_capacities(self.message, cell_name,
                capacs)

    def test_announce_capabilities(self):
        self.mox.StubOutWithMock(self.msg_handler,
                                 'tell_parents_our_capabilities')
        self.msg_handler.tell_parents_our_capabilities(self.ctxt)
        self.mox.ReplayAll()
        self.methods_cls.announce_capabilities(self.message)

    def test_announce_capacities(self):
        self.mox.StubOutWithMock(self.msg_handler,
                                 'tell_parents_our_capacities')
        self.msg_handler.tell_parents_our_capacities(self.ctxt)
        self.mox.ReplayAll()
        self.methods_cls.announce_capacities(self.message)


class CellsBroadcastMethodsTestCase(test.TestCase):
    """Test case for _TargettedMessageMethods class."""

    def setUp(self):
        super(CellsBroadcastMethodsTestCase, self).setUp()
        fakes.init(self)
        self.msg_handler = fakes.get_message_handler('api-cell')
        self.methods_cls = self.msg_handler.methods_by_type['broadcast']
        self.ctxt = context.RequestContext('fake', 'fake')
        self.message = self.msg_handler.create_broadcast_message(
                self.ctxt, 'fake_method', 'fake_kwargs',
                'down')
        self.state_manager = self.msg_handler.state_manager
        self.db = self.methods_cls.db

    def test_at_the_top(self):
        child_msg_handler = fakes.get_message_handler('grandchild-cell3')
        child_methods_cls = child_msg_handler.methods_by_type['broadcast']
        self.assertTrue(self.methods_cls._at_the_top())
        self.assertFalse(child_methods_cls._at_the_top())

    def test_instance_update_at_top(self):
        fake_info_cache = {'id': 1,
                           'instance': 'fake_instance',
                           'other': 'moo'}
        fake_sys_metadata = [{'id': 1,
                              'key': 'key1',
                              'value': 'value1'},
                             {'id': 2,
                              'key': 'key2',
                              'value': 'value2'}]
        fake_instance = {'id': 2,
                         'uuid': 'fake_uuid',
                         'security_groups': 'fake',
                         'instance_type': 'fake',
                         'volumes': 'fake',
                         'cell_name': 'fake',
                         'name': 'fake',
                         'metadata': 'fake',
                         'info_cache': fake_info_cache,
                         'system_metadata': fake_sys_metadata,
                         'other': 'meow'}
        expected_sys_metadata = {'key1': 'value1',
                                 'key2': 'value2'}
        expected_info_cache = {'other': 'moo'}
        expected_instance = {'system_metadata': expected_sys_metadata,
                             'other': 'meow',
                             'uuid': 'fake_uuid'}

        self.mox.StubOutWithMock(self.db, 'instance_update')
        self.mox.StubOutWithMock(self.db, 'instance_info_cache_update')

        self.db.instance_update(self.ctxt, 'fake_uuid',
                                expected_instance, update_cells=False)
        self.db.instance_info_cache_update(self.ctxt, 'fake_uuid',
                                           expected_info_cache,
                                           update_cells=False)
        self.mox.ReplayAll()
        self.methods_cls.instance_update_at_top(self.message,
                instance_info=fake_instance)

    def test_instance_destroy_at_top(self):
        self.mox.StubOutWithMock(self.db, 'instance_destroy')
        self.db.instance_destroy(self.ctxt, 'fake_uuid',
                                 update_cells=False)
        self.mox.ReplayAll()
        self.methods_cls.instance_destroy_at_top(self.message,
                instance_info={'uuid': 'fake_uuid'})

    def test_instance_delete_everywhere(self):
        compute_api = self.methods_cls.compute_api
        instance = {'uuid': 'meow'}
        self.mox.StubOutWithMock(compute_api, 'delete')
        compute_api.delete(self.ctxt, instance)
        self.mox.ReplayAll()
        self.methods_cls.instance_delete_everywhere(self.message,
                instance=instance, delete_type='hard')

    def test_instance_delete_everywhere_soft(self):
        compute_api = self.methods_cls.compute_api
        instance = {'uuid': 'meow'}
        self.mox.StubOutWithMock(compute_api, 'soft_delete')
        compute_api.soft_delete(self.ctxt, instance)
        self.mox.ReplayAll()
        self.methods_cls.instance_delete_everywhere(self.message,
                instance=instance, delete_type='soft')

    def test_instance_fault_create_at_top(self):
        fake_instance_fault = {'id': 1,
                               'other stuff': 2,
                               'more stuff': 3}
        expected_instance_fault = {'other stuff': 2,
                                   'more stuff': 3}
        self.mox.StubOutWithMock(self.db, 'instance_fault_create')
        self.db.instance_fault_create(self.ctxt, expected_instance_fault)
        self.mox.ReplayAll()
        self.methods_cls.instance_fault_create_at_top(self.message,
                instance_fault=fake_instance_fault)

    def test_bw_usage_update_at_top(self):
        fake_bw_update_info = {'uuid': 'fake_uuid',
                               'mac': 'fake_mac',
                               'start_period': 'fake_start_period',
                               'bw_in': 'fake_bw_in',
                               'bw_out': 'fake_bw_out',
                               'last_ctr_in': 'fake_last_ctr_in',
                               'last_ctr_out': 'fake_last_ctr_out',
                               'last_refreshed': 'fake_last_refreshed'}
        self.mox.StubOutWithMock(self.db, 'bw_usage_update')
        self.db.bw_usage_update(self.ctxt, **fake_bw_update_info)
        self.mox.ReplayAll()
        self.methods_cls.bw_usage_update_at_top(self.message,
                bw_update_info=fake_bw_update_info)
