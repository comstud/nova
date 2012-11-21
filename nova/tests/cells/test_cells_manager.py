# Copyright (c) 2012 Rackspace Hosting
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
Tests For CellsManager
"""
import datetime

from nova.cells import utils as cells_utils
from nova import context
from nova.openstack.common import timeutils
from nova import test
from nova.tests.cells import fakes


class CellsManagerClassTestCase(test.TestCase):
    """Test case for CellsManager class"""

    def setUp(self):
        super(CellsManagerClassTestCase, self).setUp()
        fakes.init(self)
        # pick a child cell to use for tests.
        our_cell = 'grandchild-cell1'
        self.cells_manager = fakes.get_cell_manager(our_cell)
        self.fake_context = 'fake_context'

    def test_get_cell_info_for_siblings(self):
        self.mox.StubOutWithMock(self.cells_manager.state_manager,
                'get_cell_info_for_siblings')
        self.cells_manager.state_manager.get_cell_info_for_siblings()
        self.mox.ReplayAll()
        self.cells_manager.get_cell_info_for_siblings(self.fake_context)

    def test_heal_instances(self):
        self.flags(instance_updated_at_threshold=1000,
                   instance_update_num_instances=2,
                   # force to update on every call
                   instance_update_interval=-1,
                   group='cells')

        fake_context = context.RequestContext('fake', 'fake')
        stalled_time = timeutils.utcnow()
        updated_since = stalled_time - datetime.timedelta(seconds=1000)

        def utcnow():
            return stalled_time

        call_info = {'get_instances': 0, 'sync_instances': []}

        instances = ['instance1', 'instance2', 'instance3']

        def get_instances_to_sync(context, **kwargs):
            self.assertEqual(context, fake_context)
            call_info['shuffle'] = kwargs.get('shuffle')
            call_info['project_id'] = kwargs.get('project_id')
            call_info['updated_since'] = kwargs.get('updated_since')
            call_info['get_instances'] += 1
            return iter(instances)

        def instance_get_by_uuid(context, uuid):
            return instances[int(uuid[-1]) - 1]

        def sync_instance(context, instance):
            self.assertEqual(context, fake_context)
            call_info['sync_instances'].append(instance)

        self.stubs.Set(cells_utils, 'get_instances_to_sync',
                get_instances_to_sync)
        self.stubs.Set(self.cells_manager.db, 'instance_get_by_uuid',
                instance_get_by_uuid)
        self.stubs.Set(self.cells_manager, '_sync_instance',
                sync_instance)
        self.stubs.Set(timeutils, 'utcnow', utcnow)

        self.cells_manager._heal_instances(fake_context)
        self.assertEqual(call_info['shuffle'], True)
        self.assertEqual(call_info['project_id'], None)
        self.assertEqual(call_info['updated_since'], updated_since)
        self.assertEqual(call_info['get_instances'], 1)
        # Only first 2
        self.assertEqual(call_info['sync_instances'],
                instances[:2])

        call_info['sync_instances'] = []
        self.cells_manager._heal_instances(fake_context)
        self.assertEqual(call_info['shuffle'], True)
        self.assertEqual(call_info['project_id'], None)
        self.assertEqual(call_info['updated_since'], updated_since)
        self.assertEqual(call_info['get_instances'], 2)
        # Now the last 1 and the first 1
        self.assertEqual(call_info['sync_instances'],
                [instances[-1], instances[0]])
