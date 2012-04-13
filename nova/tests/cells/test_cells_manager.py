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
