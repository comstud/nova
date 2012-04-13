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
Tests For CellsScheduler
"""

from nova import test
from nova.tests.cells import fakes


class CellsSchedulerTestCase(test.TestCase):
    """Test case for CellsScheduler class"""

    def setUp(self):
        super(CellsSchedulerTestCase, self).setUp()
        fakes.init(self)
        self.cells_manager = fakes.get_cell_manager('api-cell')

    def test_setup(self):
        self.assertTrue(self.cells_manager.scheduler)
