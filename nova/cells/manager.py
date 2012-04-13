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
Cells Service Manager
"""

from nova.cells import messaging
from nova.cells import state as cells_state
from nova import context
from nova import manager
from nova.openstack.common import cfg
from nova.openstack.common import importutils
from nova.openstack.common import log as logging

cell_manager_opts = [
        cfg.StrOpt('driver',
                default='nova.cells.rpc_driver.CellsRPCDriver',
                help='Cells communication driver to use'),
]


LOG = logging.getLogger(__name__)

CONF = cfg.CONF
CONF.import_opt('capabilities', 'nova.cells.opts', group='cells')
CONF.register_opts(cell_manager_opts, group='cells')


class CellsManager(manager.Manager):
    """The nova-cells manager class.  This class defines RPC
    methods that the local cell may call.  This class is NOT used for
    messages coming from other cells.  That communication is
    driver-specific.

    Communication to other cells happens via the messaging module.  The
    MessageHandler from that module will handle routing the message to
    the correct cell via the communications driver.  Most methods below
    create 'targetted' (where we want to route a message to a specific cell)
    or 'broadcast' (where we want a message to go to multiple cells)
    messages.

    Scheduling requests get passed to the scheduler class.
    """
    RPC_API_VERSION = '1.0'

    def __init__(self, *args, **kwargs):
        # Mostly for tests.
        cell_state_manager = kwargs.pop('cell_state_manager', None)
        super(CellsManager, self).__init__(*args, **kwargs)
        if cell_state_manager is None:
            cell_state_manager = cells_state.CellStateManager
        self.state_manager = cell_state_manager()
        self.message_handler = messaging.MessageHandler(self.state_manager)
        self.scheduler = self.message_handler.scheduler
        cells_driver_cls = importutils.import_class(
                CONF.cells.driver)
        self.driver = cells_driver_cls()

    def _ask_children_for_capabilities(self, ctxt):
        """Tell child cells to send us capabilities.  We do this on
        startup of cells service.
        """
        child_cells = self.state_manager.get_child_cells()
        for child_cell in child_cells:
            message = self.message_handler.create_targetted_message(
                    ctxt, 'announce_capabilities', dict(), child_cell)
            message.process()

    def _ask_children_for_capacities(self, ctxt):
        """Tell child cells to send us capacities.  We do this on
        startup of cells service.
        """
        child_cells = self.state_manager.get_child_cells()
        for child_cell in child_cells:
            message = self.message_handler.create_targetted_message(
                    ctxt, 'announce_capacities', dict(), child_cell)
            message.process()

    def post_start_hook(self):
        """Have the driver start its consumers for inter-cell communication.
        Also ask our child cells for their capacities and capabilities so
        we get them more quickly than just waiting for the next periodic
        update.  Receiving the updates from the children will cause us to
        update our parents.  If we don't have any children, just update
        our parents immediately.
        """
        # FIXME(comstud): There's currently no hooks when services are
        # stopping, so we have no way to stop consumers cleanly.
        self.driver.start_consumers(self.message_handler)
        ctxt = context.get_admin_context()
        if self.state_manager.get_child_cells():
            self._ask_children_for_capabilities(ctxt)
            self._ask_children_for_capacities(ctxt)
        else:
            self.message_handler.tell_parents_our_capabilities(ctxt)
            self.message_handler.tell_parents_our_capacities(ctxt)

    @manager.periodic_task
    def _update_our_parents(self, ctxt):
        """Update our parent cells with our capabilities and capacity
        if we're at the bottom of the tree.
        """
        self.message_handler.tell_parents_our_capabilities(ctxt)
        self.message_handler.tell_parents_our_capacities(ctxt)

    def schedule_run_instance(self, ctxt, **host_sched_kwargs):
        """Pick a cell (possibly ourselves) to build new instance(s)
        and forward the request accordingly.
        """
        self.scheduler.run_instance(ctxt, **host_sched_kwargs)

    def get_cell_info_for_siblings(self, _ctxt):
        """Return cell information for our neighbor cells."""
        return self.state_manager.get_cell_info_for_siblings()

    def run_compute_api_method(self, ctxt, cell_name, method_info, call):
        """Call a compute API method in a specific cell."""
        message = self.message_handler.create_targetted_message(
                ctxt, 'run_compute_api_method',
                dict(method_info=method_info), 'down', cell_name,
                needs_response=call)
        response = message.process()
        return response.value_or_raise()

    def instance_update_at_top(self, ctxt, instance):
        """Update an instance at the top level cell."""
        message = self.message_handler.create_broadcast_message(
                ctxt, 'instance_update_at_top',
                dict(instance=instance), 'up')
        message.process()

    def instance_destroy_at_top(self, ctxt, instance):
        """Destroy an instance at the top level cell."""
        message = self.message_handler.create_broadcast_message(
                ctxt, 'instance_destroy_at_top',
                dict(instance=instance), 'up')
        message.process()

    def instance_delete_everywhere(self, ctxt, instance, delete_type):
        """This is used by API cell when it didn't know what cell
        an instance was in, but the instance was requested to be
        deleted or soft_deleted.  So, we'll broadcast this everywhere.
        """
        message = self.message_handler.create_broadcast_message(
                ctxt, 'instance_delete_everywhere',
                dict(instance=instance, delete_type=delete_type), 'down')
        message.process()

    def instance_fault_create_at_top(self, ctxt, instance_fault):
        """Create an instance fault at the top level cell."""
        message = self.message_handler.create_broadcast_message(
                ctxt, 'instance_fault_create_at_top',
                dict(instance_fault=instance_fault), 'up')
        message.process()

    def bw_usage_update_at_top(self, ctxt, bw_update_info):
        """Update bandwidth usage at top level cell."""
        message = self.message_handler.create_broadcast_message(
                ctxt, 'bw_usage_update_at_top',
                dict(bw_update_info=bw_update_info), 'up')
        message.process()
