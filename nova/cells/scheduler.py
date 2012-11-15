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
Cells Scheduler
"""
import copy
import time

from nova.cells import filters
from nova.cells import weights
from nova import compute
from nova.compute import vm_states
from nova.db import base
from nova import exception
from nova.openstack.common import cfg
from nova.openstack.common import log as logging
from nova.scheduler import rpcapi as scheduler_rpcapi

cell_scheduler_opts = [
        cfg.ListOpt('scheduler_filter_classes',
                default=['nova.cells.filters.all_filters'],
                help='Filter classes the cells scheduler should use.  '
                        'An entry of "nova.cells.filters.standard_filters"'
                        'maps to all cells filters included with nova.'),
        cfg.ListOpt('scheduler_weight_classes',
                default=['nova.cells.weights.all_weighers'],
                help='Weigher classes the cells scheduler should use.  '
                        'An entry of "nova.cells.weights.standard_weighters"'
                        'maps to all cell weighters included with nova.'),
        cfg.IntOpt('scheduler_retries',
                default=10,
                help='How many retries when no cells are available.'),
        cfg.IntOpt('scheduler_retry_delay',
                default=2,
                help='How often to retry in seconds when no cells are '
                        'available.')
]

LOG = logging.getLogger(__name__)

CONF = cfg.CONF
CONF.register_opts(cell_scheduler_opts, group='cells')


class CellsScheduler(base.Base):
    """The cells scheduler."""

    def __init__(self, message_handler):
        super(CellsScheduler, self).__init__()
        self.message_handler = message_handler
        self.state_manager = message_handler.state_manager
        self.compute_api = compute.API()
        self.scheduler_rpcapi = scheduler_rpcapi.SchedulerAPI()
        self.filter_handler = filters.CellFilterHandler()
        self.filter_classes = self.filter_handler.get_matching_classes(
                CONF.cells.scheduler_filter_classes)
        self.weight_handler = weights.CellWeightHandler()
        self.weigher_classes = self.weight_handler.get_matching_classes(
                CONF.cells.scheduler_weight_classes)

    def _create_instances_here(self, context, request_spec):
        instance_values = request_spec['instance_properties']
        for instance_uuid in request_spec['instance_uuids']:
            instance_values['uuid'] = instance_uuid
            instance = self.compute_api.create_db_entry_for_new_instance(
                    context,
                    request_spec['instance_type'],
                    request_spec['image'],
                    instance_values,
                    request_spec['security_group'],
                    request_spec['block_device_mapping'])
            message = self.message_handler.create_broadcast_message(context,
                    'instance_update_at_top',
                    dict(instance=instance), 'up')
            message.process()

    def _get_possible_cells(self):
        cells = set(self.state_manager.get_child_cells())
        our_cell = self.state_manager.get_my_info()
        # Include our cell in the list, if we have any capacity info
        if not cells or our_cell.capacities:
            cells.add(our_cell)
        return cells

    def _filter_cells(self, cells, filter_properties):
        for filter_cls in self.filter_classes:
            filter_inst = filter_cls()
            fn = getattr(filter_inst, 'filter_cells')
            if not fn:
                continue
            filter_response = fn(cells, filter_properties)
            if not filter_response:
                continue
            if 'action' in filter_response:
                return filter_response
            if 'drop' in filter_response:
                for cell in filter_response.get('drop', []):
                    try:
                        cells.remove(cell)
                    except KeyError:
                        pass
        return None

    def _run_instance(self, context, routing_path, **host_sched_kwargs):
        """Attempt to schedule instance(s).  If we have no cells
        to try, raise exception.NoCellsAvailable
        """
        request_spec = host_sched_kwargs['request_spec']

        filter_properties = copy.copy(host_sched_kwargs['filter_properties'])
        filter_properties.update({'context': context,
                              'scheduler': self,
                              'routing_path': routing_path,
                              'request_spec': request_spec})

        # The message we might forward to a child cell
        cells = self._get_possible_cells()
        filter_resp = self._filter_cells(cells, filter_properties)
        if filter_resp and 'action' in filter_resp:
            if filter_resp['action'] == 'direct_route':
                target = filter_resp['target']
                if target == routing_path:
                    # Ah, it's for me.
                    cells = [self.state_manager.get_my_info()]
                else:
                    message = self.message_handler.create_targetted_message(
                            context, 'schedule_run_instance',
                            host_sched_kwargs,
                            target)
                    message.process()
                    return
        if not cells:
            raise exception.NoCellsAvailable()

        LOG.debug(_("Scheduling with routing_path=%(routing_path)s"),
                locals())

        weighted_cells = self.weight_handler.get_weighed_objects(
                self.weigher_classes, cells, filter_properties)
        LOG.debug(_("Weighted cells: %(weighted_cells)s"), locals())

        # Keep trying until one works
        for weighted_cell in weighted_cells:
            cell = weighted_cell.obj
            try:
                if cell.is_me:
                    # Need to create instance DB entry as scheduler
                    # thinks it's already created... At least how things
                    # currently work.
                    self._create_instances_here(context, request_spec)
                    self.scheduler_rpcapi.run_instance(context,
                            **host_sched_kwargs)
                    return
                # Forward request to cell
                message = self.message_handler.create_targetted_message(
                        context, 'schedule_run_instance', host_sched_kwargs,
                        cell)
                message.process()
                return
            except Exception:
                LOG.exception(_("Couldn't communicate with cell '%s'") %
                        cell.name)
        # FIXME(comstud): Would be nice to kick this back up so that
        # the parent cell could retry, if we had a parent.
        msg = _("Couldn't communicate with any cells")
        LOG.error(msg)
        raise exception.NoCellsAvailable()

    def run_instance(self, ctxt, **host_sched_kwargs):
        """Pick a cell where we should create a new instance."""
        try:
            for i in xrange(max(0, CONF.cells.scheduler_retries) + 1):
                try:
                    return self._run_instance(ctxt, **host_sched_kwargs)
                except exception.NoCellsAvailable:
                    if i == max(0, CONF.cells.scheduler_retries):
                        raise
                    sleep_time = max(1, CONF.cells.scheduler_retry_delay)
                    LOG.info(_("No cells available when scheduling.  Will "
                            "retry in %(sleep_time)s second(s)"), locals())
                    time.sleep(sleep_time)
                    continue
        except Exception:
            request_spec = host_sched_kwargs['request_spec']
            instance_uuids = request_spec['instance_uuids']
            LOG.exception(_("Error scheduling instances %(instance_uuids)s"),
                    locals())
            for instance_uuid in instance_uuids:
                if self.state_manager.get_parent_cells():
                    self.cells_rpcapi.instance_update(ctxt,
                            {'uuid': instance_uuid,
                             'vm_state': vm_states.ERROR})
                else:
                    self.db.instance_update(ctxt,
                            instance_uuid,
                            {'vm_state': vm_states.ERROR})
