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
Cells RPC Communication Driver
"""
from nova.cells import driver
from nova.openstack.common import cfg
from nova.openstack.common import rpc
from nova.openstack.common.rpc import dispatcher as rpc_dispatcher
from nova.openstack.common.rpc import proxy as rpc_proxy

cell_rpc_driver_opts = [
        cfg.StrOpt('rpc_driver_queue_base',
                   default='cells.intercell',
                   help="Base queue name to use when communicating between "
                        "cells.  Various topics by message type will be "
                        "appended to this.")]

CONF = cfg.CONF
CONF.register_opts(cell_rpc_driver_opts, group='cells')
CONF.import_opt('call_timeout', 'nova.cells.opts', group='cells')

_CELL_TO_CELL_RPC_API_VERSION = '1.0'


class CellsRPCDriver(driver.BaseCellsDriver):
    """Driver for cell<->cell communication via RPC.

    This is only called from the local cell initiating messages.  Receiving
    messages from other cells will happen via the InterCellRPCDispatcher.
    """
    BASE_RPC_API_VERSION = _CELL_TO_CELL_RPC_API_VERSION

    def __init__(self, *args, **kwargs):
        super(CellsRPCDriver, self).__init__(*args, **kwargs)
        self.rpc_connections = []
        self.intercell_rpcapi = InterCellRPCAPI(
                self.BASE_RPC_API_VERSION)

    def _start_consumer(self, proxy_manager, topic, fanout=False,
            host_too=False):
        """Start an RPC consumer."""
        dispatcher = rpc_dispatcher.RpcDispatcher([proxy_manager])
        conn = rpc.create_connection(new=True)
        conn.create_consumer(topic, dispatcher, fanout=fanout)
        if host_too:
            topic += '.' + CONF.host
        conn.create_consumer(topic, dispatcher, fanout=fanout)
        self.rpc_connections.append(conn)
        conn.consume_in_thread()
        return conn

    def start_consumers(self, message_handler):
        """Start RPC consumers.

        Start up 2 separate consumers for handling inter-cell
        communication via RPC.  Both handle the same types of
        messages, but requests/replies are separated to solve
        potential deadlocks. (If we used the same queue for both,
        it's possible to exhaust the RPC thread pool while we wait
        for replies.. such that we'd never consume a reply.)
        """
        topic_base = CONF.cells.rpc_driver_queue_base
        proxy_manager = InterCellRPCDispatcher(message_handler)
        for msg_type in message_handler.get_message_types():
            if msg_type == 'response':
                host_too = True
                fanout = False
            else:
                host_too = False
                fanout = True
            topic = '%s.%s' % (topic_base, msg_type)
            self._start_consumer(proxy_manager, topic,
                    fanout=fanout, host_too=host_too)

    def stop_consumers(self):
        """Stop RPC consumers.

        NOTE: Currently there's no hooks when stopping services
        to have managers cleanup, so this is not currently called.
        """
        for conn in self.rpc_connections:
            conn.close()

    def send_message_to_cell(self, cell_state, message):
        self.intercell_rpcapi.process_message(cell_state, message)


class InterCellRPCAPI(rpc_proxy.RpcProxy):
    """Client side of the Cell<->Cell RPC API.

    This is used when the current cell needs to send a message to
    another cell.  This could be called from the InterCellRPCDispatcher
    (if the message's origin is another cell) or from the CellsRPCDriver
    (if the message's origin is this cell).

    API version history:
        1.0 - Initial version.
    """
    def __init__(self, default_version):
        super(InterCellRPCAPI, self).__init__(None, default_version)

    def _get_server_params_for_cell(self, next_hop):
        """Turn the DB information for a cell into the parameters
        needed for the RPC call.
        """
        param_map = {'username': 'username',
                     'password': 'password',
                     'rpc_host': 'hostname',
                     'rpc_port': 'port',
                     'rpc_virtual_host': 'virtual_host'}
        server_params = {}
        for source, target in param_map.items():
            if next_hop.db_info[source]:
                server_params[target] = next_hop.db_info[source]
        return server_params

    def send_message_to_cell(self, cell_state, message):
        ctxt = message.ctxt
        json_message = message.to_json()
        rpc_message = self.make_msg('process_message', message=json_message)
        topic_base = CONF.cells.rpc_driver_queue_base
        topic = '%s.%s' (topic_base, message.message_type)
        server_params = self._get_server_params_for_cell(cell_state)
        if message.fanout:
            self.fanout_cast_to_server(ctxt, server_params,
                    rpc_message, topic=topic)
        else:
            self.cast_to_server(ctxt, server_params,
                    rpc_message, topic=topic)


class InterCellRPCDispatcher(object):
    """RPC Dispatcher to handle messages received from other cells.

    All messages received here have come from a sibling cell.  Depending
    on the ultimate target and type of message, we may process the message
    in this cell, relay the message to another sibling cell, or both.

    If we're processing the message locally and a response is desired,
    we encapsulate the response in a ResponseMessage and route the message
    back to the sender.
    """
    BASE_RPC_API_VERSION = _CELL_TO_CELL_RPC_API_VERSION

    def __init__(self, message_handler):
        """Init the Intercell RPC Dispatcher."""
        self.message_handler = message_handler

    def process_message(self, ctxt, message):
        """We received a routing message.  If it's for us, process it,
        otherwise forward it to the next hop.

        If a reply to desired, a ResponseMessage will be routed back to the
        caller.
        """
        message = self.message_handler.message_from_json(message)
        message.process()
