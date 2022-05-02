import grpc
from concurrent import futures
from collections import deque

from federatedscope.core.configs.config import global_cfg
from federatedscope.core.proto import gRPC_comm_manager_pb2, gRPC_comm_manager_pb2_grpc
from federatedscope.core.gRPC_server import gRPCComServeFunc
from federatedscope.core.message import Message


class StandaloneCommManager(object):
    """
    The communicator used for standalone mode
    """
    def __init__(self, comm_queue):
        self.comm_queue = comm_queue
        self.neighbors = dict()

    def receive(self):
        # we don't need receive() in standalone
        pass

    def add_neighbors(self, neighbor_id, address=None):
        self.neighbors[neighbor_id] = address

    def get_neighbors(self, neighbor_id=None):
        address = dict()
        if neighbor_id:
            if isinstance(neighbor_id, list):
                for each_neighbor in neighbor_id:
                    address[each_neighbor] = self.get_neighbors(each_neighbor)
                return address
            else:
                return self.neighbors[neighbor_id]
        else:
            # Get all neighbors
            return self.neighbors

    def send(self, message):
        self.comm_queue.append(message)


class gRPCCommManager(object):
    """
        The implementation of gRPCCommManager is referred to the tutorial on https://grpc.io/docs/languages/python/
    """
    def __init__(self, host='0.0.0.0', port='50050', client_num=2):
        self.host = host
        self.port = port
        options = [
            ("grpc.max_send_message_length",
             global_cfg.distribute.grpc_max_send_message_length),
            ("grpc.max_receive_message_length",
             global_cfg.distribute.grpc_max_receive_message_length),
            ("grpc.enable_http_proxy",
             global_cfg.distribute.grpc_enable_http_proxy),
        ]
        self.server_funcs = gRPCComServeFunc()
        self.grpc_server = self.serve(max_workers=client_num,
                                      host=host,
                                      port=port,
                                      options=options)
        self.neighbors = dict()
        self.msg_queue = deque()

    def serve(self, max_workers, host, port, options):
        """
        This function is referred to https://grpc.io/docs/languages/python/basics/#starting-the-server
        """
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=max_workers),
            options=options)
        gRPC_comm_manager_pb2_grpc.add_gRPCComServeFuncServicer_to_server(
            self.server_funcs, server)
        server.add_insecure_port("{}:{}".format(host, port))
        server.start()

        return server

    def add_neighbors(self, neighbor_id, address):
        self.neighbors[neighbor_id] = '{}:{}'.format(address['host'],
                                                     address['port'])

    def get_neighbors(self, neighbor_id=None):
        address = dict()
        if neighbor_id:
            if isinstance(neighbor_id, list):
                for each_neighbor in neighbor_id:
                    address[each_neighbor] = self.get_neighbors(each_neighbor)
                return address
            else:
                return self.neighbors[neighbor_id]
        else:
            #Get all neighbors
            return self.neighbors

    def _send(self, receiver_address, message):
        def _create_stub(receiver_address):
            """
            This part is referred to https://grpc.io/docs/languages/python/basics/#creating-a-stub
            """
            channel = grpc.insecure_channel(receiver_address,
                                            options=(('grpc.enable_http_proxy',
                                                      0), ))
            stub = gRPC_comm_manager_pb2_grpc.gRPCComServeFuncStub(channel)
            return stub, channel

        stub, channel = _create_stub(receiver_address)
        request = message.transform(to_list=True)
        try:
            stub.sendMessage(request)
        except grpc._channel._InactiveRpcError:
            pass
        channel.close()

    def send(self, message):
        receiver = message.receiver
        if receiver is not None:
            if not isinstance(receiver, list):
                receiver = [receiver]
            for each_receiver in receiver:
                if each_receiver in self.neighbors:
                    receiver_address = self.neighbors[each_receiver]
                    self._send(receiver_address, message)
        else:
            for each_receiver in self.neighbors:
                receiver_address = self.neighbors[each_receiver]
                self._send(receiver_address, message)

    def _replace(self, new_message):
        modified_msg_queue = self.msg_queue.copy()
        sender, msg_type, state = new_message.sender, new_message.msg_type, new_message.state
        for message in self.msg_queue:
            if message.strategy == 'replaceable':
                if message.sender == sender and message.msg_type == msg_type and message.state <= state:
                    modified_msg_queue.remove(message)
        self.msg_queue = modified_msg_queue

    def receive(self):
        while True:
            received_msgs = self.server_funcs.receive()
            if received_msgs is not None:
                for received_msg in received_msgs:
                    message = Message()
                    message.parse(received_msg.msg)
                    self._replace(message)
                    self.msg_queue.append(message)

            print('-----------------------------------')
            for mm in self.msg_queue:
            print(message)
            print('-----------------------------------')

            if len(self.msg_queue) > 0:
                return self.msg_queue.popleft()
            else:
                continue
