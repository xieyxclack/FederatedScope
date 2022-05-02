from federatedscope.core.proto import gRPC_comm_manager_pb2, gRPC_comm_manager_pb2_grpc


class gRPCComServeFunc(gRPC_comm_manager_pb2_grpc.gRPCComServeFuncServicer):
    def __init__(self):
        self.msg_list = list()

    def sendMessage(self, request, context):
        self.msg_list.append(request)

        return gRPC_comm_manager_pb2.MessageResponse(msg='ACK')

    def receive(self):
        if len(self.msg_list) > 0:
            msgs = self.msg_list.copy()
            self.msg_list.clear()
            return msgs
        else:
            return None
