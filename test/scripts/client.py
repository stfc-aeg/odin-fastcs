
import time

from odin_data.control.ipc_message import IpcMessage, IpcMessageException
from odin_data.control.ipc_channel import IpcChannel

def main():

    endpoint = "tcp://127.0.0.1:5000"

    channel = IpcChannel(IpcChannel.CHANNEL_TYPE_DEALER, endpoint=endpoint)
    channel.connect()

    msg = IpcMessage("cmd", "request_adapters", id = 2)
    channel.send(msg.encode())
    response = IpcMessage(from_str=channel.recv())

    adapters = response.get_param("adapters", {})
    print(f"Adapters loaded into server: {adapters}")

    msg = IpcMessage("cmd", "get", id = 3)
    msg.set_param("paths", [
        "system_info",
        "system_info/platform/node",
        "fastcs/clients"
    ])
    msg.set_param("metadata", True)
    channel.send(msg.encode())
    response = IpcMessage(from_str=channel.recv())
    print(response)

    time.sleep(0.5)

    msg.set_msg_id(4)
    msg.set_param("metadata", False)
    channel.send(msg.encode())
    response = IpcMessage(from_str=channel.recv())
    print(response)

    time.sleep(0.5)

    msg.set_msg_id(5)
    msg.set_param("delta", True)
    channel.send(msg.encode())
    response = IpcMessage(from_str=channel.recv())
    print(response)

    msg = IpcMessage("cmd", "subscribe", id=7)
    msg.set_param("paths", [
        "system_info",
        "system_info/platform/node",
        "fastcs_clients"
    ])
    channel.send(msg.encode())
    response = IpcMessage(from_str=channel.recv())
    print(response)

    msg = IpcMessage("cmd", "bye", id=6)
    channel.send(msg.encode())
    response = IpcMessage(from_str=channel.recv())
    print(response)

    channel.close()

if __name__ == '__main__':
    main()