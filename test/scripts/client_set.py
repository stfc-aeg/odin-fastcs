import time

from odin_data.control.ipc_message import IpcMessage, IpcMessageException
from odin_data.control.ipc_channel import IpcChannel

def main():

    endpoint = "tcp://127.0.0.1:5000"

    channel = IpcChannel(IpcChannel.CHANNEL_TYPE_DEALER, endpoint=endpoint)
    channel.connect()

    msg = IpcMessage("cmd", "request_adapters", id = 1)
    channel.send(msg.encode())
    response = IpcMessage(from_str=channel.recv())
    adapters = response.get_param("adapters", {})
    print(f"Adapters loaded into server: {adapters}")

    msg = IpcMessage("cmd", "set", id=2)
    msg.set_param("paths", {
        "workshop/background_task/enable": True,
        "workshop/background_task/interval": 0.5,
    })
    channel.send(msg.encode())
    response = IpcMessage(from_str=channel.recv())
    print(response)

    channel.close()

if __name__ == '__main__':
    main()