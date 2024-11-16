import requests
import socket
import threading
from sseclient import SSEClient as EventSource

HOST = 'localhost'
PORT = 8761

def stream_wikimedia_changes():
    """Stream data from Wikimedia and send it to a local socket."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen(1)
        print(f"Socket server started at {HOST}:{PORT}. Waiting for connection...")

        conn, addr = server_socket.accept()
        print(f"Connection established with {addr}")

        url = "https://stream.wikimedia.org/v2/stream/recentchange"

        for event in EventSource(url):
            if event.event == 'message':
                try:
                    conn.sendall(event.data.encode('utf-8') + b'\n')
                except BrokenPipeError:
                    print("Connection closed by client.")
                    break

if __name__ == '__main__':
    # Run the server in a separate thread
    stream_wikimedia_changes()
