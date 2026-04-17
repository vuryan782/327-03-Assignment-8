# CECS 327 Assignment 8
# Group members: Ryan Vu, Brandon Samson

import socket


# Asks user for a valid port
def get_server_port() -> int:

    while True:
        raw_port = input("Enter server port (1-65535): ").strip()
        try:
            port = int(raw_port)
            if 1 <= port <= 65535:
                return port
            print("Error: Port must be between 1 and 65535.")
        except ValueError:
            print("Error: Port must be a number.")


# Starts TCP server and echos back uppercase messages
def run_server() -> None:
    host = "0.0.0.0"  # Listen on all interfaces for LAN/Internet testing
    port = get_server_port()

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket: # Creates TCP socket
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((host, port))
            server_socket.listen(1)

            print(f"Server listening on {host}:{port}")
            print("Waiting for a client to connect...")

            conn, addr = server_socket.accept() # Waits for client to connect
            with conn: # Runs if client connects
                print(f"Client connected: {addr[0]}:{addr[1]}")
                print("Type Ctrl+C to stop the server.")

                while True: # Infinite loop
                    data = conn.recv(1024)
                    if not data: # 
                        print("Client disconnected.")
                        break

                    message = data.decode("utf-8", errors="replace")
                    response = message.upper() # Converts message to uppercase
                    conn.sendall(response.encode("utf-8")) # Sends uppercase message back to client

                    print(f"Received: {message}")
                    print(f"Sent: {response}")

    # Error handling
    except OSError as e:
        print(f"Socket error: {e}")
    except KeyboardInterrupt:
        print("\nServer stopped by user.")


if __name__ == "__main__":
    run_server()
