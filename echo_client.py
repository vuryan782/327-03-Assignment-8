# CECS 327 Assignment 5 
# Group members: Matthew Saldivar, Brandon Samson

import socket


# Asks user for a valid server IP or hostname
def get_server_ip() -> str:
    
    while True:
        ip_or_host = input("Enter server IP address or hostname: ").strip()
        if not ip_or_host:
            print("Error: Server IP/hostname cannot be empty.")
            continue
        try:
            # Validate by trying to resolve the input.
            socket.gethostbyname(ip_or_host)
            return ip_or_host
        except socket.gaierror:
            print("Error: Invalid IP address/hostname. Please try again.")


# Asks user for a valid server port
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


# Connects to server, sends messages, and prints responses
def run_client() -> None:
    server_ip = get_server_ip()
    server_port = get_server_port()

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket: # Creates TCP socket
            client_socket.connect((server_ip, server_port)) # Connects to server 
            print(f"Connected to server {server_ip}:{server_port}")
            print("Type messages to send. Type 'quit' to exit.")

            while True: # Infinite loop
                message = input("Message: ")
                if message.lower() == "quit": # Breaks infinite loop if user types "quit"
                    print("Closing connection.")
                    break

                client_socket.sendall(message.encode("utf-8"))
                response = client_socket.recv(1024) # Waits for response from server

                if not response:
                    print("Server closed the connection.")
                    break

                print("Server response:", response.decode("utf-8", errors="replace"))
    # Error handling
    except socket.gaierror:
        print("Error: Invalid server IP address/hostname.")
    except ValueError:
        print("Error: Invalid port number.")
    except ConnectionRefusedError:
        print("Error: Connection refused. Verify the server is running and the port is correct.")
    except TimeoutError:
        print("Error: Connection timed out.")
    except OSError as e:
        print(f"Socket error: {e}")
    except KeyboardInterrupt:
        print("\nClient stopped by user.")


if __name__ == "__main__":
    run_client()
