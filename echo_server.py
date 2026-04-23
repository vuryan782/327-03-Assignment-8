# CECS 327 Assignment 8
# Group members: Ryan Vu, Brandon Samson

import socket

from db import (
    get_average_moisture,
    get_average_water_consumption,
    get_house_electricity_totals_24h,
)


MOISTURE_QUERY = "What is the average moisture inside our kitchen fridges in the past hours, week and month?"
WATER_QUERY = "What is the average water consumption per cycle across our smart dishwashers in the past hour, week and month?"
ELECTRICITY_QUERY = "Which house consumed more electricity in the past 24 hours, and by how much?"


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


def handle_query(message: str) -> str:
    """
    Routes the incoming client query to the correct database/calculation function.
    """
    if message == MOISTURE_QUERY:
        hour_avg = get_average_moisture(1)
        week_avg = get_average_moisture(24 * 7)
        month_avg = get_average_moisture(24 * 30)

        if hour_avg is None and week_avg is None and month_avg is None:
            return "No moisture data found."

        return (
            "Average fridge moisture:\n"
            f"Past hour: {hour_avg:.2f}%\n"
            f"Past week: {week_avg:.2f}%\n"
            f"Past month: {month_avg:.2f}%"
        )

    elif message == WATER_QUERY:
        hour_avg = get_average_water_consumption(1)
        week_avg = get_average_water_consumption(24 * 7)
        month_avg = get_average_water_consumption(24 * 30)

        if hour_avg is None and week_avg is None and month_avg is None:
            return "No dishwasher water consumption data found."

        return (
            "Average dishwasher water consumption per cycle:\n"
            f"Past hour: {hour_avg:.2f} gallons\n"
            f"Past week: {week_avg:.2f} gallons\n"
            f"Past month: {month_avg:.2f} gallons"
        )

    elif message == ELECTRICITY_QUERY:
        totals = get_house_electricity_totals_24h()

        if not totals or len(totals) < 2:
            return "Not enough house electricity data found to compare both houses."

        house_a = totals[0]
        house_b = totals[1]

        if house_a["total_current_usage"] > house_b["total_current_usage"]:
            winner = house_a["house"]
            loser = house_b["house"]
            diff = house_a["total_current_usage"] - house_b["total_current_usage"]
        else:
            winner = house_b["house"]
            loser = house_a["house"]
            diff = house_b["total_current_usage"] - house_a["total_current_usage"]

        return (
            f"{winner} consumed more electricity than {loser} in the past 24 hours.\n"
            f"Difference: {diff:.2f} amps"
        )

    else:
        return "Sorry, this query cannot be processed. Please try one of the supported queries."


def run_server() -> None:
    host = "0.0.0.0"
    port = get_server_port()

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((host, port))
            server_socket.listen(1)

            print(f"Server listening on {host}:{port}")
            print("Waiting for a client to connect...")

            conn, addr = server_socket.accept()
            with conn:
                print(f"Client connected: {addr[0]}:{addr[1]}")
                print("Type Ctrl+C to stop the server.")

                while True:
                    data = conn.recv(4096)
                    if not data:
                        print("Client disconnected.")
                        break

                    message = data.decode("utf-8", errors="replace").strip()
                    print(f"Received: {message}")

                    try:
                        response = handle_query(message)
                    except Exception as e:
                        response = f"Server error while processing query: {e}"

                    conn.sendall(response.encode("utf-8"))
                    print(f"Sent: {response}")

    except OSError as e:
        print(f"Socket error: {e}")
    except KeyboardInterrupt:
        print("\nServer stopped by user.")


if __name__ == "__main__":
    run_server()
