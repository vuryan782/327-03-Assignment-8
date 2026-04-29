# CECS 327 Assignment 8
# Group members: Ryan Vu, Brandon Samson

import socket
from typing import Optional

from db import (
    get_average_moisture,
    get_average_water_consumption,
    get_house_electricity_totals_24h,
    get_query_coverage_note,
)


MOISTURE_QUERY = "What is the average moisture inside our kitchen fridges in the past hours, week and month?"
WATER_QUERY = "What is the average water consumption per cycle across our smart dishwashers in the past hour, week and month?"
ELECTRICITY_QUERY = "Which house consumed more electricity in the past 24 hours, and by how much?"

INVALID_QUERY_MESSAGE = "Sorry, this query cannot be processed. Please try one of the supported queries."


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


def _format_optional(value: Optional[float], unit: str) -> str:
    if value is None:
        return "No data"
    return f"{value:.2f} {unit}"


def _format_average_report(
    title: str,
    unit: str,
    one_hour: Optional[float],
    one_week: Optional[float],
    one_month: Optional[float],
) -> str:
    return (
        f"{title}\n"
        f"Past hour: {_format_optional(one_hour, unit)}\n"
        f"Past week: {_format_optional(one_week, unit)}\n"
        f"Past month: {_format_optional(one_month, unit)}\n"
        f"Coverage note: {get_query_coverage_note(24 * 30)}"
    )


def handle_query(message: str) -> str:
    message = message.strip()

    if message == MOISTURE_QUERY:
        return _format_average_report(
            "Average moisture inside kitchen fridges:",
            "%",
            get_average_moisture(1),
            get_average_moisture(24 * 7),
            get_average_moisture(24 * 30),
        )

    if message == WATER_QUERY:
        return _format_average_report(
            "Average water consumption per dishwasher cycle:",
            "gallons",
            get_average_water_consumption(1),
            get_average_water_consumption(24 * 7),
            get_average_water_consumption(24 * 30),
        )

    if message == ELECTRICITY_QUERY:
        totals = get_house_electricity_totals_24h()

        if len(totals) < 2:
            return "Not enough house electricity data found to compare both houses."

        house_a = totals[0]
        house_b = totals[1]

        a_total = float(house_a.get("total_current_usage", 0.0))
        b_total = float(house_b.get("total_current_usage", 0.0))

        if a_total > b_total:
            winner = house_a["house"]
            loser = house_b["house"]
            difference = a_total - b_total
        elif b_total > a_total:
            winner = house_b["house"]
            loser = house_a["house"]
            difference = b_total - a_total
        else:
            return (
                "Both houses consumed the same amount of electricity in the past 24 hours.\n"
                f"{house_a['house']}: {a_total:.2f} amp-reading total "
                f"({house_a.get('reading_count', 0)} readings)\n"
                f"{house_b['house']}: {b_total:.2f} amp-reading total "
                f"({house_b.get('reading_count', 0)} readings)\n"
                f"Coverage note: {get_query_coverage_note(24)}"
            )

        return (
            f"{winner} consumed more electricity than {loser} in the past 24 hours.\n"
            f"Difference: {difference:.2f} amp-reading total.\n"
            f"{house_a['house']}: {a_total:.2f} amp-reading total "
            f"({house_a.get('reading_count', 0)} readings)\n"
            f"{house_b['house']}: {b_total:.2f} amp-reading total "
            f"({house_b.get('reading_count', 0)} readings)\n"
            f"Coverage note: {get_query_coverage_note(24)}"
        )

    return INVALID_QUERY_MESSAGE


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
                    except Exception as exc:
                        response = f"Server error while processing query: {exc}"

                    conn.sendall(response.encode("utf-8"))
                    print(f"Sent: {response}")

    except OSError as exc:
        print(f"Socket error: {exc}")
    except KeyboardInterrupt:
        print("\nServer stopped by user.")


if __name__ == "__main__":
    run_server()
