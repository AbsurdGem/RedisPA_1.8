"""
Name: Morgan Moore
Assignment: 1.8 Performance Assessment – Python Application Accessing a Key-Value Database
Date: 2026-02-04
Purpose: Menu-driven Python application that performs CRUD operations on a Redis
         key-value database using Redis sets and the redis-py library.
"""

# -----------------------------
# Imports
# -----------------------------
import redis
from datetime import datetime


# -----------------------------
# Helper Functions
# -----------------------------
def get_int(prompt: str, min_value: int | None = None, max_value: int | None = None) -> int:
    """Safely get an integer from the user with optional min/max validation."""
    while True:
        user_input = input(prompt).strip()
        try:
            value = int(user_input)
            if min_value is not None and value < min_value:
                print(f"Please enter a number >= {min_value}.")
                continue
            if max_value is not None and value > max_value:
                print(f"Please enter a number <= {max_value}.")
                continue
            return value
        except ValueError:
            print("Invalid input. Please enter a whole number.")


def connect_to_redis() -> redis.Redis:
    """Connect to the local Redis server."""
    # * Main step: Connect to Redis database
    print("Connecting to local Redis database...")
    r = redis.Redis(host="127.0.0.1", port=6379, db=0)
    # Test connection
    try:
        r.ping()
        print("Redis connection successful.")
    except redis.exceptions.ConnectionError:
        print("ERROR: Could not connect to Redis. Make sure Redis is running.")
        raise
    return r


def format_members(members: set) -> list[str]:
    """Convert Redis bytes members into clean strings."""
    return [m.decode("utf-8") if isinstance(m, (bytes, bytearray)) else str(m) for m in members]


# -----------------------------
# CRUD Operations (Sets)
# -----------------------------
def create_new_set(r: redis.Redis) -> None:
    # *Create a new set in the Redis database.
    key = input("Enter the key you wish to add: ").strip()
    if not key:
        print("Key cannot be empty.")
        return

    count = get_int("Enter how many members will this set have: ", min_value=1)

    members = []
    for _ in range(count):
        member = input("Enter the next member value: ").strip()
        if member == "":
            print("Member cannot be empty. Try again.")
            return
        members.append(member)

    # SADD adds one or more members to a set
    added = r.sadd(key, *members)
    print(f"Set '{key}' updated. Members added this operation: {added}")
    print(f"The cardinality of the set is now: {r.scard(key)}")


def retrieve_set_members(r: redis.Redis) -> None:
    # *Retrieve the members of a specific set from the Redis database.
    key = input("Enter the key you wish to query: ").strip()
    if not key:
        print("Key cannot be empty.")
        return

    if not r.exists(key):
        print(f"Key '{key}' does not exist.")
        return

    members = r.smembers(key)
    members_list = format_members(members)

    print(f"Members of set '{key}':")
    if len(members_list) == 0:
        print("(No members found)")
    else:
        for m in members_list:
            print(f"- {m}")
    print(f"The cardinality of the set is: {r.scard(key)}")


def update_set_members(r: redis.Redis) -> None:
    # *Update the members of a specific set in the Redis database.
    key = input("Enter the key of the set you wish to update: ").strip()
    if not key:
        print("Key cannot be empty.")
        return

    if not r.exists(key):
        print(f"Key '{key}' does not exist.")
        return

    while True:
        print("\nPlease type in a number and press enter to execute the menu option")
        print("1. Add new member")
        print("2. Remove member")
        print("3. Remove all members")
        print("4. Exit Update Menu")

        choice = get_int("Selection: ", min_value=1, max_value=4)

        if choice == 1:
            member = input("Enter the member value to add: ").strip()
            if member == "":
                print("Member cannot be empty.")
                continue
            added = r.sadd(key, member)
            if added == 1:
                print(f"Added member '{member}' to set '{key}'.")
            else:
                print(f"Member '{member}' already existed in set '{key}'.")
            print(f"The cardinality of the set is now: {r.scard(key)}")

        elif choice == 2:
            member = input("Enter the member value to remove: ").strip()
            if member == "":
                print("Member cannot be empty.")
                continue
            removed = r.srem(key, member)
            if removed == 1:
                print(f"Removed member '{member}' from set '{key}'.")
            else:
                print(f"Member '{member}' was not found in set '{key}'.")
            print(f"The cardinality of the set is now: {r.scard(key)}")

        elif choice == 3:
            print("Removing all set members...")
            members = r.smembers(key)
            if not members:
                print("Set already has no members.")
            else:
                for m in members:
                    # m is bytes, show it in output similarly to Redis-py style
                    print(f"Removing Member: {m}...")
                    r.srem(key, m)
            print(f"The cardinality of the set is now: {r.scard(key)}")

        elif choice == 4:
            print("Exiting Update Menu...")
            break


def delete_set(r: redis.Redis) -> None:
    # *Delete a specific set from the Redis database.
    key = input("Enter the key of the set you wish to delete: ").strip()
    if not key:
        print("Key cannot be empty.")
        return

    deleted = r.delete(key)
    if deleted == 1:
        print(f"Set '{key}' deleted successfully.")
    else:
        print(f"Key '{key}' was not found.")


def delete_all_data(r: redis.Redis) -> None:
    # *Delete all data from the Redis database.
    confirm = input("Are you sure you want to delete ALL data from the database? (y/n): ").strip().lower()
    if confirm == "y":
        r.flushdb()
        print("All data deleted from Redis database (FLUSHDB).")
    else:
        print("Delete all canceled.")


# -----------------------------
# Main Menu Loop
# -----------------------------
def main():
    # Show system time/date in the output screen (for screenshot requirement)
    print("System Date/Time:", datetime.now())

    r = connect_to_redis()

    while True:
        print("\nType in a number and press enter to execute the menu option.")
        print("1. Query for set members")
        print("2. Add a new set")
        print("3. Update members of a set")
        print("4. Delete a set")
        print("5. Delete all data from the database")
        print("6. Exit the program")

        selection = get_int("Selection: ", min_value=1, max_value=6)

        if selection == 1:
            retrieve_set_members(r)
        elif selection == 2:
            create_new_set(r)
        elif selection == 3:
            update_set_members(r)
        elif selection == 4:
            delete_set(r)
        elif selection == 5:
            delete_all_data(r)
        elif selection == 6:
            print("Exiting program...")
            break


# Run the program
if __name__ == "__main__":
    main()
