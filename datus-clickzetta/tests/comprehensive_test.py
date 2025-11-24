"""ClickZetta Connector Comprehensive Functional Test Script"""

import os
import sys


def main():
    print("=== ClickZetta Connector Comprehensive Functionality Test ===")
    print()

    all_ok = True

    # Test environment variable loading
    print("1. 📋 Environment Variable Validation")
    required_vars = [
        "CLICKZETTA_SERVICE",
        "CLICKZETTA_USERNAME",
        "CLICKZETTA_PASSWORD",
        "CLICKZETTA_INSTANCE",
        "CLICKZETTA_WORKSPACE",
        "CLICKZETTA_SCHEMA",
        "CLICKZETTA_VCLUSTER",
    ]

    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        status = "✅" if value else "❌"
        display_value = "[HIDDEN]" if "PASSWORD" in var else value
        print(f"   {status} {var}: {display_value}")
        if not value:
            missing_vars.append(var)

    if missing_vars:
        print(f"   ❌ Missing required environment variables: {missing_vars}")
        return False

    print()

    # Test basic connection
    print("2. 🔌 Basic Connection Test")
    try:
        import clickzetta

        connection = clickzetta.connect(
            service=os.getenv("CLICKZETTA_SERVICE"),
            username=os.getenv("CLICKZETTA_USERNAME"),
            password=os.getenv("CLICKZETTA_PASSWORD"),
            instance=os.getenv("CLICKZETTA_INSTANCE"),
            workspace=os.getenv("CLICKZETTA_WORKSPACE"),
            schema=os.getenv("CLICKZETTA_SCHEMA"),
            vcluster=os.getenv("CLICKZETTA_VCLUSTER"),
        )
        print("   ✅ ClickZetta SDK connection successful")
    except Exception as e:
        print(f"   ❌ ClickZetta SDK connection failed: {e}")
        return False

    # Test SQL queries
    print()
    print("3. 📊 SQL Query Test")
    try:
        cursor = connection.cursor()

        # Test simple query
        cursor.execute('SELECT 1 as test_number, "Hello ClickZetta" as message')
        results = cursor.fetchall()
        print(f"   ✅ Basic query successful: {results}")

        # Test current timestamp query
        cursor.execute("SELECT current_timestamp();")
        time_results = cursor.fetchall()
        print(f'   ✅ Time query successful: {time_results[0] if time_results else "No results"}')

        cursor.close()
    except Exception as e:
        print(f"   ❌ SQL query failed: {e}")
        all_ok = False

    # Test metadata retrieval
    print()
    print("4. 🗂️ Metadata Query Test")
    try:
        cursor = connection.cursor()
        workspace = os.getenv("CLICKZETTA_WORKSPACE")
        schema = os.getenv("CLICKZETTA_SCHEMA")

        # Validate identifiers to prevent injection (basic alphanumeric + underscore)
        import re

        if not workspace or not re.match(r"^[a-zA-Z0-9_]+$", workspace):
            raise ValueError(f"Invalid workspace name: {workspace}")
        if not schema or not re.match(r"^[a-zA-Z0-9_]+$", schema):
            raise ValueError(f"Invalid schema name: {schema}")

        # Get table list (now safely validated)
        cursor.execute(f"SHOW TABLES IN `{workspace}`.`{schema}`")
        tables = cursor.fetchall()
        table_count = len(tables) if tables else 0
        print(f"   ✅ Table list retrieval successful: Found {table_count} tables")

        if table_count > 0:
            print(f'   📝 Example table name: {tables[0][0] if tables else "None"}')

        cursor.close()
    except Exception as e:
        print(f"   ❌ Metadata query failed: {e}")
        all_ok = False

    # Removed use_workspace related tests since this feature is not supported by design

    print()
    print("5. 🧹 Resource Cleanup")
    try:
        connection.close()
        print("   ✅ Connection closed")
    except Exception as e:
        print(f"   ❌ Connection closure failed: {e}")
        all_ok = False

    print()
    print("🎉 Real connection testing completed!")
    if not all_ok:
        print("Some steps failed; see messages above.")
    return all_ok


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
