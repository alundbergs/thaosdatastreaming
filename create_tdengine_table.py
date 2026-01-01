import taos

def create_database_and_table():
    try:
        conn = taos.connect(user="root", password="taosdata", host="localhost", port=6030)
        cursor = conn.cursor()

        # Create database if not exists
        cursor.execute("CREATE DATABASE IF NOT EXISTS dataDB;")
        cursor.execute("USE dataDB;")




        # Create super table if not exists
        cursor.execute("""

            CREATE STABLE IF NOT EXISTS factoryData (
                ts TIMESTAMP,
                val DOUBLE
            ) TAGS (
                datatag NCHAR(64)
            );
        """)

        # Create child tables based on super table with sanitized tag names
        tags = [
            'factory/oee/performance',
            'factory/equipment/cnc/1/temperature',
            'factory/oee/quality'
        ]

        for tag in tags:
            table_name = tag.replace('/', '_')
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} USING factoryData TAGS ('{tag}');")

        print("Database and tables created or already exist.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating database or tables: {e}")

if __name__ == "__main__":
    create_database_and_table()
