import json, psycopg2
from pathlib import Path


def cleanStr4SQL(s: str) -> str:
    return s.replace("'","`").replace("\n"," ")

def int2BoolStr(value: str) -> bool:
    if value == 0:
        return 'False'
    else:
        return 'True'

def get_pg_config_str(config_path: Path) -> str:
    with open(config_path, "r") as file:
        data = json.load(file)

    return f"dbname='{data['dbname']}' user='{data['user']}' host='{data['host']}' password='{data['password']}'"

# lots of code duplication for each insert... function but the values tuple makes it difficult
def insert2BusinessTable(conn: psycopg2.extensions.connection, writeout=False):
    cur = conn.cursor()

    with open('./yelp_business.JSON', 'r') as f, open('./yelp_business.SQL', 'w') as outfile:
        line_ct = 0
        for line in f.readlines():
            data = json.loads(line)
            sql_str = "INSERT INTO Business (business_id, name, address, city, state, zipcode, stars, review_rating, review_count, num_checkins) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (
                cleanStr4SQL(data['business_id']),
                cleanStr4SQL(data["name"]),
                cleanStr4SQL(data["address"]),
                cleanStr4SQL(data["city"]),
                cleanStr4SQL(data["state"]),
                cleanStr4SQL(data["postal_code"]),
                data["stars"],
                0.0,
                data["review_count"],
                0
            )

            try:
                cur.execute(sql_str, values)
            except Exception as e:
                print(f"Insert to Business failed due to: {e}")
                conn.rollback() # ensure to rollback on failure to keep the transaction state clean

            conn.commit()

            if writeout:
                outfile.write(sql_str)

            line_ct +=1

    print(f"Processed {line_ct} lines. Business loading complete.")
    cur.close()

def insert2ReviewTable(conn: psycopg2.extensions.connection, writeout=False):
    cur = conn.cursor()

    with open('./yelp_review.JSON', 'r') as f, open('./yelp_review.SQL', 'w') as outfile:
        line_ct = 0
        for line in f.readlines():
            data = json.loads(line)
            sql_str = "INSERT INTO Review (review_id, user_id, business_id, stars, date, text, useful, funny, cool) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (
                cleanStr4SQL(data['review_id']),
                cleanStr4SQL(data['user_id']),
                cleanStr4SQL(data['business_id']),
                data['stars'],
                data['date'],  # Ensure this is in a format that PostgreSQL can accept as a date
                data['text'],
                data['useful'],
                data['funny'],
                data['cool']
            )

            try:
                cur.execute(sql_str, values)
            except Exception as e:
                print(f"Insert to Review failed due to: {e}")
                conn.rollback()

            conn.commit()

            if writeout:
                outfile.write(sql_str)

            line_ct +=1

    print(f"Processed {line_ct} lines. Review loading complete.")
    cur.close()

def insert2UsersTable(conn: psycopg2.extensions.connection, writeout=False):
    cur = conn.cursor()

    with open('./yelp_user.JSON', 'r') as f, open('./yelp_user.SQL', 'w') as outfile:
        line_ct = 0
        for line in f.readlines():
            data = json.loads(line)
            sql_str = "INSERT INTO Users (user_id, avg_stars, name, review_count, yelping_since) VALUES (%s, %s, %s, %s, %s)"
            values = (
                cleanStr4SQL(data['user_id']),
                data['average_stars'],
                cleanStr4SQL(data['name']),
                data['review_count'],
                data['yelping_since']
            )

            try:
                cur.execute(sql_str, values)
            except Exception as e:
                print(f"Insert to Users failed due to: {e}")
                conn.rollback()

            conn.commit()

            if writeout:
                outfile.write(sql_str)

            line_ct +=1

    print(f"Processed {line_ct} lines. Users loading complete.")
    cur.close()

def insert2CheckinTable(conn: psycopg2.extensions.connection, writeout=False):
    cur = conn.cursor()
    with open('./yelp_checkin.JSON', 'r') as f, open('./yelp_checkin.SQL', 'w') as outfile:
        line_ct = 0
        for line in f.readlines():
            data = json.loads(line)
            business_id = cleanStr4SQL(data['business_id'])
            for day, times in data['time'].items():
                for time, count in times.items():
                    sql_str = """
                        INSERT INTO Checkins (business_id, day, time, count)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (business_id, day, time)
                        DO UPDATE SET count = EXCLUDED.count;
                    """
                    values = (
                        business_id,
                        day,
                        time,
                        count
                    )

                    try:
                        cur.execute(sql_str, values)
                    except Exception as e:
                        print(f"Insert to Checkin failed due to: {e}")
                        conn.rollback()

                    conn.commit()

                    if writeout:
                        outfile.write(sql_str)

                    line_ct +=1

    print(f"Processed {line_ct} lines. Checkins loading complete.")
    cur.close()

def insert2AttributesTable(conn: psycopg2.extensions.connection):
    cur = conn.cursor()
    with open('./yelp_business.JSON', 'r') as f:
        for line in f.readlines():
            data = json.loads(line)
            business_id = data["business_id"]
            attributes = data.get("attributes", {})

            # Flatten nested dictionaries and handle non-dict attributes
            flat_attributes = {}
            for key, value in attributes.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        flat_attributes[f"{key}.{sub_key}"] = sub_value
                else:
                    flat_attributes[key] = value

            # Insert attributes into the database, only if attribute_value is True
            for attr_name, value in flat_attributes.items():
                if value:
                    sql_str = """
                    INSERT INTO Attributes (business_id, attr_name, value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (business_id, attr_name)
                    DO UPDATE SET value = EXCLUDED.value;
                    """
                    try:
                        cur.execute(sql_str, (business_id, attr_name, json.dumps(value)))
                        conn.commit()
                    except Exception as e:
                        print(f"ERROR - Insert failed for business_id {business_id}, attribute {attr_name}: {e}")
                        conn.rollback()  # Rollback in case of error

    print("Attributes loading complete.")
    cur.close()

def insert2CategoriesTable(conn: psycopg2.extensions.connection):
    """Parse categories from businesses and insert into the Categories table."""
    cur = conn.cursor()
    with open('./yelp_business.JSON', 'r') as f:
        line_ct = 0
        for line in f.readlines():
            data = json.loads(line)
            business_id = cleanStr4SQL(data['business_id'])

            if 'categories' in data and data['categories']:
                categories = data['categories']  # Assume categories are already a list

                for cat_name in categories:
                    sql_str = """
                        INSERT INTO Categories (business_id, cat_name)
                        VALUES (%s, %s)
                        ON CONFLICT (business_id, cat_name)
                        DO NOTHING;
                    """
                    try:
                        cur.execute(sql_str, (business_id, cat_name))
                        conn.commit()
                    except Exception as e:
                        print(f"ERROR - Insert failed for business_id {business_id}, category {cat_name}: {e}")
                        conn.rollback()
            line_ct += 1

    print(f"Processed {line_ct} lines. Categories loading complete.")
    cur.close()

def updateBusinessCheckins(conn: psycopg2.extensions.connection):
    cur = conn.cursor()
    update_sql = """
        UPDATE Business
        SET num_checkins = COALESCE((
            SELECT SUM(count) FROM Checkins
            WHERE Checkins.business_id = Business.business_id
        ), 0);
    """

    try:
        cur.execute(update_sql)
        conn.commit()
    except Exception as e:
        print(f"ERROR - Update failed for num_checkins: {e}")
        conn.rollback()

    cur.close()
    print("num_checkins updated successfully.")


def updateBusinessAggregateData(conn: psycopg2.extensions.connection, writeout=False):
    cur = conn.cursor()
    try:
        # Step 1: Create a temporary table
        cur.execute("""
            CREATE TEMP TABLE IF NOT EXISTS temp_aggregate AS
            SELECT
                business_id,
                COALESCE(COUNT(*), 0) AS review_count,
                COALESCE(AVG(stars), 0) AS review_rating
            FROM Review
            GROUP BY business_id
        """)

        # Step 2: Update the Business table using the temporary table
        cur.execute("""
            UPDATE Business
            SET
                review_count = temp_aggregate.review_count,
                review_rating = temp_aggregate.review_rating
            FROM temp_aggregate
            WHERE Business.business_id = temp_aggregate.business_id
        """)

        # Optionally: Drop the temporary table if you wish
        cur.execute("DROP TABLE temp_aggregate")

        conn.commit()

    except Exception as e:
        print(f"ERROR: {e}")
        conn.rollback()

    cur.close()
    print("Review Count and Review Rating Updated Successfully.")

def insertFromSQLFile(conn: psycopg2.extensions.connection, sql_file_path: Path):
    cur = conn.cursor()
    with open(sql_file_path, 'r') as file:
        sql_script = file.read()
        try:
            cur.execute(sql_script)
            conn.commit()
            print(f"{sql_file_path} script executed successfully.")
        except Exception as e:
            print(f"Failed to execute SQL script due to: {e}")
            conn.rollback()
        finally:
            cur.close()

if __name__ == "__main__":
    config_file = Path("pg_config.json")
    config = get_pg_config_str(config_file)
    try:
        conn = psycopg2.connect(config)
    except psycopg2.errors.OperationalError:
        print("Unable to connect to the database!")
        exit()

    insertFromSQLFile(conn, Path("Waynes_Task_Force_relations_v2.sql"))
    insert2BusinessTable(conn)
    insert2UsersTable(conn)
    insert2CheckinTable(conn)
    insert2ReviewTable(conn)
    insert2AttributesTable(conn)
    insert2CategoriesTable(conn)
    updateBusinessCheckins(conn)
    updateBusinessAggregateData(conn)
    insertFromSQLFile(conn, Path("data/zipData.sql"))
