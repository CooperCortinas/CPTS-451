import json
import psycopg2

def cleanStr4SQL(s):
    return s.replace("'","`").replace("\n"," ")

def int2BoolStr (value):
    if value == 0:
        return 'False'
    else:
        return 'True'
dbname = ''

password = ''
def insert2BusinessTable():
    #reading the JSON file
    with open('./yelp_business.JSON', 'r') as f:
        outfile = open('./yelp_business.SQL', 'w')  #uncomment this line if you are writing the INSERT statements to an output file.
        line = f.readline()
        count_line = 0

        try:
            conn = psycopg2.connect(f"dbname={dbname} user='postgres' host='localhost' password={password}")
        except:
            print('Unable to connect to the database!')
        cur = conn.cursor()

        while line:
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
            except:
                print("Insert to Business failed!")
            conn.commit()
            
            outfile.write(sql_str)

            line = f.readline()
            count_line +=1

        cur.close()
        conn.close()

    print(count_line)
    outfile.close()  
    f.close()

def insert2ReviewTable():
    
    with open('./yelp_review.JSON', 'r') as f:
        outfile = open('./yelp_review.SQL', 'w')  
        line = f.readline()
        count_line = 0

        try:
            conn = psycopg2.connect(f"dbname={dbname} user='postgres' host='localhost' password={password}")
        except:
            print('Unable to connect to the database!')
        cur = conn.cursor()

        while line:
            data = json.loads(line)
            
            try:
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
                cur.execute(sql_str, values)
            except Exception as e:
                print(f"Insert to Review failed due to: {e}")
                conn.rollback()  # Ensure to rollback on failure to keep the transaction state clean

            conn.commit()
            
            outfile.write(sql_str)

            line = f.readline()
            count_line +=1

        cur.close()
        conn.close()

    print(count_line)
    outfile.close()  
    f.close()

def insert2UsersTable():

    with open('./yelp_user.JSON', 'r') as f:
        outfile = open('./yelp_user.SQL', 'w')  
        line = f.readline()
        count_line = 0

        try:
            conn = psycopg2.connect(f"dbname={dbname} user='postgres' host='localhost' password={password}")
        except:
            print('Unable to connect to the database!')
        cur = conn.cursor()

        while line:
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
            except:
                print("Insert to Users failed!")
            conn.commit()
            
            outfile.write(sql_str)

            line = f.readline()
            count_line +=1

        cur.close()
        conn.close()

    print(count_line)
    outfile.close()  
    f.close()

def insert2CheckinTable():

    with open('./yelp_checkin.JSON', 'r') as f:
        
        with open('./yelp_checkin.SQL', 'w') as outfile:
            count_line = 0

            try:
                conn = psycopg2.connect(f"dbname={dbname} user='postgres' host='localhost' password={password}")
                
            except:
                print('Unable to connect to the database!')
            cur = conn.cursor()

            for line in f:
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

                        values = (business_id, day, time, count)

                        
                        try:
                            cur.execute(sql_str, values)
                            conn.commit()  # Commit after each successful insert
                        except:
                            print("Insert to Checkin failed!")
                        count_line += 1

            cur.close()
            conn.close()

    print(count_line)

def insert2AttributesTable():

    try:
        conn = psycopg2.connect(f"dbname={dbname} user='postgres' host='localhost' password={password}")
    except:
        print('Unable to connect to the database!')
    cur = conn.cursor()

    with open('./yelp_business.JSON', 'r') as f:
        for line in f:
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
                if value is True: 
                    
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

    cur.close()
    conn.close()

    print("Attributes loading complete.")

def insert2CategoriesTable():
    """Parse categories from businesses and insert into the Categories table."""
    try:
        conn = psycopg2.connect(f"dbname={dbname} user='postgres' host='localhost' password={password}")
    except:
        print('Unable to connect to the database!')
    cur = conn.cursor()

    with open('./yelp_business.JSON', 'r') as f:
        line_count = 0

        for line in f:
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
            line_count += 1

    cur.close()
    conn.close()
    print(f"Processed {line_count} lines. Categories loading complete.")

def updateBusinessCheckins():
    try:
        conn = psycopg2.connect(f"dbname={dbname} user='postgres' host='localhost' password={password}")
        cur = conn.cursor()
    except:
        print('Unable to connect to the database!')
        return

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
    conn.close()
    print("num_checkins updated successfully.")


def updateBusinessAggregateData():
    try:
        conn = psycopg2.connect(f"dbname={dbname} user='postgres' host='localhost' password={password}")
        cur = conn.cursor()
        
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
        print("Review Count and Review Rating Updated Successfully.")
        
    except Exception as e:
        print(f"ERROR: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

updateBusinessAggregateData()



insert2BusinessTable()
insert2UsersTable()
insert2CheckinTable()
insert2ReviewTable()
insert2AttributesTable()
insert2CategoriesTable()
updateBusinessCheckins()
updateBusinessAggregateData()