import psycopg
import os
from user_config import user, host, dbname

def drop_tables(user, host, dbname):
    with psycopg.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
        with conn.cursor() as curs:
            drop_tables =\
                """
                DROP TABLE IF EXISTS
                report_type, incident_type, location, incident, employee
                CASCADE;
                """
            curs.execute(drop_tables)
        conn.commit()


def create_tables(user, host, dbname):
    with psycopg.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
        with conn.cursor() as curs:
            report_type =\
                """
                CREATE TABLE report_type
                (
                    report_type_code VARCHAR(2) NOT NULL,
                    report_type_description VARCHAR(100) NOT NULL,
                    PRIMARY KEY (report_type_code)
                )
                """
            incident_type =\
                """
                CREATE TABLE incident_type
                (
                    incident_code INTEGER NOT NULL,
                    incident_category VARCHAR(100) NULL,
                    incident_subcategory VARCHAR(100) NULL,
                    incident_description VARCHAR(200) NULL,
                    PRIMARY KEY (incident_code)
                );
                """
            location =\
                """
                CREATE TABLE location
                (
                    longitude REAL NOT NULL,
                    latitude REAL NOT NULL,
                    supervisor_district REAL NULL,
                    police_district VARCHAR(100) NOT NULL,
                    neighborhood VARCHAR(100) NULL,
                    PRIMARY KEY(longitude, latitude)
                );
                """
            incident =\
                """
                CREATE TABLE incident
                (
                    id INTEGER NOT NULL,
                    incident_datetime TIMESTAMP NOT NULL,
                    report_datetime TIMESTAMP NOT NULL,
                    longitude REAL NULL,
                    latitude REAL NULL,
                    report_type_code VARCHAR(2) NOT NULL,
                    incident_code INTEGER NOT NULL,
                    PRIMARY KEY (id),
                    FOREIGN KEY (report_type_code)
                    REFERENCES  report_type (report_type_code)
                    ON UPDATE CASCADE,
                    FOREIGN KEY (incident_code)
                    REFERENCES  incident_type (incident_code)
                    ON UPDATE CASCADE,
                    FOREIGN KEY (longitude, latitude)
                    REFERENCES  location (longitude, latitude)
                    ON UPDATE CASCADE
                );
                """
            employee =\
                """
                CREATE TABLE employee
                (
                    employee_id INTEGER NOT NULL,
                    name VARCHAR NOT NULL,
                    manager_id INTEGER NULL,
                    PRIMARY KEY (employee_id)
                );
                """
            curs.execute(report_type)
            curs.execute(incident_type)
            curs.execute(location)
            curs.execute(incident)
            curs.execute(employee)
        conn.commit()


def copy_data(user, host, dbname, dir):
    with psycopg.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
        with conn.cursor() as curs:
            report_type =\
                f"""
                COPY report_type
                FROM '{dir}/report_type.csv'
                DELIMITER ','
                CSV HEADER;
                """
            incident_type =\
                f"""
                COPY incident_type
                FROM '{dir}/incident_type.csv'
                DELIMITER ','
                CSV HEADER;
                """
            location =\
                f"""
                COPY location
                FROM '{dir}/location.csv'
                DELIMITER ','
                CSV HEADER;
                """
            incident =\
                f"""
                COPY incident
                FROM '{dir}/incident.csv'
                DELIMITER ','
                CSV HEADER;
                """
            employee =\
                f"""
                COPY employee
                FROM '{dir}/employee.csv'
                DELIMITER ','
                CSV HEADER;
                """
            curs.execute(report_type)
            curs.execute(incident_type)
            curs.execute(location)
            curs.execute(incident)
            curs.execute(employee)
        conn.commit()


def process_query_print(user, host, dbname, query, join=False):
    with psycopg.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
        with conn.cursor() as curs:
            curs.execute(query)
            data = curs.fetchall()
            if not join:
                print("Table - " + query.split()[3])
            print("-------------------------------------------")
            print(data)
            print("\n\n")


def select_sample(user, host, dbname):
    query = "SELECT * FROM report_type LIMIT 5;"
    process_query_print(user, host, dbname, query)
    query = "SELECT * FROM incident_type LIMIT 5;"
    process_query_print(user, host, dbname, query)
    query = "SELECT * FROM location LIMIT 5;"
    process_query_print(user, host, dbname, query)
    query = "SELECT * FROM incident LIMIT 5;"
    process_query_print(user, host, dbname, query)
    query = "SELECT * FROM employee LIMIT 5;"
    process_query_print(user, host, dbname, query)


def sample_joins(user, host, dbname):
    query = "SELECT * FROM incident a FULL JOIN location b on a.longitude = b.longitude AND a.latitude = b.latitude LIMIT 5;"
    print("Example of FULL JOIN")
    process_query_print(user, host, dbname, query, join=True)
    query = "SELECT * FROM incident a JOIN location b on a.longitude = b.longitude AND a.latitude = b.latitude LIMIT 5;"
    print("Example of INNER JOIN")
    process_query_print(user, host, dbname, query, join=True)
    query = """SELECT a.incident_code, a.incident_datetime, b.incident_description
    FROM incident a LEFT JOIN incident_type b on a.incident_code=b.incident_code
    ORDER BY incident_code ASC, incident_datetime ASC LIMIT 5;"""
    print("Example of LEFT JOIN")
    process_query_print(user, host, dbname, query, join=True)
    query = """SELECT b.incident_code, a.incident_datetime, b.incident_description
    FROM incident a RIGHT JOIN incident_type b on a.incident_code=b.incident_code
    WHERE b.incident_code in (1000, 1005)
    ORDER BY incident_code ASC, incident_datetime ASC LIMIT 5;"""
    print("Example of RIGHT JOIN")
    process_query_print(user, host, dbname, query, join=True)
    query = """SELECT neigh.incident_category,neigh.incident_subcategory,neigh.neighborhood,cnt FROM (Select * from (SELECT DISTINCT neighborhood FROM location WHERE neighborhood IS NOT null) as a 
            CROSS JOIN
            (SELECT DISTINCT incident_category, incident_subcategory from incident_type)as b) as neigh
            LEFT JOIN (
            SELECT COUNT(*) as cnt,incident_category, incident_subcategory,neighborhood FROM incident
            INNER JOIN incident_type ON incident.incident_code=incident_type.incident_code
            INNER JOIN location ON location.longitude=incident.longitude AND location.latitude=incident.latitude
            GROUP BY incident_category, incident_subcategory,neighborhood
            ) as Z on Z.incident_category=neigh.incident_category AND Z.incident_subcategory=neigh.incident_subcategory AND Z.neighborhood=neigh.neighborhood
            ORDER BY neigh.neighborhood LIMIT 15;"""
    print("Example of CROSS JOIN")
    process_query_print(user, host, dbname, query, join=True)
    query = """SELECT a.employee_id as manager_id, a.name as manager_name, b.employee_id, b.name as employee_name
    FROM employee a INNER JOIN employee b on a.employee_id=b.manager_id LIMIT 5;"""
    print("Example of SELF JOIN")
    process_query_print(user, host, dbname, query, join=True)


def process():
    drop_tables(user, host, dbname)
    create_tables(user, host, dbname)
    data_dir = os.getcwd() + "/Data"
    copy_data(user, host, dbname, data_dir)
    select_sample(user, host, dbname)
    sample_joins(user, host, dbname)

if __name__ == "__main__":
    process()
