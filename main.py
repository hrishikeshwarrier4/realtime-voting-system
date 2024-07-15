import json
from datetime import datetime
import random

import psycopg2
import requests
from confluent_kafka import SerializingProducer, KafkaException
from confluent_kafka.serialization import StringSerializer
from uuid import uuid4

BASE_URL = 'https://randomuser.me/api/?nat=us'
PARTIES = ['Unity Party', 'Liberty Alliance', 'Progressive Front', 'People\'s Choice Party']

random.seed(43)

# Sample data for dynamic fields
EDUCATION_OPTIONS = [
    "Bachelor's in Political Science",
    "Master's in Public Administration",
    "PhD in Economics",
    "Law Degree",
    "Bachelor's in Sociology"
]

EXPERIENCE_OPTIONS = [
    "5 years as a City Council Member",
    "3 years as a State Representative",
    "10 years in Non-Profit Management",
    "8 years in Business Leadership",
    "5 years as a Community Organizer"
]

BIOGRAPHY_OPTIONS = [
    "A dedicated public servant with a passion for community development.",
    "An experienced leader with a background in law and public policy.",
    "Committed to improving education and healthcare for all citizens.",
    "A strong advocate for economic reform and environmental sustainability.",
    "Focused on creating inclusive and transparent government policies."
]

CAMPAIGN_PLATFORM_OPTIONS = [
    "Affordable healthcare for all.",
    "Improving education and job opportunities.",
    "Environmental protection and renewable energy.",
    "Criminal justice reform.",
    "Strengthening local economies and small businesses."
]

POLLING_STATIONS = [
    "Community Center Polling Station",
    "High School Gym Polling Station",
    "Library Polling Station",
    "Town Hall Polling Station",
    "Fire Station Polling Station"
]

def create_tables(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            date_of_birth DATE,
            gender VARCHAR(255),
            party_affiliation VARCHAR(255),
            education TEXT,
            experience TEXT,
            biography TEXT,
            campaign_platform TEXT,
            campaign_funds DECIMAL(15,2),
            photo_url TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER,
            polling_station VARCHAR(255)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)
    conn.commit()

def generates_candidates(candidate_num, total_parties):
    response = requests.get(BASE_URL + '&gender=' + ('male' if candidate_num % 2 == 1 else 'female'))
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "candidate_id": user_data['login']['uuid'],
            "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": datetime.strptime(user_data['dob']['date'], "%Y-%m-%dT%H:%M:%S.%fZ").date(),
            "gender": user_data['gender'],
            "party_affiliation": PARTIES[candidate_num % total_parties],
            "education": random.choice(EDUCATION_OPTIONS),
            "experience": random.choice(EXPERIENCE_OPTIONS),
            "biography": random.choice(BIOGRAPHY_OPTIONS),
            "campaign_platform": random.choice(CAMPAIGN_PLATFORM_OPTIONS),
            "campaign_funds": round(random.uniform(50000, 1000000), 2),
            "photo_url": user_data['picture']['large']
        }
    else:
        return "Error fetching data"

def insert_candidates(conn, cur, candidates):
    try:
        cur.execute("""
            INSERT INTO candidates (candidate_id, candidate_name, date_of_birth, gender, party_affiliation, education, experience, biography, campaign_platform, campaign_funds,photo_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            candidates['candidate_id'], candidates['candidate_name'], candidates['date_of_birth'], candidates['gender'],
            candidates['party_affiliation'],
            candidates["education"], candidates['experience'], candidates['biography'],
            candidates['campaign_platform'], candidates['campaign_funds'], candidates['photo_url']))
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error inserting candidate: {e}")
        conn.rollback()

def generates_voter():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age'],
            "polling_station": random.choice(POLLING_STATIONS)
        }
    else:
        return "Error fetching data"

def insert_voters(conn, cur, voter):
    while True:
        try:
            cur.execute("""
                INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, cell_number, picture, registered_age, polling_station)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                voter['nationality'], voter['registration_number'], voter['address']['street'],
                voter['address']['city'], voter['address']['state'], voter['address']['country'],
                voter['address']['postcode'], voter['email'], voter['phone_number'],
                voter['cell_number'], voter['picture'], voter['registered_age'], voter['polling_station']
            ))
            conn.commit()
            break
        except psycopg2.errors.UniqueViolation:
            voter["voter_id"] = str(uuid4())
        except psycopg2.Error as e:
            print(f"Error inserting voter: {e}")
            conn.rollback()
            break

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}[{msg.partition()}]")

if __name__ == '__main__':
    producer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': StringSerializer('utf_8')
    }
    producer = SerializingProducer(producer_conf)

    try:
        conn = psycopg2.connect(
            dbname="election",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5433"
        )
        cur = conn.cursor()

        create_tables(conn, cur)

        cur.execute("""
            SELECT * FROM candidates
        """)

        candidates = cur.fetchall()

        if len(candidates) == 0:
            for i in range(4):
                candidate = generates_candidates(i, 4)
                insert_candidates(conn, cur, candidate)
                print(candidate)

        for i in range(4000):
            voter = generates_voter()
            if voter != "Error fetching data":
                insert_voters(conn, cur, voter)
                print(voter)

                producer.produce(
                    topic="voters_topic",
                    key=voter['voter_id'],
                    value=json.dumps(voter),
                    on_delivery=delivery_report
                )
                producer.poll(0)

        producer.flush()

    except Exception as e:
        print(e)
    finally:
        cur.close()
        conn.close()
