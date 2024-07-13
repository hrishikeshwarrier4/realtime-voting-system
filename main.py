from _datetime import datetime
import random

import psycopg2
import requests



BASE_URL = 'https://randomuser.me/api/?nat=us'
PARTIES = ['Unity Party','Liberty Alliance','Progressive Front','People\'s Choice Party']

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
    response = requests.get(BASE_URL + '&gender='+('male' if candidate_num %2 == 1 else 'female'))
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






if __name__=='__main__':
    try:
        conn = psycopg2.connect(
            dbname="election",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5433")
        cur = conn.cursor()


        create_tables(conn,cur)


        cur.execute("""
            SELECT * FROM candidates
        """)

        candidates = cur.fetchall()
        print(candidates)


        if len(candidates)==0:
            for i in range(4):
                candidates= generates_candidates(i,4)
                print(candidates)
                cur.execute("""
                            INSERT INTO candidates (candidate_id, candidate_name, date_of_birth, gender, party_affiliation, education, experience, biography, campaign_platform, campaign_funds,photo_url)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                    candidates['candidate_id'], candidates['candidate_name'],candidates['date_of_birth'],candidates['gender'], candidates['party_affiliation'],
                    candidates["education"], candidates['experience'],candidates['biography'],
                    candidates['campaign_platform'],candidates['campaign_funds'], candidates['photo_url']))
                conn.commit()






    except Exception as e:
        print(e)
