import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ---------- Configuration ----------
MAP_NUMBER = 5
LOGIN_URL = "https://expo-tech-backend.onrender.com/users/login"
REVIEWS_URL = "https://expo-tech-backend.onrender.com/reviews/project/"
LOGIN_DATA = {
    "username": f"expositor_project-uuid-{MAP_NUMBER}@example.com",
    "password": "senha123"
}
LOGIN_HEADERS = {
    "accept": "application/json",
    "Content-Type": "application/x-www-form-urlencoded"
}

# If True, drop & recreate tables (destructive). Default False -> keep/append.
replace_tables = False

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise ValueError("Set DATABASE_URL in your environment (.env)")

# ---------- 1) Login to API ----------
resp = requests.post(LOGIN_URL, data=LOGIN_DATA, headers=LOGIN_HEADERS)
resp.raise_for_status()
token = resp.json().get("access_token")
if not token:
    raise ValueError("Could not obtain access token from login response")

# ---------- 2) Fetch reviews ----------
headers = {"accept": "application/json", "Authorization": f"Bearer {token}"}
reviews_resp = requests.get(REVIEWS_URL, headers=headers)
reviews_resp.raise_for_status()
reviews = reviews_resp.json()
if not isinstance(reviews, list):
    raise ValueError("API returned unexpected structure (expected list of reviews)")

# ---------- 3) Connect to Postgres ----------
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

# ---------- 4) Create tables (if needed) ----------
if replace_tables:
    cur.execute("DROP TABLE IF EXISTS feedback.scores CASCADE;")
    cur.execute("DROP TABLE IF EXISTS feedback.criteria CASCADE;")
    conn.commit()

# Ensure schema exists
cur.execute("CREATE SCHEMA IF NOT EXISTS feedback;")

# Create criteria table:
cur.execute("""
CREATE TABLE IF NOT EXISTS feedback.criteria (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    weight NUMERIC
);
""")

# Create scores table:
cur.execute("""
CREATE TABLE IF NOT EXISTS feedback.scores (
    id SERIAL PRIMARY KEY,
    review_id TEXT,
    criteria_id INT REFERENCES feedback.criteria(id),
    score INT
);
""")
conn.commit()

# ---------- 5) Upsert criteria and collect criteria_id mapping ----------
# We'll upsert each unique criterion (name + weight) and collect its id.
criteria_map = {}  # name -> id

# Build a set of unique (name, weight) from API data
unique_criteria = {}
for review in reviews:
    for g in review.get("grades", []):
        name = g["name"]
        weight = g.get("weight")
        # If the same name appears with different weights, we'll prefer the latest seen value.
        unique_criteria[name] = weight

# Upsert all unique criteria and store ids
upsert_query = """
INSERT INTO feedback.criteria (name, weight)
VALUES %s
ON CONFLICT (name) DO UPDATE SET weight = EXCLUDED.weight
RETURNING id, name;
"""

values = [(name, unique_criteria[name]) for name in unique_criteria]
if values:
    execute_values(cur, upsert_query, values)
    returned = cur.fetchall()
    # Build name -> id map
    for cid, name in returned:
        criteria_map[name] = cid
    conn.commit()

# In case some criteria were already present (no RETURNING rows for them), ensure we fetch ids for all
if set(criteria_map.keys()) != set(unique_criteria.keys()):
    # fetch missing ids
    missing = tuple(set(unique_criteria.keys()) - set(criteria_map.keys()))
    cur.execute(
        "SELECT id, name FROM feedback.criteria WHERE name IN %s",
        (missing,)
    )
    for cid, name in cur.fetchall():
        criteria_map[name] = cid

# ---------- 6) Insert scores rows ----------
# We'll bulk insert into feedback.scores: (review_id, criteria_id, score)
rows_to_insert = []
for review in reviews:
    review_id = review.get("id")
    for g in review.get("grades", []):
        criteria_name = g["name"]
        score = g.get("score")
        criteria_id = criteria_map.get(criteria_name)
        if criteria_id is None:
            # Fallback: try to query (shouldn't normally happen)
            cur.execute("SELECT id FROM feedback.criteria WHERE name = %s", (criteria_name,))
            r = cur.fetchone()
            if r:
                criteria_id = r[0]
                criteria_map[criteria_name] = criteria_id
            else:
                # create it (very unlikely unless race condition)
                cur.execute("INSERT INTO feedback.criteria (name, weight) VALUES (%s, %s) RETURNING id",
                            (criteria_name, g.get("weight")))
                criteria_id = cur.fetchone()[0]
                conn.commit()
                criteria_map[criteria_name] = criteria_id

        rows_to_insert.append((review_id, criteria_id, score))

if rows_to_insert:
    insert_scores_q = """
    INSERT INTO feedback.scores (review_id, criteria_id, score)
    VALUES %s
    """
    execute_values(cur, insert_scores_q, rows_to_insert)
    conn.commit()

cur.close()
conn.close()

print("✅ Done. Tables in schema 'feedback':")
print(" - feedback.criteria (id serial, name, weight)")
print(" - feedback.scores (id serial, review_id, criteria_id, score)")
