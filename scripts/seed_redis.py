import redis

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

def seed_data():
    # Adding some "Banned" IDs
    banned_ids = ["user_666", "attacker_1", "stolen_card_99"]
    
    # SADD adds members to a set
    r.sadd("banned_users", *banned_ids)
    print(f"✅ Successfully banned {len(banned_ids)} users in Redis.")

if __name__ == "__main__":
    seed_data()