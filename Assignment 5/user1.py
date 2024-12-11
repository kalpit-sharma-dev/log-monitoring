import redis
import csv
import json

class RedisClient:
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self.redis = redis.StrictRedis(host=host, port=port, db=db, password=password, decode_responses=True)
        self.redis.flushdb()

    def connect(self):
        try:
            self.redis.flushdb()
            self.redis.ping()
            print("Connected to Redis successfully!")
        except redis.ConnectionError as e:
            print(f"Failed to connect to Redis: {e}")

    def clear_database(self):
        self.redis.flushdb()
        print("Database cleared!")

    def load_users(self, users_file):
        with open(users_file, 'r') as file:
            for line in file:
                # Parse the data and load into Redis hash
                tokens = line.strip().split('" "')
                user_key = tokens[0].strip('"')  # Extract user key (e.g., "user:1")
                user_data = {tokens[i].strip('"'): tokens[i + 1].strip('"') for i in range(1, len(tokens) - 1, 2)}

                # Save the user data in Redis hash
                self.redis.hset(user_key, mapping=user_data)
        print("Users loaded successfully!")

    # def load_scores(self, scores_file):
    #     with open(scores_file, 'r') as file:
    #         reader = csv.reader(file)
    #         next(reader)  # Skip header row
    #         for row in reader:
    #             leaderboard_key, user_id, score = row
    #             leaderboard_key = f"leaderboard:{leaderboard_key.strip()}"
    #             self.redis.zadd(leaderboard_key, {user_id.strip(): int(score)})
    #     print("Scores loaded successfully!")
    
    # def load_scores(self, scores_file):
    #  with open(scores_file, 'r') as file:
    #     reader = csv.reader(file)
    #     next(reader)  # Skip header row
    #     for row in reader:
    #         leaderboard_key, user_id, score = row

    #         # Ensure the score is not empty and is a valid integer
    #         score = score.strip()  # Remove leading/trailing whitespace
    #         if not score.isdigit():  # Check if score is a valid integer
    #             print(f"Skipping row with invalid score: {row}")
    #             continue

    #         leaderboard_key = f"leaderboard:{leaderboard_key.strip()}"
    #         print(f"Adding to {leaderboard_key}: {user_id.strip()} with score {score}")
    #         self.redis.zadd(leaderboard_key, {user_id.strip(): int(score)})

    # print("Scores loaded successfully!")

    # def load_scores(self, scores_file):
    #  with open(scores_file, 'r') as file:
    #     reader = csv.reader(file)
    #     next(reader)  # Skip header row
    #     for row in reader:
    #         leaderboard_key, user_id, score = row

    #         # Ensure the score is not empty and is a valid integer
    #         score = score.strip()  # Remove leading/trailing whitespace
    #         if not score.isdigit():  # Check if score is a valid integer
    #             print(f"Skipping row with invalid score: {row}")
    #             continue

    #         leaderboard_key = f"leaderboard:{leaderboard_key.strip()}"
    #         print(f"Adding to {leaderboard_key}: {user_id.strip()} with score {score}")
    #         self.redis.zadd(leaderboard_key, {user_id.strip(): int(score)})

    # print("Scores loaded successfully!")

    # def load_scores(self, scores_file):
    #  with open(scores_file, 'r') as file:
    #     reader = csv.reader(file)
    #     next(reader)  # Skip the header row
    #     for row in reader:
    #         if len(row) != 3:
    #             print(f"Skipping malformed row: {row}")
    #             continue

    #         leaderboard_key, user_id, score = row
    #         leaderboard_key = f"leaderboard:{leaderboard_key.strip()}"  # Namespace
    #         score = score.strip()

    #         if not score.isdigit():  # Check if the score is a valid number
    #             print(f"Skipping row with invalid score: {row}")
    #             continue

    #         print(f"Adding {user_id.strip()} with score {score} to {leaderboard_key}")
    #         self.redis.zadd(leaderboard_key, {user_id.strip(): int(score)})

    # print("Scores loaded successfully!")
    def load_scores(self, scores_file):
     with open(scores_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            if len(row) != 3:
                print(f"Skipping malformed row: {row}")
                continue

            leaderboard_key, user_id, score = row
            leaderboard_key = f"leaderboard:{leaderboard_key.strip()}"  # Namespace
            score = score.strip()

            if not score.isdigit():  # Check if the score is a valid number
                print(f"Skipping row with invalid score: {row}")
                continue

            print(f"Adding {user_id.strip()} with score {score} to {leaderboard_key}")
            self.redis.zadd(leaderboard_key, {user_id.strip(): int(score)})

    print("Scores loaded successfully!")

    def query1(self, user_id):
        result = self.redis.hgetall(f"user:{user_id}")
        print("Query 1 Result:", result)
        return result

    def query2(self, user_id):
        user_data = self.redis.hgetall(f"user:{user_id}")
        if user_data:
            result = (user_data.get('longitude'), user_data.get('latitude'))
        else:
            result = None
        print("Query 2 Result:", result)
        return result


    def queryq(self, user_id):
        user_data = self.redis.hgetall(f"user:{user_id}")
        if user_data:
            result = (user_data.get('email'), user_data.get('city'))
        else:
            result = None
        print("Query 2 Result:", result)
        return result

    def query3(self):
        cursor = 0
        result_keys = []
        result_lastnames = []
        while True:
            cursor, keys = self.redis.scan(cursor=cursor, match="user:*")
            for key in keys:
                user_id = key.split(":")[1]
                if int(user_id) % 2 == 0:  # Even IDs only
                    last_name = self.redis.hget(key, "last_name")
                    result_keys.append(key)
                    result_lastnames.append(last_name)
            if cursor == 0:
                break
        print("Query 3 Results:", result_keys, result_lastnames)
        return result_keys, result_lastnames

    def query4(self):
        matching_users = []
        # Iterate through all user keys and filter based on gender, country, and latitude
        for user_key in self.redis.keys('user:*'):  # Iterate through all user keys
            user_data = self.redis.hgetall(user_key)
            if user_data.get("gender") == "female" and user_data.get("country") in ["China", "Russia"]:
                latitude = float(user_data.get("latitude", 0))
                if 40 <= latitude <= 46:
                    matching_users.append(user_data)

        print("Query 4 Results:", matching_users)
        return matching_users
    
    def query5(self):
        result = self.redis.zrevrange("leaderboard:2", 0, 9, withscores=True)
        email_ids = []
        for user_id, score in result:
            email = self.redis.hget(f"user:{user_id}", "email")
            email_ids.append(email)
        print("Query 5 Results:", email_ids)
        return email_ids
    
    # def query5(self):
    #  leaderboard_key = "leaderboard:2"  # Example leaderboard
    #  top_10 = self.redis.zrevrange(leaderboard_key, 0, 9, withscores=True)
    
    # # Print the results for debugging
    #  print(f"Top 10 players for {leaderboard_key}: {top_10}")
    
    #  if not top_10:
    #      print(f"No players found in {leaderboard_key}.")
    
    #  return top_10

    # def query5(self):
    #     leaderboard_key = "leaderboard:2"  # Example leaderboard
    #     top_10 = self.redis.zrevrange(leaderboard_key, 0, 9, withscores=True)
    #     print("Top 10 players:", top_10)
    #     return top_10
 

# Initialize RedisClient
client = RedisClient(host='localhost', port=6379, db=0)

# Connect to Redis
client.connect()

# Clear the database before loading new data
client.clear_database()

# Load data into Redis
client.load_users("users.txt")
client.load_scores("userscores.csv")

# Execute Queries
client.query1(2836)  # Example query for user ID 2836
client.query2(2836)  # Example query for user ID 2836
#client.query3()   # Example query to fetch all even IDs
#client.query4()   # Query for females from China or Russia with latitude between 40 and 46
client.query5()   # Example query to get the top 10 players from leaderboard:2
client.queryq(2836)
