import redis
import csv
import json
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition
from redis.commands.search.query import Query

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

    # def load_users(self, users_file):
    #     with open(users_file, 'r') as file:
    #         for line in file:
    #             data = json.loads(line.replace('"', '').replace(' ', ',').replace(':', ': '))
    #             key = data.pop('user:id')
    #             self.redis.hset(key, mapping=data)
    #     print("Users loaded successfully!")


    def load_users(self, users_file):
     with open(users_file, 'r') as file:
        for line in file:
            # Split the line into key-value pairs
            tokens = line.strip().split('" "')
            user_key = tokens[0].strip('"')  # Extract the user key (e.g., "user:1")
            user_data = {tokens[i].strip('"'): tokens[i + 1].strip('"') for i in range(1, len(tokens) - 1, 2)}

            # Save user data as a hash in Redis
            self.redis.hset(user_key, mapping=user_data)
    print("Users loaded successfully!")


    # def load_scores(self, scores_file):
    #     with open(scores_file, 'r') as file:
    #         reader = csv.reader(file)
    #         for row in reader:
    #             leaderboard, user_id, score = row
    #             self.redis.zadd(leaderboard, {user_id: int(score)})
    #     print("Scores loaded successfully!")

    # def load_scores(self, scores_file):
    #  with open(scores_file, 'r') as file:
    #     reader = csv.reader(file)
    #     next(reader)  # Skip the header row
    #     for row in reader:
    #         leaderboard, user_id, score = row
    #         self.redis.zadd(leaderboard, {user_id: int(score)})
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
    #         print(f"Adding {user_id} with score {score} to {leaderboard_key}")
    #         self.redis.zadd(leaderboard_key, {user_id.strip(): int(score.strip())})
    # print("Scores loaded successfully!")

    def load_scores(self, scores_file):
     with open(scores_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
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

    def query1(self, usr):
        result = self.redis.hgetall(f"user:{usr}")
        print("Query 1 Result:", result)
        return result

    def query2(self, usr):
        user_data = self.redis.hgetall(f"user:{usr}")
        if user_data:
            result = (user_data.get('longitude'), user_data.get('latitude'))
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
        # Create an index if not exists
        index_name = "idx:users"
        try:
            self.redis.ft(index_name).create_index([
                TextField("gender"),
                TagField("country"),
                NumericField("latitude")
            ], definition=IndexDefinition(prefix=["user:"]))
        except redis.exceptions.ResponseError:
            pass  # Index already exists

        query = Query("@gender: female @country:{China|Russia} @latitude:[40 46]")
        result = self.redis.ft(index_name).search(query)
        print("Query 4 Results:", result.docs)
        return result.docs

    def query5(self):
        result = self.redis.zrevrange("leaderboard:2", 0, 9, withscores=True)
        email_ids = []
        for user_id, score in result:
            email = self.redis.hget(f"user:{user_id}", "email")
            email_ids.append(email)
        print("Query 5 Results:", email_ids)
        return email_ids
    
    def clear_database(self):
        self.redis.flushdb()
        print("Database cleared!")


# Initialize RedisClient
client = RedisClient(host='localhost', port=6379, db=0)

# Connect to Redis
client.connect()
client.clear_database()
# Load data into Redis
client.load_users("users.txt")
client.load_scores("userscores.csv")

# Execute Queries
client.query1(1)
client.query2(2)
client.query3()
client.query4()
client.query5()
