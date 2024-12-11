import csv
from traceback import print_stack
import redis
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition
from redis.commands.search.query import Query

class Redis_Client:
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self.redis = redis.StrictRedis(host=host, port=port, db=db, password=password, decode_responses=True)

    """
    Connect to Redis with "host", "port", "db", "username" and "password".
    """
    def connect(self):
        try:
            self.redis.ping()
            print("Connected to Redis successfully.")
        except Exception as e:
            print(f"Failed to connect to Redis: {e}")
            print_stack()

    """
    Load the users dataset into Redis DB.
    """
    def load_users(self, file):
        result = 0
        try:
            with open(file, 'r') as f:
                for line in f:
                    tokens = line.strip().split('" "')
                    user_key = tokens[0].strip('"')
                    user_data = {tokens[i].strip('"'): tokens[i + 1].strip('"') for i in range(1, len(tokens) - 1, 2)}

                    # Save the user data into Redis Hash
                    self.redis.hset(user_key, mapping=user_data)
                    result += 1
            print(f"Loaded {result} users successfully!")
        except Exception as e:
            print(f"Error loading users: {e}")
            print_stack()
        return result

    """
    Load the scores dataset into Redis DB.
    """
    def load_scores(self, file):
        result = 0
        try:
            with open(file, 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header row
                for row in reader:
                    leaderboard_key, user_id, score = row
                    leaderboard_key = f"leaderboard:{leaderboard_key.strip()}"
                    score = score.strip()

                    if not score.isdigit():  # Ensure score is valid
                        print(f"Skipping invalid score row: {row}")
                        continue

                    self.redis.zadd(leaderboard_key, {user_id.strip(): int(score)})
                    result += 1
            print(f"Loaded {result} scores successfully!")
        except Exception as e:
            print(f"Error loading scores: {e}")
            print_stack()
        return result

    """
    Return all the attributes of the user by user id.
    """
    def query1(self, usr):
        print(f"Executing query 1 for user: {usr}.")
        try:
            result = self.redis.hgetall(f"user:{usr}")
            print("User data:", result)
            return result
        except Exception as e:
            print(f"Error executing query 1: {e}")
            print_stack()

    """
    Return the coordinates (longitude and latitude) of the user by the user id.
    """
    def query2(self, usr):
        print(f"Executing query 2 for user: {usr}.")
        try:
            user_data = self.redis.hgetall(f"user:{usr}")
            coordinates = (user_data.get('longitude'), user_data.get('latitude'))
            print(f"Coordinates for user {usr}: {coordinates}")
            return coordinates
        except Exception as e:
            print(f"Error executing query 2: {e}")
            print_stack()

    """
    Get the keys and last names of the users whose ids do not start with an odd number.
    We will use SCAN to iterate over keys.
    """
    def query3(self):
        print("Executing query 3.")
        result_keys = []
        result_lastnames = []
        cursor = 0
        try:
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
            print(f"Result keys: {result_keys}")
            print(f"Result last names: {result_lastnames}")
            return result_keys, result_lastnames
        except Exception as e:
            print(f"Error executing query 3: {e}")
            print_stack()

    """
    Return the female users in China or Russia with latitude between 40 and 46.
    We first need to create a secondary index in Redis for attributes gender, country, and latitude.
    """
    def query4(self):
        print("Executing query 4.")
        # Define the index for secondary search
        try:
            index_name = "idx:users"
            try:
                self.redis.ft(index_name).create_index([
                    TextField("gender"),
                    TagField("country"),
                    NumericField("latitude")
                ], definition=IndexDefinition(prefix=["user:"]))
            except Exception as e:
                print(f"Error creating index: {e}")

            query = Query("@gender: female @country:{China|Russia} @latitude:[40 46]")
            result = self.redis.ft(index_name).search(query)
            print(f"Query results: {result.docs}")
            return result.docs
        except Exception as e:
            print(f"Error executing query 4: {e}")
            print_stack()

    """
    Get the email ids of the top 10 players in leaderboard:2.
    """
    def query5(self):
        print("Executing query 5.")
        try:
            result = self.redis.zrevrange("leaderboard:2", 0, 9, withscores=True)
            email_ids = []
            for user_id, score in result:
                email = self.redis.hget(f"user:{user_id}", "email")
                email_ids.append(email)
            print(f"Top 10 emails for leaderboard:2: {email_ids}")
            return email_ids
        except Exception as e:
            print(f"Error executing query 5: {e}")
            print_stack()



if __name__ == "__main__":
    rs = Redis_Client()
    rs.connect()
   # Load data from files (uncomment to load from actual files)
    rs.load_users("users.txt")
    rs.load_scores("userscores.csv")
    
  #  Execute queries
    rs.query1(299)
    rs.query2(2836)
    rs.query3()
    rs.query4()
    rs.query5()
