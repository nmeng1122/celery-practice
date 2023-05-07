from pymongo import MongoClient
import subprocess

def start_mongodb():
    try:
        subprocess.Popen(["mongod"])
        print("MongoDB server started.")
    except FileNotFoundError:
        print("MongoDB server could not be started. Please ensure 'mongod' is in the system PATH.")

def main():
    start_mongodb()
    mongo_client = MongoClient('localhost', 27017)
    db = mongo_client['dut_allocation']

    duts = [
        {"_id": "1", "type": "A"},
        {"_id": "2", "type": "A"},
        {"_id": "3", "type": "B"},
        {"_id": "4", "type": "B"}
    ]

    db.duts.insert_many(duts)

if __name__ == "__main__":
    main()