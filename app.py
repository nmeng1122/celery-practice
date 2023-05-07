from datetime import datetime
from flask import Flask, jsonify, request
from celery import Celery
from pymongo import MongoClient
import redis

# Configuration
REDIS_URL = 'redis://localhost:6379'
MONGO_URL = 'mongodb://localhost:27017'
MONGO_DB = 'dut_allocation'
DUT_COLLECTION = 'duts'

# Initialize Flask app
app = Flask(__name__)

# Initialize Celery
# Celery configuration
celery.conf.update({
    'task_routes': {
        'process_request': {
            'queue': 'dut_queue_default',
            'routing_key': 'dut_queue_default'
        }
    }
})

def dut_queue_router(name, args, kwargs, options, task=None, **kwds):
    dut_type = kwargs['dut_type']
    queue_name = f'dut_queue_{dut_type}'
    return {'queue': queue_name, 'routing_key': queue_name}

celery.conf.task_routes = (dut_queue_router, )

# Initialize Redis
cache = redis.Redis.from_url(REDIS_URL)

# Initialize MongoDB
mongo_client = MongoClient(MONGO_URL)
mongo_db = mongo_client[MONGO_DB]

# Initialize redis for store allocated request
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

class DUT:
    def __init__(self, _id, dut_type, available):
        self.id = _id
        self.type = dut_type
        self.available = available

class Request:
    def __init__(self, client_id, dut_type, timestamp):
        self.client_id = client_id
        self.dut_type = dut_type
        self.timestamp = timestamp

    def to_string(self):
        return f'{self.client_id};{self.dut_type};{self.timestamp.isoformat()}'

    @classmethod
    def from_string(cls, request_str):
        client_id, dut_type, timestamp_str = request_str.split(';')
        timestamp = datetime.fromisoformat(timestamp_str)
        return cls(client_id, dut_type, timestamp)

def get_dut_by_type(dut_type):
    dut_doc = mongo_db[DUT_COLLECTION].find_one({'type': dut_type, 'available': True})
    if dut_doc:
        return DUT(dut_doc['_id'], dut_doc['type'], dut_doc['available'])
    return None

def enqueue_request(request):
    cache.rpush(f'requests:{request.dut_type}', request.to_string())

def get_next_request(dut_type):
    request_str = cache.lpop(f'requests:{dut_type}')
    if request_str:
        return Request.from_string(request_str.decode())
    return None

def allocate_dut(dut, request):
    mongo_db[DUT_COLLECTION].update_one({'_id': dut.id}, {'$set': {'available': False}})
    # Add your logic to associate the DUT with the client request
    # Store the allocated DUT information in Redis with a 5-minute expiration
    key = f"allocated_dut_{request.client_id}"
    redis_client.set(key, dut['_id'], ex=300)

@app.route('/request_dut', methods=['POST'])
def request_dut():
    client_id = request.json['client_id']
    dut_type = request.json['dut_type']

    # Check if the request has already been allocated a DUT
    key = f"allocated_dut_{client_id}"
    dut_id = redis_client.get(key)

    if dut_id:
        # Remove the allocated request record from Redis
        redis_client.delete(key)
        return jsonify({'message': 'DUT allocated.', 'dut_id': dut_id.decode('utf-8')})

    # If a DUT has not been allocated, add the request to the queue
    new_request = Request(client_id, dut_type, datetime.utcnow())
    enqueue_request(new_request)

    process_request.apply_async(args=[client_id, dut_type], kwargs={'dut_type': dut_type})

    return jsonify({'message': 'Request queued.'})

@app.route('/status', methods=['GET'])
def get_status():
    # Add your logic to fetch the DUT status from MongoDB and format it as a JSON object
    pass

@celery.task(bind=True)
def process_request(self, client_id, dut_type):
    available_dut = get_dut_by_type(dut_type)

    if available_dut:
        oldest_request = get_next_request(dut_type)

        if oldest_request:
            allocate_dut(available_dut, oldest_request)
            # Notify the client that a DUT has been allocated to them
    else:
        # Retry the task after a short delay
        self.retry(args=[client_id, dut_type], countdown=1)

if __name__ == '__main__':
    app.run(debug=True)