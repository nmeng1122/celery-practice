import json
from datetime import datetime

import redis
from celery import Celery
from flask import Flask, jsonify, request
from pymongo import MongoClient

# Create Flask app and configure Celery
app = Flask(__name__)
# app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
# app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

app.config.from_object('celery_config')

def make_celery(app):
    celery = Celery(app.import_name, broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    celery.Task = ContextTask
    return celery

celery = make_celery(app)

# Connect to MongoDB and Redis
mongo_client = MongoClient('localhost', 27017)
db = mongo_client['dut_management']
dut_collection = db['duts']
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# DUT and Request classes
class DUT:
    def __init__(self, dut_id, dut_type, available=True, allocation_timestamp=None):
        self.dut_id = dut_id
        self.dut_type = dut_type
        self.available = available
        self.allocation_timestamp = allocation_timestamp

class Request:
    def __init__(self, client_id, dut_type, timestamp):
        self.client_id = client_id
        self.dut_type = dut_type
        self.timestamp = timestamp

# Utility functions for serialization and deserialization
def serialize_dut(dut):
    return json.dumps(dut.__dict__)

def deserialize_dut(dut_str):
    dut_dict = json.loads(dut_str)
    return DUT(**dut_dict)

def serialize_request(request):
    return json.dumps(request.__dict__)

def deserialize_request(request_str):
    request_dict = json.loads(request_str)
    return Request(**request_dict)

# Utility functions for DUT management
def check_allocated_dut(client_id):
    allocated_dut_str = redis_client.get(f'allocated_dut_{client_id}')
    if allocated_dut_str:
        return deserialize_dut(allocated_dut_str.decode())

    return None

def find_available_dut(dut_type):
    return dut_collection.find_one({"available": True, "dut_type": dut_type})

def allocate_dut_to_client(client_id, dut):
    current_time = datetime.utcnow().timestamp
    serialized_dut = serialize_dut(DUT(dut_id=dut["dut_id"], dut_type=dut["dut_type"], available=False, allocation_timestamp=current_time))
    redis_client.set(f'allocated_dut_{client_id}', serialized_dut)
    dut_collection.update_one({"_id": dut["_id"]}, {"$set": {"available": False}})

def is_request_already_queued(dut_type, client_id):
    queue_name = f'dut_queue_{dut_type}'
    for _, item in redis_client.hgetall(queue_name).items():
        request_obj = deserialize_request(item.decode())
        if request_obj.client_id == client_id:
            return True
    return False

def release_dut(client_id):
    allocated_dut = check_allocated_dut(client_id)
    if allocated_dut:
        dut_collection.update_one({"dut_id": allocated_dut.dut_id}, {"$set": {"available": True}})
        redis_client.delete(f'allocated_dut_{client_id}')
        return True

    return False

def add_request_to_queue(dut_type, request_obj):
    queue_name = f'dut_queue_{dut_type}'
    redis_client.hset(queue_name, request_obj.client_id, serialize_request(request_obj))

def remove_request_from_queue(dut_type, client_id):
    queue_name = f'dut_queue_{dut_type}'
    redis_client.hdel(queue_name, client_id)

# Route for allocating a DUT
@app.route('/allocate_dut', methods=['POST'])
def allocate_dut():
    data = request.get_json()
    dut_type = data['dut_type']
    client_id = data['client_id']

    allocated_dut = check_allocated_dut(client_id)
    if allocated_dut:
        return jsonify({"dut_id": allocated_dut.dut_id, "allocated_to": client_id})

    if not is_request_already_queued(dut_type, client_id):
        request_obj = Request(client_id=client_id, dut_type=dut_type, timestamp=datetime.utcnow())
        add_request_to_queue(dut_type, request_obj)
        process_dut_queues.apply_async(args=[dut_type], queue=f'dut_queue_{dut_type}')

    return jsonify({"status": "queued"})

# Route for releasing a DUT
@app.route('/release_dut', methods=['POST'])
def release_dut():
    data = request.get_json()
    client_id = data['client_id']

    if release_dut(client_id):
        return jsonify({"status": "released"})

    return jsonify({"status": "no_dut_allocated"})

# Celery task for processing DUT queues
@celery.task(name='process_dut_queues')
def process_dut_queues(dut_type):
    while True:
        # Release unclaimed DUTs after 5 minutes
        timeout = 5 * 60  # 5 minutes in seconds
        current_time = datetime.utcnow().timestamp

        for key in redis_client.scan_iter(match=f'allocated_dut_*'):
            allocated_dut = deserialize_dut(redis_client.get(key).decode())
            if allocated_dut.allocation_timestamp < current_time - timeout:
                release_dut(allocated_dut.client_id)

        available_dut = find_available_dut(dut_type)
        if not available_dut:
            break

        queue_name = f'dut_queue_{dut_type}'
        queued_requests = [(key.decode(), deserialize_request(val.decode())) for key, val in redis_client.hgetall(queue_name).items()]
        if not queued_requests:
            break

        queued_requests.sort(key=lambda x: x[1].timestamp)
        client_id, request_obj = queued_requests.pop(0)
        allocate_dut_to_client(client_id, available_dut)
        remove_request_from_queue(dut_type, client_id)
# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)