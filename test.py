import requests
import time

base_url = "http://localhost:5000"

# Helper functions to interact with the API
def allocate_dut(client_id, dut_type):
    response = requests.post(f"{base_url}/allocate_dut", json={"client_id": client_id, "dut_type": dut_type})
    return response.json()

def release_dut(client_id):
    response = requests.post(f"{base_url}/release_dut", json={"client_id": client_id})
    return response.json()

def check_allocated_dut(client_id):
    response = requests.get(f"{base_url}/check_allocated_dut?client_id={client_id}")
    return response.json()

# Test scenario
print("Client 1 allocating DUT of type A")
response = allocate_dut("client_1", "A")
print("Client 1 allocation response:", response)

print("Client 2 allocating DUT of type A")
response = allocate_dut("client_2", "A")
print("Client 2 allocation response:", response)

print("Checking allocated DUT for Client 1:")
response = check_allocated_dut("client_1")
print("Allocated DUT for Client 1:", response)

time.sleep(10)  # Wait for 10 seconds

print("Checking allocated DUT for Client 1 after waiting:")
response = check_allocated_dut("client_1")
print("Allocated DUT for Client 1:", response)

print("Releasing DUT for Client 1")
response = release_dut("client_1")
print("DUT release response for Client 1:", response)

print("Client 3 allocating DUT of type A")
response = allocate_dut("client_3", "A")
print("Client 3 allocation response:", response)

print("Checking allocated DUT for Client 3:")
response = check_allocated_dut("client_3")
print("Allocated DUT for Client 3:", response)