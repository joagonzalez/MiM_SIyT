import requests
import json
from time import sleep

def get_status(url):
    resp = requests.get(url)
    if resp.status_code != 200:
        # This means something went wrong.
        raise ApiError('GET /tasks/ {}'.format(resp.status_code))
    return resp

def describe_task(task_id):
    pass
def add_task(summary, description=""):
    pass
def task_done(task_id):
    pass
def update_task(task_id, summary, description):
    pass

for num in range(20):
    data = get_status('http://0.0.0.0:8080/api/experimental/test')
    print('El estado del api es: ' + str(data))
    sleep(.5)