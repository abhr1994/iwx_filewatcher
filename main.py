from configparser import ConfigParser
import redis

from filewatcher import acquire_lock, release_lock
from iwx_functions import get_refresh_token, get_bearer_token, get_domain_id, triggerWorkflow, getWorkflowStatus
import time

bucket_wfid_mappings = {
    "demo_bucket1": "463de23c11382d52e874e751",
    "demo_bucket2": "620280c04bc49776b06d5621",
}
config = ConfigParser()
config.optionxform = str
config.read('config.ini')

hostname = config.get("api", "ip")
port = config.get("api", "port")
protocol = config.get("api", "protocol")
refresh_token_file = config.get("api", "refresh_token_file")


def gcs_obj_monitor(event, context):
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))

    conn = redis.Redis(host='10.18.1.28', port=6379, db=0)
    wf_id = bucket_wfid_mappings.get(event['bucket'], None)
    if wf_id is not None:
        lock_name = "lock:" + wf_id
        identifier = acquire_lock(conn, lock_name)
        if identifier is not False:
            print("Triggering the workflow " + wf_id)
            refresh_token = get_refresh_token(refresh_token_file)
            bearer_token = get_bearer_token({"ip": hostname, "port": port, "protocol": protocol}, refresh_token)
            if bearer_token is not None:
                domain_id = get_domain_id(protocol, hostname, port, wf_id)
                headers = {
                    'Authorization': 'Bearer ' + bearer_token,
                    'Content-Type': 'application/json'
                }
                run_id = triggerWorkflow(protocol, hostname, port, domain_id, wf_id, headers)
                if run_id is not None:
                    timeout = time.time() + 60 * 20  # 20 minutes
                    polling_frequency = 20  # in seconds
                    while True:
                        if time.time() > timeout:
                            break
                        status = getWorkflowStatus(protocol, hostname, port, domain_id, wf_id, run_id, headers)
                        if status in ["success", "failed", "completed", "aborted"]:
                            print(f"Workflow Id: {wf_id} Run Id: {run_id} : Final State: {status}")
                            release_lock(conn, lock_name, identifier)
                            break
                        time.sleep(polling_frequency)
                else:
                    print("Unable to trigger the workflow. Removing the lock")
                    release_lock(conn, lock_name, identifier)
            else:
                print("Unable to trigger the workflow as the script was unable to generate bearer token. Removing the lock")
                release_lock(conn, lock_name, identifier)
        else:
            print(f"There is an existing lock on this workflow {wf_id}. Please try to rerun after {conn.ttl('lock:' + lock_name)} seconds!!")
