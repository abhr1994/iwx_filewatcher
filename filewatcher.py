import argparse
import time
import uuid
import redis


def acquire_lock(conn, lock_name, acquire_timeout=10, lock_timeout=300):
    identifier = str(uuid.uuid4())
    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx("lock:" + lock_name, identifier):
            conn.expire("lock:" + lock_name, lock_timeout)
            return identifier
        elif conn.ttl("lock:" + lock_name) == -1:
            conn.expire("lock:" + lock_name, lock_timeout)
        time.sleep(.001)

    return False


def release_lock(conn, lock_name, identifier):
    pipe = conn.pipeline(True)
    lockname = "lock:" + lock_name
    while True:
        try:
            # break transaction once lock has been changed,
            pipe.watch(lockname)
            if pipe.get(lockname) == identifier:
                pipe.multi()
                pipe.delete(lockname)
                pipe.execute()
                return True

            # execute when identifier not equal
            pipe.unwatch()
            break
        except redis.exceptions.WatchError as e:
            return False

    return False


if __name__ == "__main__":
    _parser = argparse.ArgumentParser()
    _parser.add_argument('-i', '--wf_id', dest='wf_id', required=True, help='')
    parse_input = _parser.parse_args()
    conn = redis.Redis(host='10.18.1.28', port=6379, db=0)
    lock_name = "lock:"+parse_input.wf_id
    identifier = acquire_lock(conn, lock_name)
    if identifier is not False:
        print("Triggering the workflow "+parse_input.wf_id)
        # release_lock(conn, lock_name, identifier)
    else:
        print(f"There is an existing lock on this workflow {parse_input.wf_id}. Please try to rerun after {conn.ttl('lock:' + lock_name)} seconds!!")




