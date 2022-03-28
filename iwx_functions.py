import json
import traceback
import requests


def get_refresh_token_from_config(refresh_token_file, key):
    try:
        return [str(v).strip() for k, v in (l.split('=') for l in open(refresh_token_file) if l.startswith(key))][0]
    except Exception as e:
        print(e)
        traceback.print_exc()


def get_refresh_token(refresh_token_file):
    print('refresh_token is not provided in the parameter, trying to get from conf.properties...')
    token = get_refresh_token_from_config(refresh_token_file, 'iw_default_refresh_token')
    if not token:
        print('iw_default_refresh_token is not found in conf.properties file please provide refresh_token in '
              'parameter.')
        return None
    else:
        return token


def get_bearer_token(client_config, token):
    headers = {'Authorization': 'Basic ' + token, 'Content-Type': 'application/json'}
    url = '{protocol}://{ip}:{port}/v3/security/token/access'.format(ip=client_config['ip'], port=client_config['port'],
                                                                         protocol=client_config['protocol'])
    response = requests.request("GET", url, headers=headers,
                                verify=False)
    if response is not None:
        delegation_token = response.json().get("result", {}).get("authentication_token")
    else:
        delegation_token = None
        print('Something went wrong, unable to get bearer token')

    return delegation_token


def get_domain_id(protocol, host, port, workflow_id):
    url = '{protocol}://{host}:{port}/v3/admin/entity'.format(
        protocol=protocol,
        host=host,
        port=port)
    body = {"entity_id": workflow_id, "entity_type": "workflow"}
    payload = json.dumps(body)
    headers = {
        'Content-Type': 'application/json'
    }
    response_obj = requests.request("GET", url, headers=headers, data=payload)
    response = response_obj.json()
    domain_id = response['result']['entity_id']
    return domain_id


def triggerWorkflow(protocol, host, port, domain_id, wfid, headers):
    url = '{protocol}://{host}:{port}/v3/domains/{domain_id}/workflows/{workflow_id}/start'.format(
        protocol=protocol,
        host=host,
        port=port,
        workflow_id=wfid,
        domain_id=domain_id)
    try:
        response_obj = requests.request("POST", url, headers=headers)
        if response_obj.status_code == 200:
            response = response_obj.json()
            runid = response['result']['$value']
            return str(runid)
    except requests.exceptions.ConnectionError as e:
        print("Failed to trigger Workflow")
        return None


def getWorkflowStatus(protocol, host, port, domain_id, wfid, runid, headers):
    url = '{protocol}://{host}:{port}/v3/domains/{domain_id}/workflows/{workflow_id}/runs/{workflow_run_id}'.format(
        protocol=protocol,
        host=host,
        port=port,
        workflow_id=wfid,
        domain_id=domain_id,
        workflow_run_id=runid)
    response_obj = requests.request("GET", url, headers=headers)
    if response_obj.status_code == 200:
        response = response_obj.json()
        try:
            wf_end_state = response['result']['workflow_status']['state']  # end result
            return wf_end_state
        except Exception as e:
            return "failed"

