#!/usr/local/bin/python3.9

import os
import subprocess
import sys

def check_python_version(min_major, min_minor):
    major, minor = sys.version_info[:2]
    return (major > min_major) or (major == min_major and minor >= min_minor)

# Check if the current Python version is at least 3.9
if not check_python_version(3, 9):
    try:
        # Attempt to restart the script with Python 3.9
        print("Restarting script with Python 3.9")
        subprocess.run(["python3.9"] + sys.argv)
        sys.exit()
    except Exception as e:
        print(f"Failed to restart script with Python 3.9: {e}")
        sys.exit(1)

def check_and_install_module(module_name):
    global modules_installed
    try:
        globals()[module_name] = __import__(module_name)
    except ImportError:
        modules_installed = True
        print(f"Automatically installing required python module {module_name}")
        subprocess.run(["pip3.9", "install", module_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def send_command(host, port, command):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            s.connect((host, port))
            s.sendall(command.encode())
            data = s.recv(1024)
        return data.decode().strip()
    except Exception as e:
        console.print(f"Error while communicating with {host}:{port} - {e}", style="bold red")
        return None

def getHostName(hostIp):
    device_name = None

    try:
        device_name = socket.gethostbyaddr(hostIp)[0]
    except socket.herror:
        pass

    if device_name is None:
        pulling_event_keys = rdb.hkeys("appsrv:pullingEventLRT")
        for key in pulling_event_keys:
            value_json = rdb.hget("appsrv:pullingEventLRT", key)
            if value_json:
                value_json = value_json.decode('utf-8')
                value_dict = json.loads(value_json)
                if value_dict.get("deviceIp") == hostIp or value_dict.get("relay") == hostIp:
                    device_name = value_dict.get("deviceName")
                    break
    return device_name

def remove_empty_keys(d):
    return {k: v for k, v in d.items() if v != ""}

def getDataHealth(nodes):
    dataNodes = nodes
    results = []
    results_by_shard = {}
    ip_to_shard = {}

    for shard in clickHouseCluster["clickhouse_cluster"]["shards"]:
        shard_name = int(shard["name"].split()[-1]) #shard["name"].replace(" ", "_").lower()
        for node in shard["nodes"]:
            ip_to_shard[node["ip"]] = shard_name

    for node in dataNodes:
        try:
            response = requests.post(f"http://{node}:8123", timeout=1, data="SELECT * FROM system.replicas WHERE database = 'fsiem' FORMAT JSON;")
            if response.status_code == 200:
                replica_data = json.loads(response.text)["data"]
                formatted_data = {
                    'deviceName': getHostName(node),
                    'deviceIp': node,
                    'replicas': [
                        remove_empty_keys({
                            'dbName': r['database'],
                            'dbTable': r['table'],
                            'readonly': r['is_readonly'],
                            'sessionExpired': r['is_session_expired'],
                            'queueSize': r['queue_size'],
                            'insertsInQueue': r['inserts_in_queue'],
                            'mergesInQueue': r['merges_in_queue'],
                            'partMutationsInQueue': r['part_mutations_in_queue'],
                            'lastQueueUpdate': r['last_queue_update'],
                            'absoluteDelay': int(r['absolute_delay']),
                            'replicasOnlinePct': int((r['active_replicas'] / r['total_replicas']) * 100 if r['total_replicas'] and
r['total_replicas'] != 0 else 0),
                            'queueOldestTime': r.get('queue_oldest_time', "") if r.get('queue_oldest_time', "") != "1969-12-31 18:00:00" else "",
                            'insertsOldestTime': r.get('inserts_oldest_time', "") if r.get('inserts_oldest_time', "") != "1969-12-31 18:00:00" else "",
                            'mergesOldestTime': r.get('merges_oldest_time', "") if r.get('merges_oldest_time', "") != "1969-12-31 18:00:00" else "",
                            'partMutationsOldestTime': r.get('part_mutations_oldest_time', "") if r.get('part_mutations_oldest_time', "") != "1969-12-31 18:00:00" else "",
                            'oldestPartToGetTime': r.get('oldest_part_to_get', "") if r.get('oldest_part_to_get', "") != "1969-12-31 18:00:00" else "",
                            'zookeeperException': r['zookeeper_exception'],
                            'lastQueueUpdateException': r['last_queue_update_exception'],
                            'activeReplicas': ','.join(sorted([str(k) for k, v in r.get('replica_is_active', {}).items() if v == 1])) if any(v == 1 for k, v in r.get('replica_is_active', {}).items()) else None
                        })
                        for r in replica_data
                    ]
                }
                shard_name = ip_to_shard.get(node, "Unknown Shard")
                if shard_name not in results_by_shard:
                    results_by_shard[shard_name] = []
                results_by_shard[shard_name].append(formatted_data)

        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching data from {node}: {e}")

    for shard_name, shard_results in results_by_shard.items():
        results.append({
            'shard': shard_name,
            'nodes': shard_results
        })

    return {"DATA_NODE_HEALTH": results}

def getDataReplicationHealth(nodes):
    dataNodes = nodes
    results = []
    results_by_shard = {}
    ip_to_shard = {}

    for shard in clickHouseCluster["clickhouse_cluster"]["shards"]:
        shard_name = shard["name"].replace(" ", "_").lower()
        for node in shard["nodes"]:
            ip_to_shard[node["ip"]] = shard_name

    pulling_event_keys = rdb.hkeys("appsrv:pullingEventLRT")

    for node in dataNodes:
        try:
            response = requests.post(f"http://{node}:8123", data="select count() from fsiem.events_replicated")
            if response.status_code == 200:
                events_replicated = int(response.text.strip())
                shard_name = ip_to_shard.get(node, "Unknown Shard")

                device_name = getHostName(node)
                if shard_name not in results_by_shard:
                    results_by_shard[shard_name] = []

                replication_failures_response = requests.post(f"http://{node}:8123", data="SELECT count() FROM system.replication_queue WHERE last_exception IS NOT NULL")
                if replication_failures_response.status_code == 200:
                    replication_failures = int(replication_failures_response.text.strip())
                else:
                    replication_failures = "N/A"

                # New Query for Metrics
                metrics_query = """SELECT
                  ROUND(sum(bytes_on_disk) / sum(rows), 2) AS avgCompressedEventSize,
                  ROUND(sum(data_uncompressed_bytes) / sum(rows), 2) AS avgUncompressedEventSize,
                  ROUND(sum(data_uncompressed_bytes) / sum(bytes_on_disk), 2) AS avgCompressionRatio,
                  ROUND(sum(bytes_on_disk)/1000000000, 2) AS gbOnDisk,
                  ROUND(((SELECT sum(bytes_on_disk) FROM system.parts WHERE table = 'events_replicated' AND database = 'fsiem' AND
active = 0) / (SELECT sum(bytes_on_disk) FROM system.parts WHERE table = 'events_replicated' AND database = 'fsiem')) * 100, 2) AS
bloatPercentage
                FROM system.parts
                WHERE table = 'events_replicated' AND database = 'fsiem' AND active = 1
                FORMAT JSON;"""

                node_data = {
                    "deviceName": device_name,
                    "deviceIp": node,
                    "events_replicated": events_replicated,
                    "replication_failures": replication_failures,
                }

                metrics_response = requests.post(f"http://{node}:8123", data=metrics_query)
                if metrics_response.status_code == 200:
                    metrics_data = json.loads(metrics_response.text)
                    metrics = metrics_data.get('data', [{}])[0]
                    node_data.update(metrics)
                results_by_shard[shard_name].append(node_data)

#                results_by_shard[shard_name].append({
#                    "deviceName": device_name,
#                    "deviceIp": node,
#                    "events_replicated": events_replicated,
#                    "replication_failures": replication_failures,
#                })

        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching data from {node}: {e}")

    for shard_name, shard_results in results_by_shard.items():
        shard_number = int(re.search(r'\d+', shard_name).group())  # Extract the shard number from the shard name
        results.append({
            "shard": shard_number,
            "nodes": shard_results
        })

    return {
        "DATA_NODE_REPLICATION_HEALTH": results
    }

def getKeeperHealth(nodes):
    keeperNodes = nodes
    results = []
    for keeper_node in keeperNodes:
        isro = send_command(keeper_node, 2181, "isro")
        ruok = send_command(keeper_node, 2181, "ruok")
        stats_raw = send_command(keeper_node, 2181, "srvr")

        if stats_raw is None:
            results.append({
                'deviceName': getHostName(keeper_node),
                'deviceIp': keeper_node,
                'error': 'No response received',
                'isro': isro,
                'ruok': ruok
            })
            continue

        stats_lines = stats_raw.split("\n")
        stats_dict = {}
        has_valid_entries = False

        for line in stats_lines:
            if ": " in line:
                has_valid_entries = True
                key, value = line.split(": ")
                if value.isdigit():
                    value = int(value)
                elif value.lower() == "true":
                    value = True
                elif value.lower() == "false":
                    value = False
                key = key.lower().replace(" ", "_")
                stats_dict[key] = value

        stats_dict.update({"isro": isro, "ruok": ruok})

        if has_valid_entries:
            node_result = {
                "deviceName": getHostName(keeper_node),
                "deviceIp": keeper_node,
                "stats": stats_dict
            }
        else:
            node_result = {
                'deviceName': getHostName(keeper_node),
                'deviceIp': keeper_node,
                'isro': isro,
                'ruok': ruok,
                'error': stats_raw.strip()
            }

        results.append(node_result)

    return {"KEEPER_NODE_HEALTH": results}

def getKeeperStats(nodes):
    keeperNodes = nodes
    results = []
    for keeper_node in keeperNodes:
        clients_raw = send_command(keeper_node, 2181, "cons")
        if clients_raw.strip() == "This instance is not currently serving requests":
            results.append({
                'deviceName': getHostName(keeper_node),
                'deviceIp': keeper_node,
                'error': "This instance is not currently serving requests"
            })
            continue

        clients = []
        for line in clients_raw.strip().split('\n'):
            if line:
                try:
                    match_dict = {
                        'clientName': getHostName(re.search(r'\[::ffff:([^\]]+)', line).group(1)),
                        'clientIpAddr': re.search(r'\[::ffff:([^\]]+)', line).group(1),
                        'port': int(re.search(r':([0-9]+)\(', line).group(1)),
                        'recved': int(re.search(r'recved=(\d+)', line).group(1)),
                        'sent': int(re.search(r'sent=(\d+)', line).group(1)),
                        'sid': re.search(r'sid=(0x[0-9a-f]+)', line).group(1),
                        'lop': re.search(r'lop=([^,]+)', line).group(1),
                        'est': int(re.search(r'est=(\d+)', line).group(1)),
                        'to': int(re.search(r'to=(\d+)', line).group(1)),
                        'lzxid': re.search(r'lzxid=(0x[0-9a-f]+)', line).group(1),
                        'lresp': int(re.search(r'lresp=(\d+)', line).group(1)),
                        'llat': int(re.search(r'llat=(\d+)', line).group(1)),
                        'minlat': int(re.search(r'minlat=(\d+)', line).group(1)),
                        'avglat': int(re.search(r'avglat=(\d+)', line).group(1)),
                        'maxlat': int(re.search(r'maxlat=(\d+)', line).group(1))
                    }
                    clients.append(match_dict)
                except Exception as e:
                    results.append({
                        'deviceName': getHostName(keeper_node),
                        'deviceIp': keeper_node,
                        'error': f"Failed to parse client data: {str(e)}"
                    })
                    break

        else:
            results.append({
                'deviceName': getHostName(keeper_node),
                'deviceIp': keeper_node,
                'clients': clients
            })

    return {"KEEPER_NODE_STATS": results}

def getKeeperParams(nodes):
    keeperNodes = nodes
    results = []
    for keeper_node in keeperNodes:
        conf_raw = send_command(keeper_node, 2181, "conf")

        if conf_raw is None:
            results.append({
                'deviceName': getHostName(keeper_node),
                'deviceIp': keeper_node,
                'error': 'No response received'
            })
            continue

        conf_lines = conf_raw.split("\n")
        conf_dict = {}
        has_valid_entries = False

        for line in conf_lines:
            if "=" in line:
                has_valid_entries = True
                key, value = line.split("=")
                if value.isdigit():
                    value = int(value)
                elif value.lower() == "true":
                    value = True
                elif value.lower() == "false":
                    value = False
                conf_dict[key] = value

        if has_valid_entries:
            node_result = {
                "deviceName": getHostName(keeper_node),
                "deviceIp": keeper_node,
                "conf": conf_dict
            }
        else:
            node_result = {
                'deviceName': getHostName(keeper_node),
                'deviceIp': keeper_node,
                'error': conf_raw.strip()
            }

        results.append(node_result)

    return {"KEEPER_NODE_PARAMS": results}

def clearReplicationFailures(nodes):
    return

def getLocalIpAddr():
    localIps = []
    hostname = socket.gethostname()
    for ip in socket.gethostbyname_ex(hostname)[2]:
        localIps.append(ip)
    return localIps

def amISuper():
    try:
        output = subprocess.check_output(['/opt/phoenix/bin/phLicenseTool', '--showAppServerHost'], stderr=subprocess.DEVNULL).decode('utf-8').strip()
        local_ips = [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2]]
        if output in local_ips:
            return True
        else:
            return False

    except FileNotFoundError:
        return False
    except subprocess.CalledProcessError:
        return False

def uploadEvents(data):
    if isinstance(data, str):
        parsed_data = json.loads(data)
    else:
        parsed_data = data

    payload_type = list(parsed_data.keys())[0] if isinstance(parsed_data, dict) else None

    if payload_type == 'DATA_NODE_HEALTH':
        handle_dataHealth(parsed_data, payload_type)
    elif payload_type == 'DATA_NODE_REPLICATION_HEALTH':
        handle_dataReplicationCount(parsed_data, payload_type)
    elif payload_type == 'KEEPER_NODE_HEALTH':
        handle_keeperHealth(parsed_data, payload_type)
    elif payload_type == 'KEEPER_NODE_PARAMS':
        handle_keeperParams(parsed_data, payload_type)
    elif payload_type == 'KEEPER_NODE_STATS':
        handle_keeperStats(parsed_data, payload_type)
    else:
        print(f"Unknown payload type: {payload_type}")

def handle_dataHealth(data, payload_type):
    for shard_info in data[payload_type]:
        shard = shard_info.get('shard', None)
        for node in shard_info['nodes']:
            deviceName = node['deviceName']
            deviceIp = node['deviceIp']
            handle_replicas(shard, deviceName, deviceIp, node.get('replicas', None), payload_type)

def handle_replicas(shard, deviceName, deviceIp, replicas, payload_type):
    for replica in replicas:
        payload = {}
        payload.update(replica)
        payload.update({
            "deviceName": deviceName,
            "deviceIp": deviceIp,
            "shard": shard
        })
        send_payload('PH_SYSTEM_' + payload_type + '=' + json.dumps(payload))

def handle_dataReplicationCount(data, payload_type):
    for shard_info in data[payload_type]:
        shard = shard_info.get('shard', None)
        for node in shard_info['nodes']:
            deviceName = node['deviceName']
            deviceIp = node['deviceIp']
            events_replicated = node.get('events_replicated', None)
            payload = {
                "shard": shard,
                "deviceName": deviceName,
                "deviceIp": deviceIp,
                "events_replicated": events_replicated
            }
            send_payload('PH_SYSTEM_' + payload_type + '=' + json.dumps(payload))

def handle_keeperHealth(data, payload_type):
    for node in data[payload_type]:
        deviceName = node['deviceName']
        deviceIp = node['deviceIp']
        stats = node['stats']
        payload = {
            "deviceName": deviceName,
            "deviceIp": deviceIp,
            "stats": stats
        }
        send_payload('PH_SYSTEM_' + payload_type + '=' + json.dumps(payload))

def handle_keeperParams(data, payload_type):
    for node in data[payload_type]:
        deviceName = node['deviceName']
        deviceIp = node['deviceIp']
        conf = node['conf']
        payload = {
            "deviceName": deviceName,
            "deviceIp": deviceIp,
            "conf": conf
        }
        send_payload('PH_SYSTEM_' + payload_type + '=' + json.dumps(payload))

def handle_keeperStats(data, payload_type):
    for node in data[payload_type]:
        deviceName = node['deviceName']
        deviceIp = node['deviceIp']
        clients = node['clients']

        for client in clients:
            clientName = client['clientName']
            clientIp = client['clientIpAddr']
            payload = {
                "deviceName": deviceName,
                "deviceIp": deviceIp,
                "clientName": clientName,
                "clientIpAddr": clientIp,
                "clientStats": client
            }
            send_payload('PH_SYSTEM_' + payload_type + '=' + json.dumps(payload))

def send_payload(payload):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(payload.encode('utf-8'), ('localhost', 514))

def getFsmInfo():
    version_file_path="/opt/phoenix/bin/VERSION"
    config_file_path="/opt/phoenix/config/phoenix_config.txt"
    fsm_info = {}
    try:
        with open(version_file_path, "r") as f:
            lines = f.readlines()
        for line in lines:
            if line.startswith("Version:"):
                fsm_info['version'] = line.split(":")[1].strip()

        with open(config_file_path, "r") as f:
            lines = f.readlines()
        for line in lines:
            if line.startswith("MON_ROLE="):
                fsm_info['role'] = line.split("=")[1].strip()

        redis_password_cmd = "/opt/phoenix/bin/phLicenseTool --showRedisPassword"
        completed_process = subprocess.run(redis_password_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)

        if completed_process.returncode == 0:
            fsm_info['redis_password'] = completed_process.stdout.strip()
        else:
            log(f"Unable to obtain Redis password: {completed_process.stderr}")
            sys.exit(1)

    except Exception as e:
        print("This can only be run on a FortiSIEM appliance")
        sys.exit(1)
        return
    return fsm_info

def handle_reload(signum, frame):
    clickHouseCluster, keeperNodes, dataNodes = refresh_variables()

def log(message):
    log_output = f"{message}\n"
    sys.stdout.write(log_output)
    sys.stdout.flush()

def refresh_variables():
    global last_refresh
    last_refresh = datetime.datetime.now()
    log(f"Parameters refreshed")
    fsmInfo = getFsmInfo()
    rdb = redis.Redis(host='localhost', port=6666, password=fsmInfo['redis_password'])
    clickHouseCluster = json.loads(rdb.get('cache:ClickHouse:clickhouseConfig'))
    dataNodes = [node['ip'] for shard in clickHouseCluster['clickhouse_cluster']['shards'] for node in shard['nodes']]
    keeperNodes = [ip for ip in clickHouseCluster['zookeeper_cluster'].values()]
    for job in jobs:
        log(f"Updating hosts for job {job['function'].__name__} to {{{', '.join(job['nodes'])}}}")
    return clickHouseCluster, keeperNodes, dataNodes

def upload_every_n_seconds(n, func_name, func, *args, **kwargs):
    interval_minutes = n // 60
    while True:
        try:
            if amISuper():
                log(f"Executing {func_name}:{interval_minutes} minutes")
                uploadEvents(func(*args, **kwargs))
            else:
                log(f"Not currently the leader, skipping {func_name}:{interval_minutes} minutes")
        except Exception as e:
            log(f"An error occurred in {func_name}: {e}")
        time.sleep(n)

def auto_refresh():
    global last_refresh
    while True:
        current_time = datetime.datetime.now()
        if (current_time - last_refresh).seconds >= 1800:
            log(f"Automatically refreshing variables after 30 minutes.")
            refresh_variables()
        time.sleep(60)

def daemon(dataNodes, keeperNodes):
    global last_refresh
    last_refresh = datetime.datetime.now()
    signal.signal(signal.SIGHUP, handle_reload)
    global jobs

    for job in jobs:
        log(f"Scheduling job {job['function'].__name__}: every {job['interval']} minutes")

    time.sleep(10)
    for job in jobs:
        log(f"{job['function'].__name__} hosts {{{', '.join(job['nodes'])}}}")

    for job in jobs:
        if 'nodes' in job:
            threading.Thread(target=upload_every_n_seconds, args=(job['interval'] * 60, job['function'].__name__, job['function'],
job['nodes'])).start()
        else:
            log(f"The job dictionary for {func_name} does not contain a 'nodes' key.")
    threading.Thread(target=auto_refresh).start()

def install_service():
    if os.geteuid() != 0:
        print("You need to run this script as root.")
        sys.exit(1)

    def spinner():
        spinner_chars = "⣾⣷⣯⣟⡿⢿⣻⣽"
        while not spinner_stop:
            for char in spinner_chars:
                sys.stdout.write(f'\r{char} Installing chStats service')
                sys.stdout.flush()
                time.sleep(0.1)

    spinner_stop = False
    spinner_thread = threading.Thread(target=spinner)
    spinner_thread.start()

    subprocess.run(["cp", "chStats.py", "/opt/phoenix/bin/chStats.py"])

    service_content = """[Unit]
Description=ClickHouse Monitoring for FortiSIEM
After=time-sync.target network-online.target ClickHouseKeeper.service
Wants=time-sync.target ClickHouseKeeper.service

[Service]
ExecStart=/usr/bin/python3 /opt/phoenix/bin/chStats.py --daemon
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
"""

    with open("/etc/systemd/system/chStats.service", "w") as f:
        f.write(service_content)

    subprocess.run(["systemctl", "daemon-reload"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["systemctl", "enable", "chStats"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["systemctl", "restart", "chStats"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    time.sleep(5)
    result = subprocess.run(["systemctl", "--no-pager", "status", "chStats"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    spinner_stop = True
    spinner_thread.join()

    print(f'\r{" " * 35}')
    print(result.stdout)

def main():
    fsmInfo = getFsmInfo()
    modules_to_check = ["rich", "redis", "warnings", "argparse", "json", "requests", "re", "socket", "rich", "xml.etree.ElementTree", "logging", "packaging", "time", "datetime", "signal", "threading"]

    modules_installed = False
    for module in modules_to_check:
        check_and_install_module(module)

    if modules_installed:
        print("Modules installed. Relaunching script.")
        os.execv(sys.executable, ['python3'] + sys.argv)

    from rich.syntax import Syntax
    from rich.console import Console
    import xml.etree.ElementTree as ET
    from packaging import version

    global rdb, clickHouseCluster, console, jobs

    indent = 2
    theme = 'github-dark'

    rdb = redis.Redis(host='localhost', port=6666, password=fsmInfo['redis_password'])
    supportedRoles = ['phMonitorSupervisor', 'phMonitorWorker']
    if fsmInfo['role'] not in supportedRoles:
        print(f"This tool only supports being run on a Supervisor or Worker")
        sys.exit(1)
    if version.parse(fsmInfo['version']) >= version.parse('7.0.0'):
        clickHouseCluster = json.loads(rdb.get('cache:ClickHouse:clickhouseConfig'))
        dataNodes = [node['ip'] for shard in clickHouseCluster['clickhouse_cluster']['shards'] for node in shard['nodes']]
        keeperNodes = [ip for ip in clickHouseCluster['zookeeper_cluster'].values()]
    else:
        print(f"This tool only supports FortiSIEM version 7.0 and higher")
        sys.exit(1)
    console = Console()

    jobs = [
        {'interval': 3, 'function': getDataHealth, 'nodes': dataNodes},
        {'interval': 3, 'function': getDataReplicationHealth, 'nodes': dataNodes},
        {'interval': 3, 'function': getKeeperHealth, 'nodes': keeperNodes},
        {'interval': 5, 'function': getKeeperStats, 'nodes': keeperNodes},
        {'interval': 30, 'function': getKeeperParams, 'nodes': keeperNodes}
    ]
    tasks = [
#        {'function': clearReplicationFailures, 'nodes': dataNodes}
    ]

    jobNames = [job['function'].__name__ for job in jobs]
    taskNames = [task['function'].__name__ for task in tasks]

    possible_themes = ['github-dark', 'monokai', 'dracula', 'nord', 'one-dark', 'zenburn']

    parser = argparse.ArgumentParser(description="A tool for extracting FortiSIEM ClickHouse information.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-m", dest='mode', type=str, help="Mode of operation.", choices=jobNames)
    parser.add_argument('-U', dest='upload', action='store_true', help='Upload events to Supervisor instead of displaying output')
    parser.add_argument("--theme", dest="theme", type=str, default="github-dark", help=f"JSON output color theme.", choices=possible_themes)
    parser.add_argument("-i", dest='indent', type=int, default=2, help="JSON output indentation level.")
    parser.add_argument("--install", action="store_true", help="Install as a systemd service")
    parser.add_argument("--daemon", action="store_true", help="Run as a daemon")

    args = parser.parse_args()

    if args.install:
        install_service()
        return
    elif args.daemon:
        daemon(dataNodes, keeperNodes)
        return

    indent = args.indent
    theme = args.theme

    input_mode = args.mode.lower() if args.mode else None
    selected_mode = next((modeName for modeName in jobNames if modeName.lower() == input_mode), None)

    if args.mode and selected_mode is None:
        parser.print_help()
        print(f"\n\nERROR: {args.mode} is not a valid selection\n")
        sys.exit(1)

    if selected_mode:
        selected_mode_index = jobNames.index(selected_mode)
        result = jobs[selected_mode_index]['function'](jobs[selected_mode_index]['nodes'])
    else:
        while True:
            num_choices = len(jobs) + len(tasks)
            print(f"\nPlease select an option [1-{num_choices}]\n")
            print("\n[Reporting Jobs]")
            for idx, job in enumerate(jobs, 1):
                print(f"{idx}. {job['function'].__name__}")
            if tasks:
                print("\n[Remediation Jobs]")
                for idx, task in enumerate(tasks, len(jobs)+1):
                    print(f"{idx}. {task['function'].__name__}")
            selection = input("\nEnter your choice: ")
            selected_mode_index = int(selection) - 1 if selection.isdigit() and 0 < int(selection) <= num_choices else None
            if selected_mode_index is not None:
                break
            else:
                print(f"\nERROR: \"{selection}\" is not a valid entry, please try again.")

    if selected_mode_index is not None:
        if selected_mode_index < len(jobs):
            result = jobs[selected_mode_index]['function'](jobs[selected_mode_index]['nodes'])
            if args.upload:
                uploadEvents(result)
            else:
                syntax = Syntax(json.dumps(result, indent=indent), "json", theme=theme, line_numbers=False)
                console.print(syntax)
        elif selected_mode_index >= len(jobs) and selected_mode_index < len(jobs) + len(tasks):
            task_index = selected_mode_index - len(jobs)
            result = tasks[task_index]['function'](tasks[task_index]['nodes'])
            syntax = Syntax(json.dumps(result, indent=indent), "json", theme=theme, line_numbers=False)
            console.print(syntax)
        else:
            print("Invalid selection. Please try again.")

if __name__ == "__main__":
    main()