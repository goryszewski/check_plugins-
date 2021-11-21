import argparse
import subprocess
import redis
import time
import string
import random

parser = argparse.ArgumentParser(description='Test Redis Master-Slave.')
parser.add_argument('--ip', metavar='N', type=str, nargs='+', help='ips')
parser.add_argument('--password', metavar='N', type=str, help='password')
parser.add_argument('--debug', type=bool, default=False)
args = parser.parse_args()

REDIS_PASSWORD = args.password
REDIS_IPs = args.ip
PORT_SENTINEL = 26379
PORT_REDIS = 6379
DEBUG = args.debug
ERROR_COUNT = 0


def cli(cmd):
    output = dict()
    try:
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        exit_code = process.wait()
    except Exception as error:
        raise Exception(f"E001:Problem CLI:{error}")
    output['data'] = commandlineparser(stdout)
    output['error'] = stderr
    output['code'] = exit_code
    return output


def commandlineparser(data):
    try:
        data_parse = data.decode().splitlines()
        output = dict()
        for line in data_parse:
            tmp_dict = line.split(":", 1)
            output[tmp_dict[0]] = tmp_dict[1]
    except Exception as error:
        raise Exception(f"E002:Problem Parser:{error}")
    return output


def getInfoOutput(ip, cmd, port):
    command = f" timeout 2 redis-cli -h {ip} -p {port} -a {REDIS_PASSWORD} info {cmd} | grep -v ^#.*$ "
    output_cli = cli(command)
    output_cli['data']['ip'] = ip
    return output_cli


def check_redis_master(array_redis_server_hosts_output_data, master):
    master_redis_server_hosts_output_data = None
    slaves = []
    # szukam mastera
    for idx, redis_server_host_output_data in enumerate(array_redis_server_hosts_output_data):
        if redis_server_host_output_data['ip'] == master:
            master_redis_server_hosts_output_data = array_redis_server_hosts_output_data.pop(idx)
            break

    # sprawdzam czy mam mastera
    if not master_redis_server_hosts_output_data:
        raise Exception(f"E003:No Master")

    # sprawdzam slave czy maja wlasciwego mastera
    for slave_redis_server_host_output_data in array_redis_server_hosts_output_data:
        if not 'master_host' in slave_redis_server_host_output_data:
            raise Exception(f"E004:Second master:{slave_redis_server_host_output_data['ip']}")

        if slave_redis_server_host_output_data['master_host'] != master_redis_server_hosts_output_data['ip']:
            raise Exception(f"E005:Unknown master:{slave_redis_server_host_output_data['ip']}")
        else:
            slaves.append(slave_redis_server_host_output_data['ip'])

    return slaves


def check_sentinel_master(array_sentinel_hosts_output_data):
    output = True
    test_sentinel_host_output_data = dict()

    # Przygotowanie danych dla wzorca
    sentinel_host_output_data = array_sentinel_hosts_output_data.pop()
    master0_args = sentinel_host_output_data['master0'].split(",")

    # Przygotowanie wzorca "test_sentinel_host_output_data"
    for master0_arg in master0_args:
        master0_arg_split = master0_arg.split("=")
        test_sentinel_host_output_data[master0_arg_split[0]] = master0_arg_split[1]

    # porównanie "array_sentinel_hosts_output_data" ze wzorcem "test_sentinel_host_output_data"
    for sentinel_host in array_sentinel_hosts_output_data:
        sentinel_host_args = sentinel_host['master0'].split(",")
        for sentinel_host_arg in sentinel_host_args:
            tmp = sentinel_host_arg.split("=")
            if test_sentinel_host_output_data[tmp[0]] != tmp[1]:
                raise Exception(f"E006:Not the same parameters:{sentinel_host['ip']}")
    return test_sentinel_host_output_data['address']

def generator_random_string():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))

def check_redis_get_set(master, slaves):
    test = generator_random_string()
    master = redis.Redis(host=master, port=PORT_REDIS, db=0, password=REDIS_PASSWORD)
    master.set('l3_key', test)

    for slave in slaves:
        slave_connect_redis = redis.Redis(host=slave, port=PORT_REDIS, db=0, password=REDIS_PASSWORD)
        test_slave_value = slave_connect_redis.get('l3_key')
        if test_slave_value.decode() != test:
            raise Exception('E007:GET/SET:Not the same value')

    return True


if __name__ == '__main__':
    while True:
        array_error = []
        array_sentinel_hosts_output_data = []
        array_redis_server_hosts_output_data = []

        for ip in REDIS_IPs:
            try:
                info_output_sentinel = getInfoOutput(ip, 'sentinel', PORT_SENTINEL)
                info_output_replication = getInfoOutput(ip, 'replication', PORT_REDIS)
            except Exception as error:
                print(f'Error:{error}:End program')
                exit(2)

            if not info_output_sentinel['code'] == 0:
                array_error.append(info_output_sentinel['error'])
            else:
                array_sentinel_hosts_output_data.append(info_output_sentinel['data'])

            if not info_output_replication['code'] == 0:
                array_error.append(info_output_replication['error'])
            else:
                array_redis_server_hosts_output_data.append(info_output_replication['data'])

        if len(array_error) == len(REDIS_IPs) * 2:
            print(f"All REDIS DOWN - Check Debug Mode")
            exit(2)

        if not len(array_redis_server_hosts_output_data) == len(REDIS_IPs):
            print('Error count output: array_redis_server_hosts_output_data')

        if not len(array_sentinel_hosts_output_data) == len(REDIS_IPs):
            print('Error count output: array_sentinel_hosts_output_data')

        if not len(array_error) == 0:
            print(f"Find error:{len(array_error)} - Check Debug Mode")
            ERROR_COUNT += 1

        if DEBUG:
            for item in array_sentinel_hosts_output_data:
                print(f"{item['ip']} - {item['master0']}")
            for item in array_redis_server_hosts_output_data:
                print(f"{item['ip']} - {item['role']}")

        try:
            master = check_sentinel_master(array_sentinel_hosts_output_data).split(":")[0]
            slaves = check_redis_master(array_redis_server_hosts_output_data, master)
            result_get_set = check_redis_get_set(master, slaves)
            print(f"Master: {master} ; Slaves: {slaves} ; GET/SET: {result_get_set}")
        except Exception as error:
            print(f'Error:{error}')
            ERROR_COUNT += 1
        if DEBUG:
            print(f"Error:COUNT:{ERROR_COUNT}")
            time.sleep(2)
        else:
            break;
