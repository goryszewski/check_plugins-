import argparse
import subprocess
import logging
import redis
import time

logging.basicConfig(level=logging.DEBUG)
parser = argparse.ArgumentParser(description='Test Redis Master-Slave.')
parser.add_argument('--ip', metavar='N', type=str, nargs='+', help='ips')
parser.add_argument('--password', metavar='N', type=str, help='password')
args = parser.parse_args()

REDIS_PASSWORD = args.password
REDIS_IPs = args.ip
PORT_SENTINEL = 26379
PORT_REDIS = 6379


def cli(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)


def parse_string_by_comma(data):
    output = dict()
    while True:
        line = data.stdout.readline()
        if not line:
            break
        string = line.decode().rstrip()
        tmp_dict = string.split(":", 1)
        output[tmp_dict[0]] = tmp_dict[1]
    return output


def getInfoSentinel():
    output = []
    commd_redis = "info sentinel"
    extend = "| grep -v ^#.*$ "
    for ip in REDIS_IPs:
        command = f"redis-cli -h {ip} -p {PORT_SENTINEL} -a {REDIS_PASSWORD} {commd_redis} {extend}"
        data = cli(command)
        tmp = parse_string_by_comma(data)
        tmp['ip'] = ip
        output.append(tmp)
    return output


def getInfoREDIS():
    output = []
    commd_redis = "info replication"
    extend = "| grep -v ^#.*$ "
    for ip in REDIS_IPs:
        command = f"redis-cli -h {ip} -p {PORT_REDIS} -a {REDIS_PASSWORD} {commd_redis} {extend}"
        data = cli(command)
        tmp = parse_string_by_comma(data)
        tmp['ip'] = ip
        output.append(tmp)
    return output


def check_redis_master(array_redis_server_hosts_output_data, master):
    master_redis_server_hosts_output_data = None
    slaves = []
    # szukam mastera
    for idx, redis_server_host_output_data in enumerate(array_redis_server_hosts_output_data):
        if redis_server_host_output_data['ip'] == master:
            master_redis_server_hosts_output_data = array_redis_server_hosts_output_data.pop(idx)

    # sprawdzam czy mam mastera
    if not master_redis_server_hosts_output_data:
        print("nie mam mastera")
        return False

    # sprawdzam slave czy maja wlasciwego mastera
    for slave_redis_server_host_output_data in array_redis_server_hosts_output_data:
        if slave_redis_server_host_output_data['master_host'] != master_redis_server_hosts_output_data['ip']:
            print(f"Ustawiony inny master na hoscie:{slave_redis_server_host_output_data['ip']}")
            return False
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
                print(f"error sync host: {sentinel_host['ip']}")
                output = False
                break;
    return test_sentinel_host_output_data['address']


def check_redis_get_set(master, slaves):
    test = 'bar'
    master = redis.Redis(host=master, port=PORT_REDIS, db=0, password=REDIS_PASSWORD)
    master.set('foo', test)

    for slave in slaves:
        slave_connect_redis = redis.Redis(host=slave, port=PORT_REDIS, db=0, password=REDIS_PASSWORD)
        test_slave_value = slave_connect_redis.get('foo')
        if test_slave_value.decode() != test:
            print('Error get/SET not same')
            return False

    return True


def print_log(data_output, p):
    for item in data_output:
        print(f"{item['ip']} - {item[p]}")


if __name__ == '__main__':
    while True:
        array_sentinel_hosts_output_data = getInfoSentinel()
        array_redis_server_hosts_output_data = getInfoREDIS()

        print_log(array_sentinel_hosts_output_data, 'master0')
        print_log(array_redis_server_hosts_output_data, 'role')
        check_result_master1 = check_sentinel_master(array_sentinel_hosts_output_data)
        check_result_master2 = check_redis_master(array_redis_server_hosts_output_data,
                                                  check_result_master1.split(":")[0])
        if check_result_master1 and check_result_master2:
            print(f"Check #1: Master to: {check_result_master1.split(':')[0]}")

            print(f"Check #2: Slave to:  {check_result_master2}")

            print(f"Check #3: GET/SET:  {check_redis_get_set(check_result_master1.split(':')[0], check_result_master2)}")
        else:
            print("error")
        time.sleep(3)