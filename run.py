#!/usr/bin/python3
import os
import sys
import io
import argparse
import time
import threading
import boto3
import paramiko
import operator
import yaml
import statistics

SSH_USER = 'ubuntu'
OWNER_ID = '099720109477'
REMOTE_SCRIPT = 'remote/remote_script.sh'

def run_io_setup_on_ec2(instance_type, cnt, image_id, args):
    ec2 = boto3.resource('ec2', region_name=args.region)
    instances = ec2.create_instances(ImageId=image_id, MinCount=1, MaxCount=1, InstanceType=instance_type, KeyName=args.key_name, SubnetId=args.subnet_id, SecurityGroupIds=[args.security_group_id])
    instance = instances[0]
    instance_id = instance.instance_id
    print(f'Launched {instance_type} instance: {instance_id}')

    instance.wait_until_running()

    instance_info = ec2.Instance(instance_id)
    public_ip_address = instance_info.public_ip_address

    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    connected = False
    for i in range(300):
        try:
            ssh.connect(public_ip_address, username=SSH_USER, key_filename=f'{args.key_name}.pem')
            stdin, stdout, stderr = ssh.exec_command('true')
            connected = True
            break
        except paramiko.ssh_exception.NoValidConnectionsError as e:
            time.sleep(10.0)
    if not connected:
        print(f'Timeout to connect instance: {instance_id}')
        instance.terminate()
        return
    print(f'Connected to instance: {instance_id}')
    sftp = ssh.open_sftp()
    sftp.put(REMOTE_SCRIPT, 'remote_script.sh')
    stdin, stdout, stderr = ssh.exec_command('bash -e remote_script.sh > output.log 2>&1')
    exit_status = stdout.channel.recv_exit_status()
    if exit_status != 0:
        print(f'Command failed on the instance: {instance_id}')
    sftp.get('output.log', f'results/{instance_type}-{cnt}.output.log')
    print(f'received file: results/{instance_type}-{cnt}.output.log')

    if exit_status == 0:
        sftp.get(f'/etc/scylla.d/io_properties.yaml', f'results/{instance_type}-{cnt}.io_properties.yaml')
        print(f'received file: results/{instance_type}-{cnt}.io_properties.yaml')
    ssh.close()
    instance.terminate()
    print(f'Terminated {instance_type} instance: {instance_id}')


def print_tsv(args):
    print("instance_type	read_iops	read_bandwidth	write_iops	write_bandwidth")
    sio = io.StringIO()
    for instance_type in args.instance_types:
        read_iops_list = []
        read_bandwidth_list = []
        write_iops_list = []
        write_bandwidth_list = []
        for cnt in range(1, args.num_run_io_setup+1):
            with open(f'results/{instance_type}-{cnt}.io_properties.yaml') as f:
                result = yaml.safe_load(f.read())
            read_iops = result['disks'][0]['read_iops']
            read_iops_list.append(read_iops)
            read_bandwidth = result['disks'][0]['read_bandwidth']
            read_bandwidth_list.append(read_bandwidth)
            write_iops = result['disks'][0]['write_iops']
            write_iops_list.append(write_iops)
            write_bandwidth = result['disks'][0]['write_bandwidth']
            write_bandwidth_list.append(write_bandwidth)
            print(f'{instance_type}.{cnt}	{read_iops}	{read_bandwidth}	{write_iops}	{write_bandwidth}')
        read_iops_avg = int(statistics.mean(read_iops_list))
        read_bandwidth_avg = int(statistics.mean(read_bandwidth_list))
        write_iops_avg = int(statistics.mean(write_iops_list))
        write_bandwidth_avg = int(statistics.mean(write_bandwidth_list))
        sio.write(f'{instance_type}	{read_iops_avg}	{read_bandwidth_avg}	{write_iops_avg}	{write_bandwidth_avg}\n')
    print()
    print('AVG')
    print(sio.getvalue())

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--arch', default='x86_64')
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--instance-types', nargs='+', required=True)
    parser.add_argument('--num-run-io-setup', type=int, default=3)
    parser.add_argument('--key-name', required=True)
    parser.add_argument('--security-group-id', required=True)
    parser.add_argument('--subnet-id', required=True)
    parser.add_argument('--tsv-only', action='store_true')
    args = parser.parse_args()

    if not os.path.exists(f'{args.key_name}.pem'):
        print(f'Please copy {args.key_name}.pem to current directory')
        sys.exit(1)

    if not args.tsv_only:
        os.makedirs('results', exist_ok=True)
        ec2 = boto3.resource('ec2', region_name=args.region)
        deb_arch = 'amd64' if args.arch == 'x86_64' else args.arch
        ami_name = f'ubuntu/images/hvm-ssd/ubuntu-focal-20.04-{deb_arch}*'
        images = ec2.images.filter(Owners=[OWNER_ID], Filters=[{'Name':'name', 'Values':[ami_name]}])
        image_id = sorted(images, key=operator.attrgetter('creation_date'), reverse=True)[0].id
        print(f'Use Ubuntu 20.04 LTS({deb_arch}) AMI: {image_id}')

        threads = []
        for instance_type in args.instance_types:
            for cnt in range(1, args.num_run_io_setup+1):
                thread = threading.Thread(target=run_io_setup_on_ec2, args=(instance_type, cnt, image_id, args,))
                thread.start()
                threads.append(thread)
                time.sleep(1.0)
        for thread in threads:
            thread.join()

    print_tsv(args)
