import inspect
import os
import yaml

from vmlauncher.transfer import FileTransferManager
from vmlauncher import build_vm_launcher

from fabric.api import env, settings

from argparse import ArgumentParser

from tempfile import tempdir


def setup_fabric(vm_launcher, ip):
    env.user = vm_launcher.get_user()
    env.hosts = [ip]
    env.key_filename = vm_launcher.get_key_file()
    env.disable_known_hosts = True
    setup_environment()


def setup_environment():
    """
    """
    env.keepalive = 30
    env.timeout = 60
    env.connection_attempts = 5
    env.shell = "/bin/bash -l -c"
    env.use_sudo = True


def read_yaml(yaml_file):
    with open(yaml_file) as in_handle:
        return yaml.load(in_handle)


def path_from_root(name):
    root_path = os.path.join(os.path.dirname(inspect.getfile(inspect.currentframe())), "..")
    file_path = os.path.join(root_path, name)
    return file_path


def parse_settings(name="settings.yaml"):
    return read_yaml(path_from_root(name))


def parse_args():
    parser = ArgumentParser("Spawn a VM and transfer files to it with specified parameters.")
    parser.add_argument('--compressed_file', dest="compressed_files", action="append", default=[])
    parser.add_argument('files', metavar='file', nargs='*',
                        help='files to upload to Galaxy')
    args = parser.parse_args()
    print args
    return args


def transfer_files(options, args):
    # Use just transfer settings in YAML
    options = options['transfer']
    transfer_options = {}
    transfer_options['compress'] = get_boolean_option(options, 'compress', True)
    transfer_options['num_compress_threads'] = int(get_main_options_string(options, 'num_compress_threads', '1'))
    transfer_options['num_transfer_threads'] = int(get_main_options_string(options, 'num_transfer_threads', '1'))
    transfer_options['num_decompress_threads'] = int(get_main_options_string(options, 'num_decompress_threads', '1'))
    transfer_options['chunk_size'] = int(get_main_options_string(options, 'transfer_chunk_size', '0'))
    transfer_options['transfer_retries'] = int(get_main_options_string(options, 'transfer_retries', '3'))
    transfer_options['destination'] = "/mnt/"
    #ransfer_options['transfer_as'] = "galaxy"
    transfer_options['local_temp'] = get_main_options_string(options, 'local_temp_dir', tempdir)
    FileTransferManager(**transfer_options).transfer_files(args.files, args.compressed_files)


def get_boolean_option(options, name, default=False):
    if name not in options:
        return default
    else:
        return options[name]


def get_main_options_string(options, key, default=''):
    value = default
    if key in options:
        value = options[key]
    return value


def setup_vm(options, args, vm_launcher):
    ip = vm_launcher.get_ip()
    setup_fabric(vm_launcher, ip)
    with settings(host_string=ip):
        transfer_files(options, args)


def main():
    options = parse_settings()
    args = parse_args()
    vm_launcher = build_vm_launcher(options)
    vm_launcher.boot_and_connect()
    setup_vm(options, args, vm_launcher)

if __name__ == "__main__":
    main()
