#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Steps for building dist package:
# 1. install docker 
# 2. build docker image
# 3. python3 scripts/build-pkg.py
# 4. packages are placed in dist/

# Steps for debug
# 1. copy & paste the docker run command with small changes:
#    sudo docker run -d (-v parts) /bin/bash -c 'sleep 3600'
# 2. sudo docker ps -a to find the container id
# 3. attach to the container: sudo docker exec -it [id] bash
# 4. cd /var/ossfs/command and manually run the script

import subprocess
from subprocess import Popen, PIPE
import shlex, random, string, os, glob, ntpath, re

docker_images = {
    'alinux3:dev':'ossfs-alinux3:dev',
    'alinux3:test':'ossfs-alinux3:test',
}

os_list = ['alinux3']
working_dir = '/tmp/ossfs'
dest_dir = '/var/ossfs'
ossfs_source_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
dist_dir = os.path.join(ossfs_source_dir, 'dist')

def get_ossfs_version():
    for line in open(os.path.join(ossfs_source_dir, 'configure.ac'), 'r'):
        r = re.match(r"AC_INIT\(ossfs, (.+)\)", line)
        if r:
            return r.group(1)

ossfs_version = get_ossfs_version()

def random_string(length):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(length))

def exec_cmd(cmd):
    print(cmd)
    p = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    out,err = p.communicate()
    if p.returncode != 0:
        print("failed to run: " + cmd)
        raise RuntimeError("Failed to run: %s\n%s" % (cmd,err))

def docker_image_exist(name):
    return True

def docker_pull_image(name):
    pass

def docker_stop(name):
    for action in ('kill', 'rm'):
        cmd = "docker %s %s" % (action, name)
        # print 'run cmd: ', cmd
        p = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
        if p.wait() != 0:
            raise RuntimeError(p.stderr.read())


def docker_run(container_name, img, volume_list, cmd):
    """Run a specified command with the given image"""
    volume_param = ''
    for volume in volume_list:
        volume_param += (' -v ' + volume)
    if volume_param:
        cmd = 'docker run --rm=true --name %s %s %s %s' % (container_name, volume_param, img, cmd)
    else:
        cmd = 'docker run --rm=true --name %s %s %s' % (container_name, img, cmd)

    print(cmd)
    p = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    out,err = p.communicate()
    print("=====DOCKER INFO=====")
    print(out)
    exitcode = p.returncode

    if exitcode != 0:
        # print "failed to run docker: " + cmd
        # print err
        raise RuntimeError(err)

    if exitcode == -9: # happens on timeout
        # print "timeout to run docker: " + cmd
        docker_stop(container_name)

def prepare():
    """
    Prepare the environment, such as get source code from git,
    generate script running in docker container etc.
    """
    exec_cmd('rm -rf %s'%working_dir)
    os.makedirs(os.path.join(working_dir, 'command'))
    os.makedirs(os.path.join(working_dir, 'package'))

    for name in os_list:
        image = docker_images[name+':dev']
        if not docker_image_exist(image):
            docker_pull_image()

def command_build_package(f, install_dir):
    f.write('ldconfig\n')
    f.write('cd %s/source\n' % dest_dir)
    f.write('./autogen.sh\n')
    f.write('./configure\n')
    f.write('make clean\n')
    f.write('make\n')
    f.write('make install DESTDIR=%s\n' % install_dir)
    f.write('cd %s/package\n' % dest_dir)

def command_test_package(f):
    f.write('version=$(ossfs --version | grep -E -o "V[0-9.]+[^ ]+" | cut -d"V" -f2)\n')
    f.write('test "$version" = "%s"\n' % ossfs_version)

def command_build_package_alinux(os_name):
    """
    Generate the build package script running in docker container
    """
    cmd_dir = os.path.join(working_dir, 'command')
    install_dir = '/tmp/ossfs_install'
    f = open(os.path.join(cmd_dir, 'build_package_%s.sh'%os_name), 'w')
    f.write('#!/bin/bash\n')
    command_build_package(f, install_dir)
    f.write('fpm -s dir -t rpm -n ossfs -v %s -C %s -p ossfs_VERSION_%s_ARCH.rpm -d "fuse >= 2.8.4" -d "fuse-libs >= 2.8.4" -d "libcurl >= 7.0" -d "libxml2 >= 2.6" -d "openssl-libs >= 0.9"\n' % (ossfs_version, install_dir, os_name))
    f.close()

def command_test_package_alinux(os_name):
    """
    Generate the test package script running in docker container
    """
    pkg_list = glob.glob(os.path.join(working_dir, 'package/*%s*'%os_name))
    if not pkg_list:
        raise RuntimeError("Can not found %s package! May be build fail?"%os_name)
    pkg = ntpath.basename(pkg_list[0])
    cmd_dir = os.path.join(working_dir, 'command')
    test_dir = os.path.join(dest_dir, 'source/test')
    f = open(os.path.join(cmd_dir, 'test_package_%s.sh'%os_name), 'w')
    f.write('#!/bin/bash\n')
    f.write('yum -y update\n')
    f.write('yum -y localinstall %s/package/%s --nogpgcheck\n' % (dest_dir, pkg))
    command_test_package(f)
    f.close()

def build_docker_image():
    exec_cmd('docker pull alibaba-cloud-linux-3-registry.cn-hangzhou.cr.aliyuncs.com/alinux3/alinux3:latest')
    exec_cmd('docker tag alibaba-cloud-linux-3-registry.cn-hangzhou.cr.aliyuncs.com/alinux3/alinux3:latest ossfs-alinux3:test')
    exec_cmd('docker build -t ossfs-alinux3:dev %s/scripts/docker-file/alinux/3'%ossfs_source_dir)
    pass

def build_package():
    prepare()
    for os_name in os_list:
        volumes = ['%s:%s'%(working_dir, dest_dir), '%s:%s'%(ossfs_source_dir, os.path.join(dest_dir, 'source'))]
        dev_image = docker_images[os_name+':dev']
        test_image = docker_images[os_name+':test']

        if os_name.startswith('alinux'):
            # build package
            print("=============================")
            print("build %s package ..." % os_name)
            print("=============================")
            command_build_package_alinux(os_name)
            container_name = 'ossfs_%s'%random_string(5)
            docker_run(container_name, dev_image, volumes, '/bin/bash %s/command/build_package_%s.sh' % (dest_dir, os_name))

            # test package
            print("============================")
            print("test %s package ..." % os_name)
            print("============================")
            command_test_package_alinux(os_name)
            container_name = 'ossfs_%s'%random_string(5)
            docker_run(container_name, test_image, volumes, '/bin/bash %s/command/test_package_%s.sh' % (dest_dir, os_name))

if __name__ == '__main__':
    build_docker_image()
    build_package()
    subprocess.check_call(['ln', '-sfT', os.path.join(working_dir, 'package'), dist_dir])
