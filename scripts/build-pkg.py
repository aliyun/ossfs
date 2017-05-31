#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Steps for building dist package:
# 1. sudo yum install alidocker -b test
# 2. sudo docker login reg.docker.alibaba-inc.com # obtain "HUB TOKEN" from http://docker.alibaba-inc.com/
# 3. sudo python scripts/build-pkg.py
# 4. packages are placed in dist/

# Steps for debug
# 1. copy & paste the docker run command with small changes:
#    sudo docker run -d (-v parts) /bin/bash -c 'sleep 3600'
# 2. sudo docker ps -a to find the container id
# 3. attach to the container: sudo docker exec -it [id] bash
# 4. cd /var/ossfs/command and manually run the script

import subprocess
from subprocess import Popen, PIPE
import shlex, random, string, os, shutil, glob, ntpath, re

docker_images = {
    'centos6.5:dev':'reg.docker.alibaba-inc.com/ossfs/ossfs-centos6.5:dev',
    'centos6.5:test':'reg.docker.alibaba-inc.com/ossfs/ossfs-centos6.5:test',
    'centos7.0:dev':'reg.docker.alibaba-inc.com/ossfs/ossfs-centos7.0:dev',
    'centos7.0:test':'reg.docker.alibaba-inc.com/ossfs/ossfs-centos7.0:test',
    'ubuntu14.04:dev':'reg.docker.alibaba-inc.com/ossfs/ossfs-ubuntu14.04:dev',
    'ubuntu14.04:test':'reg.docker.alibaba-inc.com/ossfs/ossfs-ubuntu14.04:test',
    'ubuntu16.04:dev':'reg.docker.alibaba-inc.com/ossfs/ossfs-ubuntu16.04:dev',
    'ubuntu16.04:test':'reg.docker.alibaba-inc.com/ossfs/ossfs-ubuntu16.04:test',
}

os_list = ['centos6.5', 'centos7.0', 'ubuntu14.04', 'ubuntu16.04']
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
    return ''.join(random.choice(string.lowercase) for i in range(length))

def exec_cmd(cmd):
    print cmd
    p = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    out,err = p.communicate()
    if p.returncode != 0:
        print "failed to run: " + cmd
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


def docker_run(conatiner_name, img, volume_list, cmd):
    """Run a specified command with the given image"""
    volume_param = ''
    for volume in volume_list:
        volume_param += (' -v ' + volume)
    if volume_param:
        cmd = 'docker run --rm=true --name %s %s %s %s' % (conatiner_name, volume_param, img, cmd)
    else:
        cmd = 'docker run --rm=true --name %s %s %s' % (conatiner_name, img, cmd)

    print cmd
    p = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
    out,err = p.communicate()
    print "=====DOCKER INFO====="
    print out
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
    f.write('version=$(ossfs --version | grep -E -o "V[0-9.]+" | cut -d"V" -f2)\n')
    f.write('test "$version" = "%s"\n' % ossfs_version)

def command_build_package_centos65():
    """
    Generate the build package script running in docker container
    """
    cmd_dir = os.path.join(working_dir, 'command')
    install_dir = '/root/ossfs_install'
    f = open(os.path.join(cmd_dir, 'build_package_centos65.sh'), 'w')
    f.write('#!/bin/bash\n')
    f.write('export PKG_CONFIG_PATH=/usr/lib/pkgconfig:/usr/lib64/pkgconfig/\n')
    command_build_package(f, install_dir)
    f.write('fpm -s dir -t rpm -n ossfs -v %s -C %s -p ossfs_VERSION_centos6.5_ARCH.rpm  -d "libcurl >= 7.0" -d "libxml2 >= 2.6" -d "openssl-devel >= 0.9" --after-install /root/post_action.sh --after-upgrade /root/post_action.sh' % (ossfs_version, install_dir))
    f.close()

def command_test_package_centos65():
    """
    Generate the test package script running in docker container
    """
    pkg_list = glob.glob(os.path.join(working_dir, 'package/*centos6.5*'))
    if not pkg_list:
        raise RuntimeError("Can not found centos6.5 package! May be build fail?")
    pkg = ntpath.basename(pkg_list[0])
    cmd_dir = os.path.join(working_dir, 'command')
    test_dir = os.path.join(dest_dir, 'source/test')
    f = open(os.path.join(cmd_dir, 'test_package_centos65.sh'), 'w')
    f.write('#!/bin/bash\n')
    f.write('rpm --rebuilddb\n')
    f.write('yum -y localinstall %s/package/%s --nogpgcheck\n' % (dest_dir, pkg))
    command_test_package(f)
    f.close()

def command_build_package_centos70():
    """
    Generate the build package script running in docker container
    """
    cmd_dir = os.path.join(working_dir, 'command')
    install_dir = '/tmp/ossfs_install'
    f = open(os.path.join(cmd_dir, 'build_package_centos70.sh'), 'w')
    f.write('#!/bin/bash\n')
    command_build_package(f, install_dir)
    f.write('fpm -s dir -t rpm -n ossfs -v %s -C %s -p ossfs_VERSION_centos7.0_ARCH.rpm -d "fuse >= 2.8.4" -d "fuse-libs >= 2.8.4" -d "libcurl >= 7.0" -d "libxml2 >= 2.6" -d "openssl-libs >= 0.9"\n' % (ossfs_version, install_dir))
    f.close()

def command_test_package_centos70():
    """
    Generate the test package script running in docker container
    """
    pkg_list = glob.glob(os.path.join(working_dir, 'package/*centos7.0*'))
    if not pkg_list:
        raise RuntimeError("Can not found centos7.0 package! May be build fail?")
    pkg = ntpath.basename(pkg_list[0])
    cmd_dir = os.path.join(working_dir, 'command')
    test_dir = os.path.join(dest_dir, 'source/test')
    f = open(os.path.join(cmd_dir, 'test_package_centos70.sh'), 'w')
    f.write('#!/bin/bash\n')
    f.write('yum -y localinstall %s/package/%s --nogpgcheck\n' % (dest_dir, pkg))
    command_test_package(f)
    f.close()

def command_build_package_ubuntu(os_name):
    """
    Generate the build package script running in docker container
    """
    cmd_dir = os.path.join(working_dir, 'command')
    install_dir = '/tmp/ossfs_install'
    f = open(os.path.join(cmd_dir, 'build_package_%s.sh'%os_name), 'w')
    f.write('#!/bin/bash\n')
    command_build_package(f, install_dir)
    f.write('fpm -s dir -t deb -n ossfs -v %s -C %s -p ossfs_VERSION_%s_ARCH.deb -d "fuse >= 2.8.4" -d "libcurl3-gnutls >= 7.0" -d "libxml2 >= 2.6" -d "libssl-dev >= 0.9"\n' % (ossfs_version, install_dir, os_name))
    f.close()

def command_test_package_ubuntu(os_name):
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
    f.write('apt update\n')
    f.write('gdebi -n %s/package/%s\n' % (dest_dir,pkg))
    command_test_package(f)
    f.close()

def build_package():
    prepare()
    for os_name in os_list:
        volumes = ['%s:%s'%(working_dir, dest_dir), '%s:%s'%(ossfs_source_dir, os.path.join(dest_dir, 'source'))]
        dev_image = docker_images[os_name+':dev']
        test_image = docker_images[os_name+':test']

        if os_name == 'centos6.5':
            # build package
            print "==========================="
            print "build centos6.5 package ..."
            print "==========================="
            command_build_package_centos65()
            container_name = 'ossfs_%s'%random_string(5)
            docker_run(container_name, dev_image, volumes, '/bin/bash %s/command/build_package_centos65.sh'%dest_dir)

            # test package
            print "=========================="
            print "test centos6.5 package ..."
            print "=========================="
            command_test_package_centos65()
            container_name = 'ossfs_%s'%random_string(5)
            docker_run(container_name, test_image, volumes, '/bin/bash %s/command/test_package_centos65.sh'%dest_dir)
        elif os_name == 'centos7.0':
            # build package
            print "==========================="
            print "build centos7.0 package ..."
            print "==========================="
            command_build_package_centos70()
            container_name = 'ossfs_%s'%random_string(5)
            docker_run(container_name, dev_image, volumes, '/bin/bash %s/command/build_package_centos70.sh'%dest_dir)

            # test package
            print "=========================="
            print "test centos7.0 package ..."
            print "=========================="
            command_test_package_centos70()
            container_name = 'ossfs_%s'%random_string(5)
            docker_run(container_name, test_image, volumes, '/bin/bash %s/command/test_package_centos70.sh'%dest_dir)
        elif os_name.startswith('ubuntu'):
            # build package
            print "============================="
            print "build %s package ..." % os_name
            print "============================="
            command_build_package_ubuntu(os_name)
            container_name = 'ossfs_%s'%random_string(5)
            docker_run(container_name, dev_image, volumes, '/bin/bash %s/command/build_package_%s.sh' % (dest_dir, os_name))

            # test package
            print "============================"
            print "test %s package ..." % os_name
            print "============================"
            command_test_package_ubuntu(os_name)
            container_name = 'ossfs_%s'%random_string(5)
            docker_run(container_name, test_image, volumes, '/bin/bash %s/command/test_package_%s.sh' % (dest_dir, os_name))

if __name__ == '__main__':
    build_package()
    subprocess.check_call(['ln', '-sfT', os.path.join(working_dir, 'package'), dist_dir])
