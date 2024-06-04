/*
 * ossfs - FUSE-based file system backed by Alibaba Cloud OSS
 *
 * Copyright(C) 2007 Randy Rizun <rrizun@gmail.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <algorithm>
#include <string>
#include <iostream>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <getopt.h>

const size_t MB = 1024 * 1024;

void read_from_oss_and_write_to_disk_rand(const char* read_path, 
                                          const char* write_path,
                                          const size_t total_size) {
    struct stat stbuf;
    if (stat(read_path, &stbuf) < 0) {
        std::cout << "stat file failed, errno: " << errno << std::endl;
        exit(1);
    }

    int fd = open(read_path, O_RDONLY);
    if (fd < 0) {
        std::cout << "open read file failed" << std::endl;
        exit(1);
    }

    int wfd = open(write_path, O_CREAT|O_RDWR, 0644);
    if (wfd < 0) {
        std::cout << "open write file failed" << std::endl;
        exit(1);
    }

    char buf[1 * MB];
    ssize_t bytesread = 0, byteswritten = 0;
    off_t offset = 0;

    // shuffle the read order in the unit of 100MB
    std::vector<int> read_order_in_the_unit_of_100MB;
    std::cout << "total size " << total_size << std::endl;
    for (int i = 0; i < (total_size+100*MB-1) / 100 / MB; ++i) {
        read_order_in_the_unit_of_100MB.push_back(i);
    }
    std::random_shuffle(read_order_in_the_unit_of_100MB.begin(), 
                        read_order_in_the_unit_of_100MB.end());
    size_t sub_total_size = 100*MB;

    for (int i = 0; i < read_order_in_the_unit_of_100MB.size(); ++i) {
        int index = read_order_in_the_unit_of_100MB[i];
        sub_total_size = std::min(100 * MB, total_size - index * 100 * MB);
        off_t start_offset = index * 100 * MB;
        std::cout << "random read, start from " << index*100 << "MB, sub_total_size: " << sub_total_size << std::endl;
        offset = 0;
        while (offset < sub_total_size) {
            size_t buf_size = std::min(MB, sub_total_size - offset);
            bytesread = pread(fd, buf, buf_size, start_offset+offset);
            byteswritten = pwrite(wfd, buf, bytesread, start_offset+offset);
            offset += byteswritten;
        }
    }
    
    close(fd);
    close(wfd);
}

int main(int argc, char **argv) {
    int o;
    const char* opts = "r:s:w:";
    char* read_path;
    char* write_path;
    long total_size = 0;

    while ((o = getopt(argc, argv, opts)) != -1) {
        switch (o) {
            case 'r':
                read_path = optarg;
                break;
            case 's':
                total_size = atol(optarg);
                break;
            case 'w':
                write_path = optarg;
                break;
            default:
                std::cout << "Usage: " << argv[0] << " -r <read file> -w <write file> -s <total size in bytes>" << std::endl;
                exit(1);
        }
    }

    read_from_oss_and_write_to_disk_rand(read_path, write_path, total_size);            
    return 0;
}