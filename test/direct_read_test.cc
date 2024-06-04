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

#include <iostream>
#include <stdlib.h>
#include <getopt.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string>
#include <chrono>
#include <thread>
#include <algorithm>

/*
    This program is used to test the direct reading when 
    suddenly skipping a certain piece of data. The case name is
    test_direct_read_with_out_of_order_read in integration-test-main.sh
*/

const int MB = 1024 * 1024;

int main(int argc, char **argv) 
{
    int o;
    const char* opts = "r:s:o:w:";
    char* read_path;
    char* write_path;
    size_t skip_size = 0;
    off_t start_skip_offset = 0;
    while ((o = getopt(argc, argv, opts)) != -1) {
        switch (o) {
            case 'r':
                read_path = optarg;
                break;
            case 's':
                skip_size = atoi(optarg);
                skip_size = skip_size * MB;
                break;
            case 'o':
                start_skip_offset = atoi(optarg);
                start_skip_offset = start_skip_offset * MB;
                break;
            case 'w':
                write_path = optarg;
                break;
            default:
                std::cout << "Usage: " << argv[0] << " -r <read file> -o <start skipping offset> -s <skip size> -w <write file>" << std::endl;
                exit(1);
        }
    }

    struct stat stbuf;
    if (stat(read_path, &stbuf) < 0) {
        std::cout << "stat file failed, errno: " << errno << std::endl;
        exit(1);
    }

    if (stbuf.st_size < start_skip_offset || stbuf.st_size < start_skip_offset + skip_size) {
        std::cout << "skip_offset or skip_size is invalid." << std::endl;
        exit(1);
    }

    int fd = open(read_path, O_RDONLY);
    if (fd < 0) {
        std::cout << "open read file failed" << std::endl;
        exit(1);
    }

    int wfd = open(write_path, O_CREAT|O_RDWR|O_TRUNC, 0644);
    if (wfd < 0) {
        std::cout << "open write file failed" << std::endl;
        exit(1);
    }

    char buf[1 * MB];
    ssize_t bytesread = 0;
    off_t offset_read = 0;
    off_t offset_write = 0;
    while(start_skip_offset - offset_read) {
        size_t nbytes = std::min(off_t(MB), start_skip_offset - offset_read);
        bytesread = pread(fd, buf, nbytes, offset_read);
        if (bytesread < 0) {
            std::cout << "failed to read file" << std::endl;
            exit(1);
        } else if (bytesread == 0) {
            break;
        }

        pwrite(wfd, buf, bytesread, offset_write);
        offset_read += bytesread;
        offset_write += bytesread;
        if (offset_read >= start_skip_offset) {
            break;
        }
    }

    offset_read += skip_size;
    while(offset_read < stbuf.st_size) {
        size_t nbytes = std::min(off_t(MB), stbuf.st_size - offset_read);
        bytesread = pread(fd, buf, nbytes, offset_read);
        if (bytesread < 0) {
            std::cout << "failed to read file" << std::endl;
            exit(1);
        } else if (bytesread == 0) {
            break;
        }

        pwrite(wfd, buf, bytesread, offset_write);
        offset_read += bytesread;
        offset_write += bytesread;
    }

    close(fd);
    close(wfd);

    return 0;
}