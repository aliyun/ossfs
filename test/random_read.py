import sys
import os

def read_file_interval(filename):  
    chunk_size = 4 * 1024       # 4KB  
    skip_size = 4 * 1024 * 1024  # 4MB

    offset = 0
    total_size = os.path.getsize(filename)
    
    with open(filename, 'rb') as f:  
        while offset < total_size:
            rd_size = chunk_size
            if offset + chunk_size > total_size:
                rd_size = total_size - offset
            data = f.read(rd_size)  
            if not data:
                print("ERROR. No data read.")
                break

            f.seek(skip_size, 1)
            offset = offset + rd_size + skip_size
          
        print("READ COMPLETE.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python random_read.py <filename>")
        sys.exit(1)
    filename = sys.argv[1] 
    read_file_interval(filename)
