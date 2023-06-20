import sys
import os

def get_file_size(file_name):
    # if os.path.isfile(file_name):
    fsize = os.path.getsize(file_name)
    print("[1] File size={}".format(fsize))
    # fsize = os.stat(file_name)
    # print("[2] File size={}".format(fsize))
    return fsize
    # else:
    #     return "File not found."

def read_file(filename):
    try:
        print("Reading file {}".format(filename))
        fsize = get_file_size(file_name)
        with open(filename, 'r') as file:
            content = file.read()
            print("Content: {}".format(content))
    except FileNotFoundError:
        print(f"The file '{filename}' does not exist.")
    except IOError:
        print(f"An error occurred while reading the file '{filename}'.")

# Example usage:
# file_name = '/mnt/imss/example.txt'
file_name = sys.argv[1]
# fsize = get_file_size(file_name)
read_file(file_name)
