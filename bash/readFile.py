def read_file(filename):
    try:
        with open(filename, 'r') as file:
            content = file.read()
            print(content)
    except FileNotFoundError:
        print(f"The file '{filename}' does not exist.")
    except IOError:
        print(f"An error occurred while reading the file '{filename}'.")

# Example usage:
file_name = 'example.txt'
read_file(file_name)
