def write_file(filename, content):
    try:
        with open(filename, 'w') as file:
            file.write(content)
        print(f"File '{filename}' created and written successfully.")
    except IOError:
        print(f"An error occurred while writing to the file '{filename}'.")

# Example usage:
file_name = '/mnt/imss/example.txt'
file_content = "Hello, world!"
write_file(file_name, file_content)
