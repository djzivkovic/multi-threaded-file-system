# Multi-threaded File Storage System
## Overview
This was a university project for the Parallel Algorithms course and it implements a multi-threaded file storage system in Python. The system is designed to handle basic file operations such as put, get, delete, and list through a command-line interface.

## Project Structure
The project is organized into several classes:

### File
Represents a file in the system.

Attributes:
- `id`: Unique identifier for the file.
- `name`: Name of the file.
- `status`: Current status of the file (True if ready, False otherwise).
- `part_count`: Number of parts the file is divided into.

### FileRegistry
Manages the registration of files in the system.

Attributes:
- `id_lock`: Thread lock for ensuring thread-safe access to file IDs.
- `next_id`: Next available file ID.
- `files`: Dictionary containing registered files.

### FilePart
Represents a part of a file.

Attributes:
- `id`: Unique identifier for the file part.
- `parent_id`: ID of the parent file.
- `path`: Path to the file part on disk.
- `hash`: MD5 hash of the file part.
- `status`: Current status of the file part (True if ready, False otherwise).
- `index`: Index of the file part within the file.

### FilePartRegistry
Manages the registration of file parts in the system.

Attributes:
- `id_lock`: Thread lock for ensuring thread-safe access to file part IDs.
- `next_id`: Next available file part ID.
- `parts`: Dictionary containing registered file parts.

### FileStorageSystem
The main class that orchestrates the file storage system.

Methods:
- `load_config(config_file)`: Loads the configuration from the specified YAML file.
- `set_constants()`: Sets constant values for part size and batch size.
- `accept_commands()`: Continuously accepts and processes user commands.
- `__init__(config_file)`: Initializes the file storage system with the specified configuration.
- Methods for handling commands: `handle_put_command`, `handle_get_command`, `handle_delete_command`, `handle_list_command`, `handle_exit_command`.



## Usage

Run the file storage system:
```
python run.py
```
Enter commands at the prompt to interact with the file storage system:

- `put <file_name>`: Put a file into the system.
- `get <file_id>`: Get a file from the system.
- `delete <file_id>`: Delete a file from the system.
- `list`: List all files in the system.
- `exit`: Exit the file storage system.

## Configuration
The file storage system reads its configuration from the config.yaml file, which includes parameters such as the parts directory, maximum memory, and the number of I/O processes.

Note: The system assumes that the user has the necessary permissions to perform file I/O operations in the specified parts directory.
