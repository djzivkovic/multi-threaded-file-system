import hashlib
import multiprocessing
import os
import sys
import threading
import zlib

import yaml


class File:
    def __init__(self, _id: int, name: str, status: bool, n: int):
        self.id = _id
        self.name = name
        self.status = status
        self.part_count = n


class FileRegistry:
    def __init__(self):
        self.id_lock = threading.Lock()
        self.next_id = 0
        self.files = {}


class FilePart:
    def __init__(self, _id: int, parent_id: int, _hash: str, status: bool, index: int):
        self.id = _id
        self.parent_id = parent_id
        self.path = f"{parent_id}_{index}.PART"
        self.hash = _hash
        self.status = status
        self.index = index


class FilePartRegistry:
    def __init__(self):
        self.id_lock = threading.Lock()
        self.next_id = 0
        self.parts = {}


class FileStorageSystem:
    def load_config(self, config_file):
        with open(config_file, 'r', encoding="utf-8") as file:
            self.configuration = yaml.safe_load(file)
            self.parts_directory = self.configuration["parts_directory"]
            if not os.path.exists(self.parts_directory):
                os.makedirs(self.parts_directory, exist_ok=True)
            self.memory_limit = self.configuration["max_memory"] * 1024 * 1024
            self.io_limit = self.configuration["io_processes"]

    def set_constants(self):
        self.part_size = 1024  # bytes (1KB)
        self.batch_size = 10  # batch = 10 parts

    def __init__(self, config_file):
        self.set_constants()
        self.load_config(config_file)

        self.file_registry = FileRegistry()
        self.file_part_registry = FilePartRegistry()

        self.memory_condition = threading.Condition()
        self.memory_usage = 0  # current usage

        cpus = multiprocessing.cpu_count()
        self.io = multiprocessing.Pool(self.io_limit if self.io_limit <= cpus else cpus)

        self.commands = {
            "put": self.handle_put_command,
            "get": self.handle_get_command,
            "delete": self.handle_delete_command,
            "list": self.handle_list_command,
            "exit": self.handle_exit_command
        }

    def accept_commands(self):
        while True:
            user_input = sys.stdin.readline()
            words = user_input.split()
            if len(words) < 1:
                sys.stdout.write("\nUnknown command.\n")
                continue
            command = words[0]
            handler = self.commands[command] if command in self.commands else None
            if handler is None:
                sys.stdout.write("\nUnknown command.\n")
                continue
            if command == "exit":
                self.commands[command](command)
                return
            t = threading.Thread(target=handler, args=(user_input,))
            t.start()


    def handle_put_command(self, command):
        words = command.split()
        if len(words) != 2:
            sys.stdout.write("\nWrong arguments for put command.\n")
            return

        file_name = words[1]

        if not self.file_registry.id_lock.acquire(blocking=True, timeout=5):
            sys.stdout.write("\nPut: Operation failed, acquiring file id lock timed out.\n")
            return

        file_id = self.file_registry.next_id
        self.file_registry.next_id += 1
        self.file_registry.id_lock.release()

        file = File(file_id, file_name, False, 0)
        self.file_registry.files[file_id] = file

        locked_memory = False
        try:
            with open(file_name, "rb") as reader:
                index = 0
                file_finished = False
                self.file_part_registry.parts[file_id] = []
                while not file_finished:
                    needed_memory = self.part_size * self.batch_size
                    if not self.memory_condition.acquire(blocking=True, timeout=5):
                        continue
                    if not self.memory_condition.wait_for(predicate=lambda: self.memory_usage + needed_memory < self.memory_limit, timeout=5):
                        # no memory, wait again
                        continue
                    self.memory_usage += needed_memory
                    locked_memory = True
                    self.memory_condition.release()

                    batch = []
                    file_parts = []

                    for i in range(self.batch_size):
                        if not self.file_part_registry.id_lock.acquire(blocking=True, timeout=5):
                            sys.stdout.write("\nPut: Operation failed, acquiring file part id lock timed out\n")
                            if not self.memory_condition.acquire(blocking=True, timeout=5):
                                sys.stdout.write("\nPut: Could not release locked memory.\n")
                                return
                            self.memory_usage -= needed_memory
                            locked_memory = False
                            self.memory_condition.notify_all()
                            self.memory_condition.release()
                            return
                        part_id = self.file_part_registry.next_id
                        self.file_part_registry.next_id += 1
                        self.file_part_registry.id_lock.release()

                        part = reader.read(self.part_size)

                        if part == b'':
                            file_finished = True
                            break

                        file_part = FilePart(part_id, file_id, None, False, index)
                        self.file_part_registry.parts[file_id].append(file_part)
                        index += 1

                        file_parts.append(file_part)
                        batch.append((part, self.parts_directory + file_part.path))

                    result = self.io.starmap(save_file, batch)

                    for i, part in enumerate(file_parts):
                        self.file_part_registry.parts[file_id][part.index].hash = result[i]
                        self.file_part_registry.parts[file_id][part.index].status = True

                    if not self.memory_condition.acquire(blocking=True, timeout=5):
                        sys.stdout.write("\nPut: Failed to notify memory condition, acquiring lock timed out.\n")
                        continue
                    self.memory_usage -= needed_memory
                    locked_memory = False
                    self.memory_condition.notify_all()
                    self.memory_condition.release()

                self.file_registry.files[file_id].part_count = len(self.file_part_registry.parts[file_id])
                self.file_registry.files[file_id].status = True
                sys.stdout.write(f"\nPut: {file_name}: Operation completed.\n")
        except Exception as e:
            sys.stdout.write(f"\nFailed to put file: {str(e)}\n")
            if not locked_memory:
                return
            if not self.memory_condition.acquire(blocking=True, timeout=5):
                sys.stdout.write("\nPut: Could not release locked memory.\n")
                return
            self.memory_usage -= needed_memory
            self.memory_condition.notify_all()
            self.memory_condition.release()


    def handle_get_command(self, command: str):
        words = command.split()
        if len(words) != 2:
            sys.stdout.write("\nWrong arguments for get command.\n")
            return
        raw_file_id = words[1]
        if not raw_file_id.isdigit():
            sys.stdout.write("\nGet: Invalid file id.\n")
            return
        file_id = int(raw_file_id)
        if file_id not in self.file_registry.files:
            sys.stdout.write("\nGet: File with given id does not exist.\n")
            return
        file = self.file_registry.files[file_id]
        if not file.status:
            sys.stdout.write("\nGet: File not ready.\n")
            return
        file_parts = self.file_part_registry.parts[file.id]
        index = 0
        locked_memory = False
        try:
            with open(file.name, "wb") as writer:
                while index*self.batch_size < len(file_parts):
                    needed_memory = self.part_size * self.batch_size
                    if not self.memory_condition.acquire(blocking=True, timeout=5):
                        continue
                    if not self.memory_condition.wait_for(predicate=lambda: self.memory_usage + needed_memory < self.memory_limit, timeout=5):
                        # no memory, wait again
                        continue
                    self.memory_usage += needed_memory
                    locked_memory = True
                    self.memory_condition.release()

                    start = index*self.batch_size
                    end = (index+1)*self.batch_size
                    end = end if end < len(file_parts) else len(file_parts)
                    batch = [(part.hash, self.parts_directory + part.path) for part in file_parts[start:end]]
                    index += 1

                    result = self.io.starmap(read_file, batch)

                    for i, part in enumerate(result):
                        ok, data = part
                        if not ok:
                            sys.stdout.write("\nGet: MD5 Hash mismatch.\n")
                            if not self.memory_condition.acquire(blocking=True, timeout=5):
                                sys.stdout.write("\nGet: Could not release locked memory.\n")
                                return
                            self.memory_usage -= needed_memory
                            locked_memory = False
                            self.memory_condition.notify_all()
                            self.memory_condition.release()
                            return
                        writer.write(data)

                    if not self.memory_condition.acquire(blocking=True, timeout=5):
                        sys.stdout.write("\nGet: Failed to notify memory condition, acquiring lock timed out.\n")
                        continue
                    self.memory_usage -= needed_memory
                    locked_memory = False
                    self.memory_condition.notify_all()
                    self.memory_condition.release()

        except Exception as e:
            sys.stdout.write(f"\nFailed to get file: {str(e)}\n")
            if not locked_memory:
                return
            if not self.memory_condition.acquire(blocking=True, timeout=5):
                sys.stdout.write("\nGet: Could not release locked memory.\n")
                return
            self.memory_usage -= needed_memory
            self.memory_condition.notify_all()
            self.memory_condition.release()

    def handle_delete_command(self, command: str):
        words = command.split()
        if len(words) != 2:
            sys.stdout.write("\nWrong arguments for delete command.\n")
            return
        raw_file_id = words[1]
        if not raw_file_id.isdigit():
            sys.stdout.write("\nDelete: Invalid file id.\n")
            return
        file_id = int(raw_file_id)
        if file_id not in self.file_registry.files:
            sys.stdout.write("\nDelete: File with given id does not exist.\n")
            return
        file = self.file_registry.files[file_id]
        if not file.status:
            sys.stdout.write("\nDelete: File not ready.\n")
            return
        self.file_registry.files[file_id].status = False
        file_parts = self.file_part_registry.parts[file.id]
        index = len(file_parts) // self.batch_size
        try:
            while index >= 0:
                start = index * self.batch_size
                end = (index + 1) * self.batch_size
                end = end if end < len(file_parts) else len(file_parts)
                batch = [self.parts_directory + part.path for part in file_parts[start:end]]
                for j in range(start, end):
                    self.file_part_registry.parts[file.id][j].status = False
                result = self.io.map(delete_file, batch)
                for i, delete_result in enumerate(result):
                    if delete_result:
                        del self.file_part_registry.parts[file.id][index * self.batch_size + len(result) - i - 1]
                    else:
                        sys.stdout.write("\nError deleting file.\n")
                index -= 1
            del self.file_registry.files[file_id]
        except Exception as e:
            sys.stdout.write(f"\nFailed to delete file: {str(e)}\n")
    def handle_list_command(self, command: str):
        for file in self.file_registry.files.values():
            sys.stdout.write(f"{file.id} {file.name}\n")

    def handle_exit_command(self, command: str):
        self.io.close()
        self.io.join()
        exit()


def save_file(data: bytes, path: str) -> str:
    md5 = hashlib.md5(data).hexdigest()
    compressed = zlib.compress(data)
    with open(path, "wb") as writer:
        writer.write(compressed)
    return md5


def read_file(expected_md5: str, path: str) -> (bool, bytes):
    with open(path, "rb") as reader:
        compressed = reader.read()
        decompressed = zlib.decompress(compressed)
        md5 = hashlib.md5(decompressed).hexdigest()
        if md5 != expected_md5:
            return False, None
        return True, decompressed


def delete_file(path: str) -> bool:
    try:
        os.remove(path)
        return True
    except:
        return False

if __name__ == '__main__':
    storage_system = FileStorageSystem('config.yaml')
    storage_system.accept_commands()
