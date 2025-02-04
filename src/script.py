import functools
import math
from functools import reduce
import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import json
import sys
import logging
import threading
from multiprocessing import Process
import asyncio
from collections import namedtuple
from collections import deque
from collections import defaultdict
from collections import Counter
from collections import ChainMap
from typing import NamedTuple
from dataclasses import dataclass
from enum import Enum
import weakref
import heapq
import bisect
import array
import struct
import itertools
import operator
from pathlib import Path
import uuid
import base64
import zlib
import csv
import sqlite3
import smtplib
from email.mime.text import MIMEText
import socket
import argparse
import configparser
import tarfile
import zipfile
import shutil
import subprocess
import timeit
import cProfile
import pdb
import signal
from enum import auto
from contextlib import contextmanager
from datetime import datetime, timedelta
import calendar
import random
import secrets
import hashlib
import hmac
import bcrypt
from argon2 import PasswordHasher
import unittest
from unittest.mock import Mock


# 1. Variables and Data Types
integer_var = 10
float_var = 20.5
string_var = "Hello, World!"
boolean_var = True

# 2. Lists
my_list = [1, 2, 3, 4, 5]
print("List:", my_list)

# 3. Tuples
my_tuple = (1, 2, 3, 4, 5)
print("Tuple:", my_tuple)

# 4. Dictionaries
my_dict = {"name": "Alice", "age": 25}
print("Dictionary:", my_dict)

# 5. Conditional Statements
if integer_var > 5:
    print("integer_var is greater than 5")
else:
    print("integer_var is not greater than 5")

# 6. Loops
# For loop
for i in range(5):
    print("For loop iteration:", i)

# While loop
count = 0
while count < 5:
    print("While loop iteration:", count)
    count += 1

# 7. Functions
def greet(name):
    return f"Hello, {name}!"

print(greet("Alice"))

# 8. Classes and Objects
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def introduce(self):
        return f"My name is {self.name} and I am {self.age} years old."

person = Person("Bob", 30)
print(person.introduce())

# 9. Exception Handling
try:
    result = 10 / 0
except ZeroDivisionError:
    print("Cannot divide by zero")

# 10. File I/O
with open("example.txt", "w") as file:
    file.write("This is an example file.")

with open("example.txt", "r") as file:
    content = file.read()
    print("File content:", content)

# 11. List Comprehensions
squares = [x**2 for x in range(10)]
print("Squares:", squares)

# 12. Lambda Functions
add = lambda x, y: x + y
print("Lambda function result:", add(5, 3))

# 13. Modules and Imports
print("Square root of 16:", math.sqrt(16))

# 14. Decorators
def my_decorator(func):
    def wrapper():
        print("Something is happening before the function is called.")
        func()
        print("Something is happening after the function is called.")
    return wrapper

@my_decorator
def say_hello():
    print("Hello!")

say_hello()

# 15. Generators
def my_generator(n):
    for i in range(n):
        yield i

gen = my_generator(5)
for i in gen:
    print("Generator value:", i)

# 16. Recursion
def factorial(n):
    if n == 0:
        return 1
    else:
        return n * factorial(n - 1)
    print("Factorial of 5:", factorial(5))

if __name__ == "__main__":

    # 17. Map, Filter, and Reduce

    # Map
    numbers = [1, 2, 3, 4, 5]
    squared_numbers = list(map(lambda x: x**2, numbers))
    print("Squared numbers using map:", squared_numbers)

    # Filter
    even_numbers = list(filter(lambda x: x % 2 == 0, numbers))
    print("Even numbers using filter:", even_numbers)

    # Reduce
    sum_of_numbers = reduce(lambda x, y: x + y, numbers)
    print("Sum of numbers using reduce:", sum_of_numbers)

    # 18. Sets
    my_set = {1, 2, 3, 4, 5}
    print("Set:", my_set)

    # 19. List Slicing
    sliced_list = my_list[1:4]
    print("Sliced list:", sliced_list)

    # 20. String Formatting
    formatted_string = f"My name is {my_dict['name']} and I am {my_dict['age']} years old."
    print("Formatted string:", formatted_string)

    # 21. Multiple Assignment
    a, b, c = 1, 2, 3
    print("Multiple assignment:", a, b, c)

    # 22. Swap Variables
    a, b = b, a
    print("Swapped variables:", a, b)

    # 23. Enumerate
    for index, value in enumerate(my_list):
        print(f"Index: {index}, Value: {value}")

    # 24. Zip
    list1 = [1, 2, 3]
    list2 = ['a', 'b', 'c']
    zipped = list(zip(list1, list2))
    print("Zipped list:", zipped)

    # 25. Any and All
    bool_list = [True, False, True]
    print("Any true in list:", any(bool_list))
    print("All true in list:", all(bool_list))

    # 26. Sorting
    unsorted_list = [3, 1, 4, 1, 5, 9]
    sorted_list = sorted(unsorted_list)
    print("Sorted list:", sorted_list)

    # 27. Reversing
    reversed_list = list(reversed(my_list))
    print("Reversed list:", reversed_list)

    # 28. Copying Lists
    copied_list = my_list[:]
    print("Copied list:", copied_list)

    # 29. List Methods
    my_list.append(6)
    print("List after append:", my_list)
    my_list.remove(6)
    print("List after remove:", my_list)

    # 30. Dictionary Methods
    keys = my_dict.keys()
    values = my_dict.values()
    print("Dictionary keys:", keys)
    print("Dictionary values:", values)

    # 31. Data Analysis with Pandas

    # Creating a DataFrame
    data = {
        "Name": ["Alice", "Bob", "Charlie", "David"],
        "Age": [25, 30, 35, 40],
        "City": ["New York", "Los Angeles", "Chicago", "Houston"]
    }
    df = pd.DataFrame(data)
    print("DataFrame:\n", df)

    # Descriptive Statistics
    print("Descriptive Statistics:\n", df.describe())

    # Selecting Columns
    print("Names:\n", df["Name"])

    # Filtering Rows
    print("People older than 30:\n", df[df["Age"] > 30])

    # Grouping Data
    grouped = df.groupby("City").mean()
    print("Grouped by City:\n", grouped)

    # Adding a New Column
    df["Age in 10 Years"] = df["Age"] + 10
    print("DataFrame with new column:\n", df)

    # Removing a Column
    df = df.drop(columns=["Age in 10 Years"])
    print("DataFrame after removing column:\n", df)

    # 32. Merging DataFrames
    data1 = {
        "Name": ["Alice", "Bob", "Charlie"],
        "Age": [25, 30, 35]
    }
    data2 = {
        "Name": ["Alice", "Bob", "Charlie"],
        "City": ["New York", "Los Angeles", "Chicago"]
    }
    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)
    merged_df = pd.merge(df1, df2, on="Name")
    print("Merged DataFrame:\n", merged_df)

    # 33. Pivot Tables
    data = {
        "Name": ["Alice", "Bob", "Charlie", "Alice", "Bob", "Charlie"],
        "Month": ["Jan", "Jan", "Jan", "Feb", "Feb", "Feb"],
        "Sales": [100, 200, 300, 150, 250, 350]
    }
    df = pd.DataFrame(data)
    pivot_table = df.pivot_table(values="Sales", index="Name", columns="Month", aggfunc="sum")
    print("Pivot Table:\n", pivot_table)

    # 34. Handling Missing Data
    data = {
        "Name": ["Alice", "Bob", "Charlie"],
        "Age": [25, None, 35],
        "City": ["New York", "Los Angeles", None]
    }
    df = pd.DataFrame(data)
    print("DataFrame with missing data:\n", df)
    df_filled = df.fillna({"Age": df["Age"].mean(), "City": "Unknown"})
    print("DataFrame after filling missing data:\n", df_filled)

    # 35. Time Series Analysis
    date_rng = pd.date_range(start="2023-01-01", end="2023-01-10", freq="D")
    df = pd.DataFrame(date_rng, columns=["date"])
    df["data"] = pd.Series(range(1, len(df) + 1))
    df.set_index("date", inplace=True)
    print("Time Series DataFrame:\n", df)

    # 36. Web Scraping with BeautifulSoup

    url = "http://example.com"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    print("Title of the webpage:", soup.title.string)

    # 37. Regular Expressions

    text = "The rain in Spain"
    match = re.search(r"\bS\w+", text)
    if match:
        print("Found a match:", match.group())

    # 38. JSON Handling

    json_data = '{"name": "Alice", "age": 25, "city": "New York"}'
    parsed_data = json.loads(json_data)
    print("Parsed JSON data:", parsed_data)

    # 39. Command Line Arguments

    if len(sys.argv) > 1:
        print("Command line arguments:", sys.argv[1:])

    # 40. Logging

    logging.basicConfig(level=logging.INFO)
    logging.info("This is an info message")

    # 41. Multithreading


    def print_numbers():
        for i in range(5):
            print(f"Number: {i}")

    def print_letters():
        for letter in "ABCDE":
            print(f"Letter: {letter}")

    thread1 = threading.Thread(target=print_numbers)
    thread2 = threading.Thread(target=print_letters)

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    # 42. Multiprocessing


    def print_squares():
        for i in range(5):
            print(f"Square: {i**2}")

    def print_cubes():
        for i in range(5):
            print(f"Cube: {i**3}")

    process1 = Process(target=print_squares)
    process2 = Process(target=print_cubes)

    process1.start()
    process2.start()

    process1.join()
    process2.join()

    # 43. Context Managers

    class MyContextManager:
        def __enter__(self):
            print("Entering the context")
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            print("Exiting the context")

    with MyContextManager():
        print("Inside the context")

    # 44. Type Hinting

    def add_numbers(a: int, b: int) -> int:
        return a + b

    print("Type hinted function result:", add_numbers(5, 3))

    # 45. Asynchronous Programming


    async def async_hello():
        print("Hello")
        await asyncio.sleep(1)
        print("World")

    asyncio.run(async_hello())

    # 46. Property Decorators

    class Circle:
        def __init__(self, radius):
            self._radius = radius

        @property
        def radius(self):
            return self._radius

        @radius.setter
        def radius(self, value):
            if value < 0:
                raise ValueError("Radius cannot be negative")
            self._radius = value

        @property
        def area(self):
            return math.pi * (self._radius ** 2)

    circle = Circle(5)
    print("Circle radius:", circle.radius)
    print("Circle area:", circle.area)
    circle.radius = 10
    print("Updated circle radius:", circle.radius)
    print("Updated circle area:", circle.area)

    # 47. Named Tuples


    Point = namedtuple("Point", ["x", "y"])
    p = Point(1, 2)
    print("Named tuple:", p)
    print("x coordinate:", p.x)
    print("y coordinate:", p.y)

    # 48. Deque


    d = deque([1, 2, 3])
    d.append(4)
    d.appendleft(0)
    print("Deque after append and appendleft:", d)
    d.pop()
    d.popleft()
    print("Deque after pop and popleft:", d)

    # 49. DefaultDict


    dd = defaultdict(lambda: "default value")
    dd["key1"] = "value1"
    print("DefaultDict key1:", dd["key1"])
    print("DefaultDict key2 (non-existent):", dd["key2"])

    # 50. Counter


    count = Counter(["a", "b", "c", "a", "b", "a"])
    print("Counter:", count)
    print("Most common elements:", count.most_common(2))

    # 51. ChainMap


    dict1 = {"a": 1, "b": 2}
    dict2 = {"b": 3, "c": 4}
    chain = ChainMap(dict1, dict2)
    print("ChainMap:", chain)
    print("Value for key 'b':", chain["b"])

    # 52. NamedTuple


    class Employee(NamedTuple):
        name: str
        age: int
        title: str

    emp = Employee("John Doe", 30, "Software Engineer")
    print("NamedTuple Employee:", emp)
    print("Employee name:", emp.name)
    print("Employee age:", emp.age)
    print("Employee title:", emp.title)

    # 53. Dataclasses


    @dataclass
    class Car:
        make: str
        model: str
        year: int

    car = Car("Toyota", "Corolla", 2020)
    print("Dataclass Car:", car)
    print("Car make:", car.make)
    print("Car model:", car.model)
    print("Car year:", car.year)

    # 54. Frozenset

    fs = frozenset([1, 2, 3, 4, 5])
    print("Frozenset:", fs)
    print("Is 3 in frozenset:", 3 in fs)

    # 55. Memoryview

    byte_array = bytearray("hello", "utf-8")
    mv = memoryview(byte_array)
    print("Memoryview:", mv)
    print("Memoryview to list:", mv.tolist())
    # 56. Enumerations


    class Color(Enum):
        RED = 1
        GREEN = 2
        BLUE = 3

    print("Enum member:", Color.RED)
    print("Enum name:", Color.RED.name)
    print("Enum value:", Color.RED.value)

    # 57. Weak References


    class MyClass:
        def __init__(self, value):
            self.value = value

    obj = MyClass(10)
    weak_obj = weakref.ref(obj)
    print("Weak reference:", weak_obj)
    print("Dereferenced weak reference:", weak_obj().value)

    # 58. Heap Queue


    heap = [1, 3, 5, 7, 9, 2, 4, 6, 8, 0]
    heapq.heapify(heap)
    print("Heapified list:", heap)
    heapq.heappush(heap, -5)
    print("Heap after push:", heap)
    print("Popped element:", heapq.heappop(heap))
    print("Heap after pop:", heap)

    # 59. Bisect


    sorted_list = [1, 3, 4, 4, 4, 6, 7]
    bisect.insort(sorted_list, 5)
    print("List after insort:", sorted_list)
    print("Position of 4 in list:", bisect.bisect_left(sorted_list, 4))

    # 60. Array


    arr = array.array('i', [1, 2, 3, 4, 5])
    print("Array:", arr)
    arr.append(6)
    print("Array after append:", arr)
    print("Array element at index 2:", arr[2])

    # 61. Struct


    packed_data = struct.pack('iif', 1, 2, 3.0)
    print("Packed data:", packed_data)
    unpacked_data = struct.unpack('iif', packed_data)
    print("Unpacked data:", unpacked_data)

    # 62. Itertools


    counter = itertools.count(start=5, step=2)
    print("First five elements of counter:", [next(counter) for _ in range(5)])

    cycle = itertools.cycle('ABCD')
    print("First eight elements of cycle:", [next(cycle) for _ in range(8)])

    repeat = itertools.repeat(10, 3)
    print("Repeated elements:", list(repeat))

    # 63. Functools

    @functools.lru_cache(maxsize=32)
    def fibonacci(n):
        if n < 2:
            return n
        return fibonacci(n-1) + fibonacci(n-2)

    print("Fibonacci sequence:", [fibonacci(n) for n in range(10)])

    # 64. Operator


    print("Addition using operator:", operator.add(2, 3))
    print("Concatenation using operator:", operator.concat('Hello', ' World'))

    # 65. Pathlib


    path = Path('/home/geraldohomero/Documents/Github/dh-youtube-research/src')
    print("Path exists:", path.exists())
    print("Is directory:", path.is_dir())
    print("Is file:", path.is_file())
    print("Parent directory:", path.parent)

    # 66. UUID


    unique_id = uuid.uuid4()
    print("Generated UUID:", unique_id)

    # 67. Base64 Encoding and Decoding


    message = "Hello, World!"
    encoded_message = base64.b64encode(message.encode())
    print("Encoded message:", encoded_message)
    decoded_message = base64.b64decode(encoded_message).decode()
    print("Decoded message:", decoded_message)

    # 68. Zlib Compression


    data = b"Hello, World!"
    compressed_data = zlib.compress(data)
    print("Compressed data:", compressed_data)
    decompressed_data = zlib.decompress(compressed_data)
    print("Decompressed data:", decompressed_data)

    # 69. XML Parsing

    import xml.etree.ElementTree as ET

    xml_data = """<root><child name="child1"/><child name="child2"/></root>"""
    root = ET.fromstring(xml_data)
    for child in root:
        print("Child name:", child.attrib['name'])

    # 70. CSV Handling


    csv_data = """name,age,city
    Alice,25,New York
    Bob,30,Los Angeles
    Charlie,35,Chicago"""

    with open("example.csv", "w") as file:
        file.write(csv_data)

    with open("example.csv", "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            print("CSV row:", row)

    # 71. SQLite Database


    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)''')
    cursor.execute('''INSERT INTO users (name, age) VALUES ('Alice', 25)''')
    cursor.execute('''INSERT INTO users (name, age) VALUES ('Bob', 30)''')
    conn.commit()

    cursor.execute('''SELECT * FROM users''')
    rows = cursor.fetchall()
    for row in rows:
        print("SQLite row:", row)

    conn.close()

    # 72. Email Sending


    msg = MIMEText("This is a test email.")
    msg["Subject"] = "Test Email"
    msg["From"] = "sender@example.com"
    msg["To"] = "receiver@example.com"

    # Note: This example uses a dummy SMTP server. Replace with actual server details.
    with smtplib.SMTP("localhost") as server:
        server.send_message(msg)

    # 73. Socket Programming


    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("localhost", 12345))
    server_socket.listen(1)
    print("Server listening on port 12345")

    client_socket, addr = server_socket.accept()
    print("Connection from:", addr)
    client_socket.sendall(b"Hello, Client!")
    client_socket.close()
    server_socket.close()

    # 74. Argparse for Command Line Arguments


    parser = argparse.ArgumentParser(description="Example script")
    parser.add_argument("name", type=str, help="Your name")
    args = parser.parse_args()
    print("Hello,", args.name)

    # 75. ConfigParser for Configuration Files


    config = configparser.ConfigParser()
    config["DEFAULT"] = {"Server": "localhost", "Port": "8080"}
    with open("config.ini", "w") as configfile:
        config.write(configfile)

    config.read("config.ini")
    print("Server:", config["DEFAULT"]["Server"])
    print("Port:", config["DEFAULT"]["Port"])

    # 76. Tarfile for Archiving


    with tarfile.open("example.tar.gz", "w:gz") as tar:
        tar.add("example.txt")

    with tarfile.open("example.tar.gz", "r:gz") as tar:
        tar.extractall()
        print("Extracted files:", tar.getnames())

    # 77. Zipfile for Compression


    with zipfile.ZipFile("example.zip", "w") as zipf:
        zipf.write("example.txt")

    with zipfile.ZipFile("example.zip", "r") as zipf:
        zipf.extractall()
        print("Extracted files:", zipf.namelist())

    # 78. Shutil for File Operations


    shutil.copy("example.txt", "example_copy.txt")
    print("File copied")

    shutil.move("example_copy.txt", "example_moved.txt")
    print("File moved")

    shutil.rmtree("example_dir")
    print("Directory removed")

    # 79. Subprocess for Running External Commands


    result = subprocess.run(["echo", "Hello, World!"], capture_output=True, text=True)
    print("Subprocess output:", result.stdout)

    # 80. Timeit for Performance Measurement


    execution_time = timeit.timeit("sum(range(1000))", number=1000)
    print("Execution time:", execution_time)

    # 81. CProfile for Profiling


    def example_function():
        sum(range(1000))

    cProfile.run("example_function()")

    # 82. Pdb for Debugging


    def buggy_function():
        x = 1
        y = 2
        pdb.set_trace()
        z = x + y
        print(z)

    buggy_function()

    # 83. Signal Handling


    def handle_signal(signum, frame):
        print("Signal received:", signum)

    signal.signal(signal.SIGINT, handle_signal)
    print("Press Ctrl+C to trigger signal handler")
    signal.pause()

    # 84. Threading Timer

    def print_message():
        print("Timer expired")

    timer = threading.Timer(5.0, print_message)
    timer.start()

    # 85. WeakSet


    class MyClass:
        pass

    obj = MyClass()
    weak_set = weakref.WeakSet()
    weak_set.add(obj)
    print("WeakSet contains obj:", obj in weak_set)
    del obj
    print("WeakSet contains obj after deletion:", obj in weak_set)

    # 86. Enum with Auto Values


    class AutoEnum(Enum):
        FIRST = auto()
        SECOND = auto()
        THIRD = auto()

    print("AutoEnum values:", list(AutoEnum))

    # 87. Dataclass with Default Values

    @dataclass
    class Person:
        name: str
        age: int = 30

    person = Person(name="Alice")
    print("Dataclass with default values:", person)

    # 88. NamedTuple with Default Values


    class Point(NamedTuple):
        x: int
        y: int
        z: int = 0

    point = Point(1, 2)
    print("NamedTuple with default values:", point)

    # 89. Contextlib for Simplified Context Managers


    @contextmanager
    def my_context():
        print("Entering context")
        yield
        print("Exiting context")

    with my_context():
        print("Inside context")

    # 90. Datetime for Date and Time Manipulation


    now = datetime.now()
    print("Current datetime:", now)
    future = now + timedelta(days=5)
    print("Future datetime:", future)

    # 91. Calendar for Date Manipulation


    cal = calendar.month(2023, 10)
    print("October 2023 calendar:\n", cal)

    # 92. Random for Random Number Generation


    print("Random integer:", random.randint(1, 10))
    print("Random float:", random.random())
    print("Random choice:", random.choice(['a', 'b', 'c']))

    # 93. Secrets for Secure Random Numbers


    print("Secure random integer:", secrets.randbelow(10))
    print("Secure random choice:", secrets.choice(['a', 'b', 'c']))

    # 94. Hashlib for Hashing


    hash_object = hashlib.sha256(b"Hello, World!")
    print("SHA-256 hash:", hash_object.hexdigest())

    # 95. Hmac for Message Authentication


    key = b"secret"
    message = b"Hello, World!"
    hmac_object = hmac.new(key, message, hashlib.sha256)
    print("HMAC:", hmac_object.hexdigest())

    # 96. Bcrypt for Password Hashing


    password = b"supersecret"
    hashed = bcrypt.hashpw(password, bcrypt.gensalt())
    print("Hashed password:", hashed)
    print("Password match:", bcrypt.checkpw(password, hashed))

    # 97. Argon2 for Password Hashing


    ph = PasswordHasher()
    hashed = ph.hash("supersecret")
    print("Hashed password:", hashed)
    print("Password match:", ph.verify(hashed, "supersecret"))

    # 98. Pytest for Testing

    def add(a, b):
        return a + b

    def test_add():
        assert add(1, 2) == 3
        assert add(-1, 1) == 0

    # 99. Unittest for Testing


    class TestAdd(unittest.TestCase):
        def test_add(self):
            self.assertEqual(add(1, 2), 3)
            self.assertEqual(add(-1, 1), 0)

    if __name__ == "__main__":
        unittest.main()

    # 100. Mock for Mocking


    mock = Mock()
    mock.return_value = "Hello, World!"
    print("Mock return value:", mock())