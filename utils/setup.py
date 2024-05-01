import os

directories = [
    "./data-localized",
    "./data-localized-processed",
    "./data-raw",
    "./data-raw-processed",
    "./data-source",
    "./logs",
]

for directory in directories:
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Directory {directory} created")
    else:
        print(f"Directory {directory} already exists")
