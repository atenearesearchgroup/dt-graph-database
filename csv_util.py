import csv
import os


def open_file(path: str):
    return open(path, 'r', newline='')


def find_csv_files(dir: str, extension: str):
    result = []
    for file in os.listdir(dir):
        if file.endswith(extension):
            result.append(file)
    return result


def get_reader(filepath: str, delim: str):
    file = open(filepath, 'r', newline='')
    reader = csv.reader(file, delimiter=delim)
    return file, reader


def get_writer(filepath: str, delim: str):
    file = open(filepath, 'w', newline='')
    writer = csv.writer(file, delimiter=delim, dialect='excel')
    return file, writer
