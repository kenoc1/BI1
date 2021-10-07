import csv
import pathlib
import numpy as np


def save_f2_to_comb_id_allocation_to_file(connections: list, file_name: str) -> None:
    np_connection_array = np.array(connections)
    np.savetxt(file_name, np_connection_array, delimiter=',')


def save_f2_to_comb_id_allocation_to_csv(header: list, rows: list[list], filename: str) -> None:
    with open(filename, 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        if len(header) > 0:
            writer.writerow(header)
        if len(rows) > 0:
            writer.writerows(rows)


def write_to_csv(header: list, rows: list[list], filepath: str) -> None:
    with open(filepath, 'a+', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        if len(header) > 0:
            writer.writerow(header)
        if len(rows) > 0:
            writer.writerows(rows)


def write_to_csv_with_path(header: list, rows: list[list], filepath: pathlib.Path) -> None:
    # if not filepath.exists():
    #     filepath.touch()
    with filepath.open('a+', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        if len(header) > 0:
            writer.writerow(header)
        if len(rows) > 0:
            writer.writerows(rows)
