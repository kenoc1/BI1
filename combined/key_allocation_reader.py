from numpy import genfromtxt


def read_f2_to_comb_id_allocation_from_file(file_name: str):
    return genfromtxt(file_name, delimiter=',', dtype=None)
