import numpy as np


def save_f2_to_comb_id_allocation_to_file(connections: list, file_name: str) -> None:
    np_connection_array = np.array(connections)
    np.savetxt(file_name, np_connection_array, delimiter=',')
