import os
import json
import fnmatch


def read_file(path_to_file: str) -> dict:
    """
    Lee el contenido de el archivo que se encuentra en la ruta especificada

    Args:
        path_to_file (str): ruta del archivo que se quiere leer

    Returns:
        dict: El archivo deserializado en un diccionario Python
    """
    
    with open(path_to_file, 'r') as f:
        file = json.load(f)
    return file

def write_file(path_to_file: str, file_to_write: dict) -> None:
    """escribe el contenido de file_to_write en el archivo especificado path_to_file

    Args:
        path_to_file (str): Ubicacion del archivo sobre el que se quiere escribir
        file_to_write (dict): El diccionario que contiene la informacion que se escribira en el archivo

    Returns:
        None
    """
    with open(path_to_file, 'w') as f:
	    json.dump(file_to_write, f, indent=4)
    
def find_files(pattern: str, path: str, raise_exception=False) -> list:
    """"""
    """
    Busca archivos .ORC o .JSON en la ruta especificada y sus sub directorios
    con un patron en particular

    Args:
        pattern (str): Los archivos encontrados tendran este patron
        path (str): Se realizara la busqueda de los archivos y directorios con el patron
                    especÃ­ficado, en esta ruta 
        raise_exception (bool, optional): Define si se lanza un error cuando el retorno es una lista vacia. Defaults to False.

    Returns:
        list (str): Esta lista contiene las rutas de los archivos que 
                    hicieron match con el patron
    """
    
    result = []
    
    for root, _, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    
    if len(result) == 0 and raise_exception:
        raise FileNotFoundError("Ningun archivo(s) con el patron {} ubicado en {} encontrado(s)".format(pattern, path))
    return result