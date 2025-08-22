"""
This script downloads, extracts, and parses election data from the Brazilian
Superior Electoral Court (TSE) to determine the model of the voting machine
used in each polling section for the 2022 elections.

It handles a complex, multi-layered archive format (ZIP -> 7z -> log) and
uses multi-threaded downloads for efficiency. The final processed data,
mapping each polling section to a machine model, is saved to a pickle file.
"""
import os
import zipfile
import py7zr
import pickle
import re
import codecs
import sys
import pandas as pd
import requests
import threading
from pyUFbr.baseuf import ufbr

# --- Constants for Directories and Files ---
COMPRESSED_LOG_DIRECTORY = './tmp'
DATA_DIRECTORY = './data'
MODELO_DE_URNA_PICKLE = os.path.join(DATA_DIRECTORY, 'modelo_de_urna.pickle')

# --- Constants for TSE Data and URLs ---
# https://dadosabertos.tse.jus.br/dataset/resultados-2022-arquivos-transmitidos-para-totalizacao
URL_TEMPLATE = 'https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2022/arqurnatot/bu_imgbu_logjez_rdv_vscmr_2022_%dt_%s.zip'
ZIPPED_LOG_FILENAME_PATTERN = re.compile('.*(\d{5})(\d{4})(\d{4}).logjez')

# Constants for data dictionary keys
ID_SECAO = 'ID_SECAO'
CD_MUNICIPIO = 'CD_MUNICIPIO'
NR_ZONA = 'NR_ZONA'
NR_SECAO = 'NR_SECAO'
MODELO_URNA_TEMPLATE = 'MODELO_URNA_%sT'
SE_UE2020 = 'SE_UE2020'

# --- Regex Patterns for Parsing Log Files ---
MODELO_URNA_PATTERN = re.compile('Modelo de Urna: (\w+)')
CD_MUNICIPIO_PATTERN = re.compile('Município: (\w+)')
NR_ZONA_PATTERN = re.compile('Zona Eleitoral: (\w+)')
NR_SECAO_PATTERN = re.compile('ão Eleitoral: (\w+)')

# --- Processing Configuration ---
# List of polling rounds (1 or 2) and Brazilian states (UFs) to process.
# This list is manually defined to control the scope and order of processing.
LISTA_TURNO_UFS = [
    (1, 'AC'), (1, 'AL'), (1, 'AM'), (1, 'AP'), (1, 'BA'), (1, 'CE'),
    (1, 'DF'), (1, 'ES'),
    (1, 'GO'), (1, 'MA'), (1, 'MG'), (1, 'MS'), (1, 'MT'), (1, 'PA'), (1, 'PB'), (1, 'PE'),
    (1, 'PI'), (1, 'PR'), (1, 'RJ'), (1, 'RN'), (1, 'RO'), (1, 'RR'), (2, 'SP'), (1, 'RS'),
    (1, 'SC'), (1, 'SE'), (1, 'SP'), (1, 'TO'), (2, 'AC'), (2, 'AL'), (2, 'AM'), (2, 'AP'),
    (2, 'BA'), (2, 'CE'), (2, 'DF'), (2, 'ES'), (2, 'GO'), (2, 'MA'), (2, 'MG'), (2, 'MS'),
    (2, 'MT'), (2, 'PA'), (2, 'PB'), (2, 'PE'), (2, 'PI'), (2, 'PR'), (2, 'RJ'), (2, 'RN'),
    (2, 'RO'), (2, 'RR'), (2, 'SC'), (2, 'SE'), (2, 'TO'), (2, 'RS'),
]

# --- Directory Setup ---
if not os.path.exists(COMPRESSED_LOG_DIRECTORY):
    os.makedirs(COMPRESSED_LOG_DIRECTORY)
if not os.path.exists(DATA_DIRECTORY):
    os.makedirs(DATA_DIRECTORY)


def GetModeloUrnaFromLogFile(file_path):
    """
    Parses a log file to find the voting machine model.

    Args:
        file_path (str): The path to the decoded log file.

    Returns:
        str: The model of the voting machine (e.g., 'UE2020') or None
             if not found.
    """
    with codecs.open(file_path, encoding='iso-8859-15') as f:
        content = f.read()
        content = content.encode('UTF-8').decode()
        return MODELO_URNA_PATTERN.search(content).groups()[0]


def LoadDataDict():
    """
    Loads the aggregated data dictionary from the pickle file.

    Returns:
        dict: The loaded data dictionary. Returns an empty dictionary
              if the file is not found.
    """
    try:
        with open(MODELO_DE_URNA_PICKLE, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        print("Pickle file not found. Starting with an empty data dictionary.")
        return {}


def DumpDataDict(data):
    """
    Saves the data dictionary to a pickle file.

    Args:
        data (dict): The data dictionary to save.
    """
    with open(MODELO_DE_URNA_PICKLE, 'wb') as f:
        pickle.dump(data, f)


def LoadModeloUrnasDataFrame():
    """
    Loads the data from the pickle file into a Pandas DataFrame.
    This function is intended to be called by other scripts like `report.py`.
    """
    with open(MODELO_DE_URNA_PICKLE, 'rb') as file:
        data = pickle.load(file)

    df = pd.DataFrame(data.values(), index=data.keys())
    df.index.names = [ID_SECAO]

    # Consolidate the machine model, prioritizing the 2nd round data.
    df['MODELO_URNA'] = df.MODELO_URNA_2T.combine_first(df.MODELO_URNA_1T)
    df['SE_UE2020'] = [i == 'UE2020' for i in df.MODELO_URNA_2T]
    df = df.drop(['ID_SECAO', 'NR_ZONA', 'NR_SECAO'], axis=1)

    return df


def download_chunk(url, file_path, start, end, chunk_num):
    """
    Downloads a specific byte range of a file and saves it to a temporary chunk file.
    This is a helper function for multi-threaded downloading.

    Args:
        url (str): The URL of the file to download.
        file_path (str): The base path where the final file will be saved.
        start (int): The starting byte of the chunk.
        end (int): The ending byte of the chunk.
        chunk_num (int): The sequential number of the chunk.
    """
    headers = {'Range': f'bytes={start}-{end}'}
    chunk_path = f"{file_path}.part{chunk_num}"
    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        with open(chunk_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Could not download chunk {chunk_num}: {e}")


def download_file_with_multiple_connections(url, filename, num_connections=5):
    """
    Downloads a file using multiple parallel connections for increased speed.

    Args:
        url (str): The URL of the file to download.
        filename (str): The path to save the final downloaded file.
        num_connections (int): The number of parallel connections to use.

    Returns:
        bool: True if the download was successful, False otherwise.
    """
    print(f'Starting multi-connection download for {url}')
    try:
        # Get the total file size from the server
        response = requests.head(url)
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))

        if total_size == 0:
            print(f"ERROR: Could not determine file size for {url}. Cannot download.")
            return False

        # Calculate chunk size and byte ranges
        chunk_size = total_size // num_connections
        threads = []

        # Download each chunk in a separate thread
        for i in range(num_connections):
            start = i * chunk_size
            end = start + chunk_size - 1
            if i == num_connections - 1:
                end = total_size - 1  # Ensure the last chunk gets the rest of the file

            thread = threading.Thread(target=download_chunk, args=(url, filename, start, end, i))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Combine the downloaded chunks into a single file
        print('Combining chunks...')
        with open(filename, 'wb') as output_file:
            for i in range(num_connections):
                chunk_path = f"{filename}.part{i}"
                if os.path.exists(chunk_path):
                    with open(chunk_path, 'rb') as chunk_file:
                        output_file.write(chunk_file.read())
                    os.remove(chunk_path)  # Clean up temporary chunk file
                else:
                    print(f"ERROR: Chunk {i} was not downloaded successfully. Aborting assembly.")
                    return False

        print('Download complete.')
        return True

    except requests.exceptions.RequestException as e:
        print(f"Error during multi-connection download of {url}: {e}")
        return False


def process_downloaded_zip(zip_filepath, uf, turno, data):
    """
    Processes a single downloaded .zip file, handling the nested archives
    to extract and parse the final log file.

    Args:
        zip_filepath (str): Path to the downloaded .zip file.
        uf (str): The Brazilian state (UF) being processed.
        turno (int): The election round (1 or 2) being processed.
        data (dict): The main data dictionary to update with parsed info.
    """
    plain_log_file_path = os.path.join(COMPRESSED_LOG_DIRECTORY, 'logd.dat')

    try:
        with zipfile.ZipFile(zip_filepath) as top_level_zip:
            for zip_info in top_level_zip.infolist():
                # The file we need is a 7-Zip archive ending in 'logjez'
                if not zip_info.filename.endswith('logjez'):
                    continue

                # --- First level of extraction (from .zip) ---
                logjez_path = top_level_zip.extract(zip_info, path=COMPRESSED_LOG_DIRECTORY)

                try:
                    # --- Second level of extraction (from .7z) ---
                    with py7zr.SevenZipFile(logjez_path, mode='r') as logjez_archive:
                        log_files_inside = logjez_archive.getnames()
                        logjez_archive.extractall(COMPRESSED_LOG_DIRECTORY)

                    # --- Parse metadata from filename and create section ID ---
                    match = ZIPPED_LOG_FILENAME_PATTERN.match(logjez_path)
                    if not match:
                        continue
                    cd_municipio, nr_zona, nr_secao = [int(i) for i in match.groups()]
                    section_id = f'{uf}_{cd_municipio}_{nr_zona}_{nr_secao}'
                    print(f'\tProcessing {section_id}')

                    # --- Get or create the data entry for this section ---
                    section_data = data.get(section_id, {
                        ID_SECAO: section_id,
                        CD_MUNICIPIO: cd_municipio,
                        NR_ZONA: nr_zona,
                        NR_SECAO: nr_secao
                    })

                    # --- Parse the final log file for the machine model ---
                    try:
                        log_modelo_urna = GetModeloUrnaFromLogFile(plain_log_file_path)
                        if log_modelo_urna == 'UE2020':
                            section_data[SE_UE2020] = True
                        elif log_modelo_urna:
                            section_data[SE_UE2020] = False
                        section_data[MODELO_URNA_TEMPLATE % turno] = log_modelo_urna
                    except Exception:
                        print(f'ERROR: Could not parse model for section {section_id}')
                        section_data[MODELO_URNA_TEMPLATE % turno] = 'None'

                    data[section_id] = section_data

                finally:
                    # --- Clean up all extracted temporary files ---
                    if os.path.exists(logjez_path):
                        os.remove(logjez_path)
                    for log_file in log_files_inside:
                        temp_file_path = os.path.join(COMPRESSED_LOG_DIRECTORY, log_file)
                        if os.path.exists(temp_file_path):
                            os.remove(temp_file_path)

    except zipfile.BadZipFile:
        print(f"ERROR: {zip_filepath} is a corrupted ZIP file. Skipping.")
    except Exception as e:
        print(f"ERROR: An unexpected error occurred while processing {zip_filepath}: {e}")
    finally:
        # Clean up the main downloaded zip file
        if os.path.exists(zip_filepath):
            os.remove(zip_filepath)


def main():
    """
    Main function to orchestrate the download and processing of log files.
    """
    data = LoadDataDict()

    for turno, uf in LISTA_TURNO_UFS:
        url = URL_TEMPLATE % (turno, uf)
        filename = os.path.join(COMPRESSED_LOG_DIRECTORY, url.split('/')[-1])

        # Download the file using multiple connections. If it fails, skip to the next.
        if not download_file_with_multiple_connections(url, filename):
            print(f"Skipping processing for {uf}-{turno}t due to download failure.")
            continue

        # Process the downloaded file and update the data dictionary.
        print(f'Processing {filename}')
        process_downloaded_zip(filename, uf, turno, data)

        # Save progress after each state/round combination.
        print("Saving progress to pickle file...")
        DumpDataDict(data)


if __name__ == '__main__':
    main()
