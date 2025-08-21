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

# A directory to store compressed log files temporarily
COMPRESSED_LOG_DIRECTORY = './tmp'
# A directory for the output data
DATA_DIRECTORY = './data'
# The pickle file to save the data dictionary
MODELO_DE_URNA_PICKLE = os.path.join(DATA_DIRECTORY, 'modelo_de_urna.pickle')

# The URL for downloading the election data archives
# https://dadosabertos.tse.jus.br/dataset/resultados-2022-arquivos-transmitidos-para-totalizacao
FILE_PATTERN_ARQUIVO_TRANSM = re.compile('bu_imgbu_logjez_rdv_vscmr_2022_(\d)t_([A-Z]{2}).zip')
ZIPPED_LOG_FILENAME_PATTERN = re.compile('.*(\d{5})(\d{4})(\d{4}).logjez')

# Constants for data dictionary keys
ID_SECAO = 'ID_SECAO'
CD_MUNICIPIO = 'CD_MUNICIPIO'
NR_ZONA = 'NR_ZONA'
NR_SECAO = 'NR_SECAO'
MODELO_URNA_TEMPLATE = 'MODELO_URNA_%sT'
SE_UE2020 = 'SE_UE2020'

# Regex patterns to parse log file content
MODELO_URNA_PATTERN = re.compile('Modelo de Urna: (\w+)')
CD_MUNICIPIO_PATTERN = re.compile('Município: (\w+)')
NR_ZONA_PATTERN = re.compile('Zona Eleitoral: (\w+)')
NR_SECAO_PATTERN = re.compile('ão Eleitoral: (\w+)')

# List of polling rounds and Brazilian states to process
LISTA_TURNO_UFS = [
    #(1, 'AC'), (1, 'AL'), (1, 'AM'), (1, 'AP'), (1, 'BA'), (1, 'CE'),
    (1, 'DF'), (1, 'ES'),
    (1, 'GO'), (1, 'MA'), (1, 'MG'), (1, 'MS'), (1, 'MT'), (1, 'PA'), (1, 'PB'), (1, 'PE'),
    (1, 'PI'), (1, 'PR'), (1, 'RJ'), (1, 'RN'), (1, 'RO'), (1, 'RR'), (2, 'SP'), (1, 'RS'),
    (1, 'SC'), (1, 'SE'), (1, 'SP'), (1, 'TO'), (2, 'AC'), (2, 'AL'), (2, 'AM'), (2, 'AP'),
    (2, 'BA'), (2, 'CE'), (2, 'DF'), (2, 'ES'), (2, 'GO'), (2, 'MA'), (2, 'MG'), (2, 'MS'),
    (2, 'MT'), (2, 'PA'), (2, 'PB'), (2, 'PE'), (2, 'PI'), (2, 'PR'), (2, 'RJ'), (2, 'RN'),
    (2, 'RO'), (2, 'RR'), (2, 'SC'), (2, 'SE'), (2, 'TO'), (2, 'RS'),
]

# Create directories if they don't exist
if not os.path.exists(COMPRESSED_LOG_DIRECTORY):
    os.makedirs(COMPRESSED_LOG_DIRECTORY)
if not os.path.exists(DATA_DIRECTORY):
    os.makedirs(DATA_DIRECTORY)


def GetModeloUrnaFromLogFile(file_path):
    """
    Parses a log file to find the voting machine model.
    """
    with codecs.open(file_path, encoding='iso-8859-15') as f:
        content = f.read()
        content = content.encode('UTF-8').decode()
        return MODELO_URNA_PATTERN.search(content).groups()[0]


def LoadDataDict():
    """
    Loads the data dictionary from a pickle file.
    """
    with open(MODELO_DE_URNA_PICKLE, 'rb') as f:
        return pickle.load(f)


def DumpDataDict(data):
    """
    Saves the data dictionary to a pickle file.
    """
    with open(MODELO_DE_URNA_PICKLE, 'wb') as f:
        return pickle.dump(data, f)


def LoadModeloUrnasDataFrame():
    """
    Loads the data from the pickle file into a Pandas DataFrame.
    """
    with open(MODELO_DE_URNA_PICKLE, 'rb') as file:
        data = pickle.load(file)

    df = pd.DataFrame(data.values(), index=data.keys())
    df.index.names = [ID_SECAO]

    # Use the 2nd round model if available, otherwise the 1st
    df['MODELO_URNA'] = df.MODELO_URNA_2T.combine_first(df.MODELO_URNA_1T)
    df['SE_UE2020'] = [i == 'UE2020' for i in df.MODELO_URNA_2T]
    df = df.drop(['ID_SECAO', 'NR_ZONA', 'NR_SECAO'], axis=1)

    return df


def download_chunk(url, file_path, start, end, chunk_num):
    """
    Downloads a specific byte range of a file and saves it to a temporary chunk file.
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
        print(f"Error downloading chunk {chunk_num}: {e}")
        # In a real-world app, you'd want more robust retry logic here.


def download_file_with_multiple_connections(url, filename, num_connections=5):
    """
    Downloads a file using multiple connections.
    """
    print(f'Starting multi-connection download for {url}')
    try:
        # Get the total file size from the server
        response = requests.head(url)
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))

        if total_size == 0:
            print(f"Error: Could not determine file size for {url}")
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
                    print(f"Error: Chunk {i} was not downloaded successfully.")
                    return False

        print('Download complete.')
        return True

    except requests.exceptions.RequestException as e:
        print(f"Error during multi-connection download of {url}: {e}")
        return False


def Main():
    """
    Main function to orchestrate the download and processing of log files.
    """
    plain_log_file_path = os.path.join(COMPRESSED_LOG_DIRECTORY, 'logd.dat')

    try:
        data = LoadDataDict()
    except FileNotFoundError:
        data = {}

    for turno, uf in LISTA_TURNO_UFS:
        url = 'https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2022/arqurnatot/bu_imgbu_logjez_rdv_vscmr_2022_%dt_%s.zip' % (
            turno, uf)
        filename = os.path.join(COMPRESSED_LOG_DIRECTORY, url.split('/')[-1])

        # Use the new multi-connection download function
        if download_file_with_multiple_connections(url, filename):
            print('Processing %s' % filename)
            try:
                zip_file = zipfile.ZipFile(filename)
                for zip_internal_file in zip_file.filelist:
                    if zip_internal_file.filename.endswith('logjez'):
                        zipped_log_file = zip_file.extract(zip_internal_file, path=COMPRESSED_LOG_DIRECTORY)
                        zipped_log_file_ref = py7zr.SevenZipFile(zipped_log_file, mode='r')
                        all_files = zipped_log_file_ref.getnames()
                        zipped_log_file_ref.extractall(COMPRESSED_LOG_DIRECTORY)
                        zipped_log_file_ref.close()

                        # Extract metadata from the filename
                        cd_municipio, nr_zona, nr_secao = \
                            [int(i) for i in ZIPPED_LOG_FILENAME_PATTERN.match(zipped_log_file).groups()]

                        os.remove(zipped_log_file)

                        section_id = '%s_%s_%s_%s' % (
                            uf,
                            str(cd_municipio),
                            str(nr_zona),
                            str(nr_secao)
                        )

                        print('\t Processing %s' % section_id)

                        if section_id in data:
                            section_data = data[section_id]
                        else:
                            section_data = {
                                ID_SECAO: section_id,
                                CD_MUNICIPIO: cd_municipio,
                                NR_ZONA: nr_zona,
                                NR_SECAO: nr_secao
                            }

                        try:
                            log_modelo_urna = GetModeloUrnaFromLogFile(plain_log_file_path)
                        except:
                            log_modelo_urna = 'None'
                            print('Error: Could not parse voting machine model for section %s' % section_id)

                        if log_modelo_urna == 'UE2020':
                            section_data[SE_UE2020] = True
                        elif log_modelo_urna in ['UE2010', 'UE2015', 'UE2009', 'UE2011', 'UE2013']:
                            section_data[SE_UE2020] = False
                        else:
                            print('Erro: parsing modelo urna', section_data)

                        section_data[MODELO_URNA_TEMPLATE % turno] = log_modelo_urna
                        data[section_id] = section_data

                        for log_file in all_files:
                            os.remove(os.path.join(COMPRESSED_LOG_DIRECTORY, log_file))

                zip_file.close()
                os.remove(filename)

                DumpDataDict(data)

            except zipfile.BadZipFile:
                print(f"Error: {filename} is a corrupted ZIP file. Skipping.")
                if os.path.exists(filename):
                    os.remove(filename)
            except Exception as e:
                print(f"An unexpected error occurred during processing: {e}")
                if os.path.exists(filename):
                    os.remove(filename)


if __name__ == '__main__':
    Main()
