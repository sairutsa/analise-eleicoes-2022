import zipfile
import pandas as pd
import os
from urllib import request
import pickle
from pyUFbr.baseuf import ufbr

# --- Directory and File Constants ---
DATA_DIRECTORY = './data'
BOLETINS_DIRECTORY = os.path.join(DATA_DIRECTORY, 'boletins_de_urna')
DADOS_BUS_FILE = os.path.join(DATA_DIRECTORY, 'dados_bus.pickle')

# --- DataFrame Column Constants ---
ID_SECAO = 'ID_SECAO'
NR_ZONA = 'NR_ZONA'
QT_BOLSO_2T = 'QT_BOLSO_2T'
QT_LULA_2T = 'QT_LULA_2T'
QT_VAL_PRESI_2T = 'QT_VAL_PRESI_2T'
UF = 'UF'

# --- TSE Data Constants ---
URL_TEMPLATE_2T = 'https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2022/buweb/bweb_2t_%s_311020221535.zip'
ELEICAO_PRESIDENTE = 1
CANDIDATO_LULA = 13
CANDIDATO_BOLSONARO = 22
COLUMNS_TO_USE = [
    'NR_TURNO',
    'CD_ELEICAO',
    'DS_ELEICAO',
    'SG_UF',
    'CD_MUNICIPIO',
    'NR_ZONA',
    'NR_SECAO',
    'CD_CARGO_PERGUNTA',
    'DS_CARGO_PERGUNTA',
    'NR_VOTAVEL',
    'QT_VOTOS',
]
VOTO_BRANCO = 95
VOTO_NULO = 96

def ParseChunk(chunk, data):
    """
    Parses a chunk of the vote data DataFrame and aggregates the results.

    This function iterates through a smaller piece of the full dataset,
    calculating vote totals for presidential candidates in the 2nd round
    for each polling section.

    Args:
        chunk (pd.DataFrame): A piece of the full DataFrame to be processed.
        data (dict): A dictionary to store the aggregated vote data, with
                     polling section IDs as keys.
    """
    for i, row in chunk.iterrows():
        # Create a unique ID for each polling section using its location data.
        id_secao = '%s_%s_%s_%s' %(
            row.SG_UF,
            row.CD_MUNICIPIO,
            row.NR_ZONA,
            row.NR_SECAO,
        )

        # If the section is already in our dictionary, use the existing entry.
        # Otherwise, create a new dictionary for this section.
        if id_secao in data:
            secao_data = data[id_secao]
        else:
            secao_data = {
                ID_SECAO : id_secao,
                UF : row.SG_UF,
                NR_ZONA : row.NR_ZONA,
                QT_BOLSO_2T : 0,
                QT_LULA_2T : 0,
                QT_VAL_PRESI_2T : 0,
            }
            data[id_secao] = secao_data

        nr_votavel = row.NR_VOTAVEL
        
        # Process only votes for President in the 2nd round.
        if row.CD_CARGO_PERGUNTA == ELEICAO_PRESIDENTE:
            if row.NR_TURNO == 2:
                # Aggregate votes for each candidate.
                if nr_votavel == CANDIDATO_LULA:
                    secao_data[QT_LULA_2T] = row.QT_VOTOS
                elif nr_votavel == CANDIDATO_BOLSONARO:
                    secao_data[QT_BOLSO_2T] = row.QT_VOTOS

                # Sum all votes that are not blank or null to get the total valid votes.
                if not nr_votavel in (VOTO_BRANCO, VOTO_NULO):
                    secao_data[QT_VAL_PRESI_2T] += row.QT_VOTOS


def LoadCSV(data, filename):
    """
    Loads a single CSV file of vote data in chunks and processes it.

    Args:
        data (dict): The dictionary to populate with aggregated vote data.
        filename (str): The full path to the CSV file to load.
    """
    # Reading the file in chunks is memory-efficient for large files.
    for chunk in pd.read_csv(
            filename,
            usecols=COLUMNS_TO_USE,
            encoding='iso-8859-15',
            delimiter=';',
            chunksize=10000,
            on_bad_lines='skip'
        ):
            ParseChunk(chunk, data)


def SaveData(data):
    """Saves the aggregated data dictionary to a pickle file."""
    with open(DADOS_BUS_FILE, 'wb') as f:
        pickle.dump(data, f)


def LoadVotosDataFrame():
    """
    Loads the aggregated data from the pickle file into a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the vote data, indexed by
                      polling section ID.
    """
    with open(DADOS_BUS_FILE, 'rb') as f:
        data = pickle.load(f)
        # Convert the dictionary of dictionaries into a DataFrame.
        df = pd.DataFrame(data.values(), index=data.keys())
        # The dictionary keys become the DataFrame index.
        df.index.names = ['ID_SECAO']
        # The 'ID_SECAO' column is now redundant because it's the index, so we drop it.
        df = df.drop(['ID_SECAO'], axis=1)
        return df


def DownloadBoletins():
    """
    Downloads the zipped ballot files (Boletins de Urna) for all Brazilian states (UFs).
    """
    # Ensure the target directory exists.
    os.makedirs(BOLETINS_DIRECTORY, exist_ok=True)
    
    for uf in ufbr.list_uf:
        url = URL_TEMPLATE_2T % uf
        filename = os.path.join(BOLETINS_DIRECTORY, url.split('/')[-1])

        if os.path.exists(filename):
            print(f"File already exists, skipping download: {filename}")
            continue

        print('Downloading ', url)
        request.urlretrieve(url, filename)

def ExtractBoletins():
    """Extracts all zipped ballot files in the target directory."""
    print('Extraindo BUs')
    for file in os.listdir(BOLETINS_DIRECTORY):
        if file.endswith('.zip'):
            print('Extraindo %s' %file)
            ref = zipfile.ZipFile(os.path.join(BOLETINS_DIRECTORY, file))
            ref.extractall(BOLETINS_DIRECTORY)
            ref.close()

def main():
    """
    Main execution function to download, extract, and process election data.
    """
    # Step 1: Download the raw data files from the TSE website.
    # Uncomment the line below to run the download.
    DownloadBoletins()

    # Step 2: Extract the .zip files to get the .csv files.
    # Uncomment the line below to run the extraction.
    ExtractBoletins()

    # Step 3: Process all CSV files and aggregate the data.
    data = {}
    print('Processando arquivos')
    for filename in os.listdir(BOLETINS_DIRECTORY):
        if filename.endswith('.csv'):
            print('Processando ', filename)
            LoadCSV(data, os.path.join(BOLETINS_DIRECTORY, filename))
            # Save progress after each file in case the script is interrupted.
            SaveData(data)

if __name__ == '__main__':
    main()