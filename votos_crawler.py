import zipfile

import pandas as pd
import codecs
import urllib, os, csv
from urllib import request

DATA_DIRECTORY = './data'
DADOS_BUS_FILE = os.path.join(DATA_DIRECTORY, 'dados_bus.pickle')

ID_SECAO = 'ID_SECAO'
NR_ZONA = 'NR_ZONA'
QT_BOLSO_2T = 'QT_BOLSO_2T'
QT_LULA_2T = 'QT_LULA_2T'
QT_VAL_PRESI_2T = 'QT_VAL_PRESI_2T'


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

CD_ELEICAO_ORDINARIA = 2 # CD_TIPO_ELEICAO

UF = 'UF'

ELEICAO_PRESIDENTE = 1


BOLETINS_DIRECTORY = '/home/ribeiro/Code/e2022/bus/'

# ID_SECAO  QT_BOLSO_1T    QT_LULA_1T   QT_VAL_PRESI_1T QT_DPT_22  QT_BOLSO_2T QT_LULA_2T QT_VAL_PRESI_2T
def ParseChunk(chunk, data):


    for i, row in chunk.iterrows():
        id_secao = '%s_%s_%s_%s' %(
            row.SG_UF,
            row.CD_MUNICIPIO,
            row.NR_ZONA,
            row.NR_SECAO,
        )

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

        if row.CD_CARGO_PERGUNTA == ELEICAO_PRESIDENTE:
            if row.NR_TURNO == 2:
                if nr_votavel == 13:
                    secao_data[QT_LULA_2T] = row.QT_VOTOS

                elif nr_votavel == 22:
                    secao_data[QT_BOLSO_2T] = row.QT_VOTOS

                if not nr_votavel in (VOTO_BRANCO, VOTO_NULO):
                    secao_data[QT_VAL_PRESI_2T] += row.QT_VOTOS


def LoadCSV(data, filename):
    for chunk in pd.read_csv(
            filename,
            usecols=COLUMNS_TO_USE,
            encoding='iso-8859-15',
            delimiter=';',
            #quoting=csv.QUOTE_NONE,
            chunksize=10000,
            on_bad_lines='skip'
        ):
            ParseChunk(chunk, data)


def SaveData(data):
    import pickle
    with open(DADOS_BUS_FILE, 'wb') as f:
        pickle.dump(data, f)


def LoadVotosDataFrame():
    import pickle
    with open(DADOS_BUS_FILE, 'rb') as f:
        data = pickle.load(f)
        df = pd.DataFrame(data.values(), index=data.keys())
        df.index.names = ['ID_SECAO']
        df = df.drop(['ID_SECAO'], axis=1)
        return df


def DownloadBoletins():
    from pyUFbr.baseuf import ufbr

    URL_TEMPLAT_2T = 'https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2022/buweb/bweb_2t_%s_311020221535.zip'

    for uf in ufbr.list_uf:
        url = URL_TEMPLAT_2T %uf
        filename = os.path.join(BOLETINS_DIRECTORY, url.split('/')[-1])

        print('Downloading ', url)
        request.urlretrieve(url, filename)

def ExtractBoletins():
    print('Extraindo BUs')
    for file in os.listdir(BOLETINS_DIRECTORY):
        if file.endswith('.zip'):
            print('Extraindo %s' %file)
            ref = zipfile.ZipFile(os.path.join(BOLETINS_DIRECTORY, file))
            ref.extractall(BOLETINS_DIRECTORY)
            ref.close()

if __name__ == '__main__':
    #DownloadBoletins()

    #ExtractBoletins()

    data = {}

    print('Processando arquivos')
    for filename in os.listdir(BOLETINS_DIRECTORY):
    #for filename in ('/home/ribeiro/Code/e2022/bus/bweb_2t_RO_311020221535.csv',):
        if filename.endswith('.csv'):
            print('Processando ', filename)
            LoadCSV(data, os.path.join(BOLETINS_DIRECTORY, filename))
            SaveData(data)