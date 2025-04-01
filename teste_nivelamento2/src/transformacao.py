import requests
import pandas as pd
import pdfplumber
import logging
import zipfile
import os

# logging caminho
logging.basicConfig(filename='./logs/transformacao.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def baixar_pdf(url, nome_arquivo, pasta_pdf):
    """Baixa o PDF da URL fornecida e salva na pasta especificada."""
    caminho_completo = os.path.join(pasta_pdf, nome_arquivo)
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(caminho_completo, 'wb') as f:
            f.write(response.content)
        logging.info(f'PDF baixado com sucesso: {caminho_completo}')
        print(f"PDF baixado com sucesso: {caminho_completo}")
        return caminho_completo
    except requests.exceptions.RequestException as e:
        logging.error(f'Falha ao baixar PDF: {e}')
        print(f"Falha ao baixar PDF: {e}")
        return None

def extrair_dados_pdf(pdf_path, pages='all'):
    """Extrai todas as tabelas do PDF usando pdfplumber."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            print("PDF aberto com sucesso.")
            if pages == 'all':
                pages_to_process = pdf.pages
            else:
                pages_to_process = [pdf.pages[i - 1] for i in pages]
            dfs = []
            for page in pages_to_process:
                print(f"Extraindo tabelas da página {page.page_number}...")
                tables = page.extract_tables(table_settings={"vertical_strategy": "lines",
                                                               "horizontal_strategy": "lines",
                                                               "intersection_tolerance": 5})
                if tables:
                    for table in tables:
                        df = pd.DataFrame(table[1:], columns=table[0])
                        dfs.append(df)
            if dfs:
                print("Tabelas extraídas com sucesso.")
                return pd.concat(dfs, ignore_index=True)
            else:
                logging.warning(f'Nenhuma tabela encontrada em {pdf_path}')
                print(f'Nenhuma tabela encontrada em {pdf_path}')
                return None
    except Exception as e:
        logging.error(f'Erro ao extrair dados de {pdf_path}: {e}')
        print(f"Erro ao extrair dados de {pdf_path}: {e}")
        return None

def transformar_dados(df):
    """Transforma os dados para o formato desejado."""
    if df is not None and not df.empty:
        print("Transformando dados...")
        
        df = df.rename(columns={
            'OD': 'Obrigatoriedade da Diretriz',
            'AMB': 'Abrangência'
        })
        print("Dados transformados com sucesso.")
        return df
    else:
        logging.warning('DataFrame vazio ou nulo ao transformar dados.')
        print('DataFrame vazio ou nulo ao transformar dados.')
        return pd.DataFrame()

def salvar_csv(df, csv_path, pasta_csv):
    """Salva os dados em um arquivo CSV na pasta especificada."""
    caminho_completo = os.path.join(pasta_csv, csv_path)
    if not df.empty:
        print(f"Salvando dados em {caminho_completo}...")
        df.to_csv(caminho_completo, index=False)
        logging.info(f'Dados salvos em {caminho_completo}')
        print(f"Dados salvos em {caminho_completo}")
        return caminho_completo
    else:
        logging.warning(f'DataFrame vazio, nenhum dado salvo em {caminho_completo}')
        print(f'DataFrame vazio, nenhum dado salvo em {caminho_completo}')
        return None

def compactar_arquivos(arquivos, nome_zip, pasta_zip):
    """Compacta todos os arquivos em um arquivo ZIP na pasta especificada."""
    caminho_completo = os.path.join(pasta_zip, nome_zip)
    try:
        with zipfile.ZipFile(caminho_completo, 'w') as zipf:
            for arquivo in arquivos:
                zipf.write(arquivo, os.path.basename(arquivo))
        print(f"Arquivos compactados com sucesso em {caminho_completo}")
        return caminho_completo
    except Exception as e:
        logging.error(f'Erro ao compactar arquivos: {e}')
        print(f"Erro ao compactar arquivos: {e}")
        return None

# URL do PDF
url_pdf = 'https://www.gov.br/ans/pt-br/acesso-a-informacao/participacao-da-sociedade/atualizacao-do-rol-de-procedimentos/Anexo_I_Rol_2021RN_465.2021_RN627L.2024.pdf'
nome_arquivo_pdf = 'Anexo_I_Rol_2021RN_465.2021_RN627L.2024.pdf'
nome_arquivo_csv = 'dados_transformados.csv'
nome_arquivo_zip = 'dados_compactados.zip'

# Pastas --> arquivos
pasta_pdf = './pdfs'
pasta_csv = './csvs'
pasta_zip = './zips'

# Baixar PDF
caminho_pdf = baixar_pdf(url_pdf, nome_arquivo_pdf, pasta_pdf)
if caminho_pdf:
    # Extraindo os dados
    df = extrair_dados_pdf(caminho_pdf, pages=range(1, 182))
    if df is not None:
        # Transforma os dados
        df_transformado = transformar_dados(df)
        # Salvar CSV
        caminho_csv = salvar_csv(df_transformado, nome_arquivo_csv, pasta_csv)
        if caminho_csv:
            # Compacta o arquivo
            compactar_arquivos([caminho_csv, caminho_pdf], nome_arquivo_zip, pasta_zip)
    else:
        print('Nenhum dado extraído do PDF.')
else:
    print('Falha ao baixar o PDF.')