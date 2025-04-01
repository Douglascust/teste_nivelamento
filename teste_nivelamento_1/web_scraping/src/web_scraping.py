import requests
from bs4 import BeautifulSoup
import os
import zipfile
import configparser
import logging
import asyncio
import aiohttp
import threading

# logging caminho
logging.basicConfig(filename='./web_scraping/logs/web_scraping.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Leitura do arquivo de configuração
config = configparser.ConfigParser()
config.read('./web_scraping/config/config.ini')

# Configurações do web scraping
url_anexos = config['web_scraping']['url']
pasta_destino = config['web_scraping']['pasta_destino']
arquivo_zip = config['web_scraping']['arquivo_zip']

async def baixar_anexo_pdf_async(session, anexo_url, anexo_nome):
    """Baixa um arquivo PDF de forma assíncrona."""
    try:
        async with session.get(anexo_url) as response:
            if response.status == 200:
                with open(anexo_nome, 'wb') as f:
                    f.write(await response.read())
                print(f'Anexo PDF baixado: {anexo_nome}')
                logging.info(f'Anexo PDF baixado: {anexo_nome}')
            else:
                print(f'Falha ao baixar anexo PDF: {anexo_url}')
                logging.error(f'Falha ao baixar anexo PDF: {anexo_url}')
    except Exception as e:
        print(f'Erro ao baixar anexo PDF: {anexo_url} - {e}')
        logging.error(f'Erro ao baixar anexo PDF: {anexo_url} - {e}')

async def baixar_anexos_pdf_async(url, pasta_destino, anexos_pdf):
    """Baixa os arquivos PDF de forma assíncrona."""
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for anexo_url in anexos_pdf:
            anexo_nome = os.path.join(pasta_destino, anexo_url.split('/')[-1])
            tasks.append(baixar_anexo_pdf_async(session, anexo_url, anexo_nome))
        await asyncio.gather(*tasks)

def baixar_anexos_pdf(url, pasta_destino):
    """Baixa apenas os arquivos PDF dos Anexos I e II da URL fornecida."""
    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f'Falha ao acessar a URL: {url}')
        return

    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', href=True)

    anexos_pdf = []
    for link in links:
        href = link['href']
        if 'Anexo_I' in href and href.endswith('.pdf') or \
           'Anexo_II' in href and href.endswith('.pdf'):
            anexos_pdf.append(href if href.startswith('http') else url + href)

    asyncio.run(baixar_anexos_pdf_async(url, pasta_destino, anexos_pdf))

def compactar_anexos_pdf(pasta_anexos, arquivo_zip):
    """Compacta apenas os arquivos PDF na pasta de anexos em um arquivo ZIP."""
    arquivos_pdf = [arquivo for arquivo in os.listdir(pasta_anexos) if arquivo.endswith('.pdf')]
    threads = []

    def compactar_arquivo(caminho_arquivo, arquivo_zip, arquivo_nome):
        """Função para compactar um arquivo individual."""
        try:
            with zipfile.ZipFile(arquivo_zip, 'a') as zipf:  # Use 'a' para adicionar arquivos
                zipf.write(caminho_arquivo, arquivo_nome)
        except Exception as e:
            logging.error(f"Erro ao compactar {caminho_arquivo}: {e}")

    for arquivo in arquivos_pdf:
        caminho_arquivo = os.path.join(pasta_anexos, arquivo)
        thread = threading.Thread(target=compactar_arquivo, args=(caminho_arquivo, arquivo_zip, arquivo))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print(f'Arquivos PDF compactados em: {arquivo_zip}')
    logging.info(f'Anexos PDF compactados em: {arquivo_zip}')

# Baixar os anexos PDF
baixar_anexos_pdf(url_anexos, pasta_destino)

# Compactar os anexos PDF
compactar_anexos_pdf(pasta_destino, arquivo_zip)