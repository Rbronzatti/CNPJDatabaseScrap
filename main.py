from src.ExtractUrl import ExtractUrl
from src.DownloadFiles import DownloadFiles
from src.CNPJDatabaseBuilder import CNPJDatabaseBuilder

if __name__ == "__main__":
    # # Base URL of the main directory
    # base_url = "https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/"

    # # Initialize ExtractUrl to get the latest month's URL
    # extractor = ExtractUrl(base_url)

    try:
        # latest_month_url = extractor.get_latest_month_url()
        # print(f"Latest month URL: {latest_month_url}")

        # # Initialize DownloadFiles to download the ZIP files with parallelism
        # downloader = DownloadFiles(output_dir='output', max_workers=5)

        # # Download all ZIP files from the latest month's URL
        # downloader.download_zip_files(latest_month_url)

        # After downloading, build the database using CNPJDatabaseBuilder
        # Set the input_folder to 'output' where the ZIP files are stored
        # Set the output_folder to 'data' where the unzipped files and database will be stored
        builder = CNPJDatabaseBuilder(input_folder='output', output_folder='data')
        builder.build_database()

    except ValueError as ve:
        print(f"ValueError: {ve}")
    except Exception as ex:
        print(f"An error occurred: {ex}")