import os
import sys
import glob
import time
import zipfile
import pandas as pd
import sqlite3
import sqlalchemy
import dask.dataframe as dd

class CNPJDatabaseBuilder:
    def __init__(self, input_folder, output_folder, db_name='cnpj.db', delete_unzipped_files=True):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.db_path = os.path.join(self.output_folder, db_name)
        self.delete_unzipped_files = delete_unzipped_files
        self.engine = None
        self.engine_url = None
        self.data_reference = None
        
    def check_and_prepare_output(self):
        """Checks if the output database already exists and prepares the output directory."""
        if os.path.exists(self.db_path):
            print(f'The database file {self.db_path} already exists. Please delete it before running this script again.')
            sys.exit()
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
    
    def unzip_files(self):
        """Unzips all zip files in the input folder to the output folder."""
        zip_files = glob.glob(os.path.join(self.input_folder, '*.zip'))
        if len(zip_files) != 37:
            response = input(f'The folder {self.input_folder} should contain 37 zip files, but found {len(zip_files)}. Do you want to proceed anyway? (y/n) ')
            if response.lower() != 'y':
                print('Please ensure all required zip files are in the input folder.')
                sys.exit()
        print('Starting to unzip files:', time.asctime())
        for zip_file in zip_files:
            print(f'Unzipping {zip_file}')
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(self.output_folder)
        print('Finished unzipping files:', time.asctime())
    
    def connect_to_database(self):
        """Connects to the SQLite database."""
        self.engine = sqlite3.connect(self.db_path)
        self.engine_url = f'sqlite:///{self.db_path}'
    
    def disconnect_database(self):
        """Closes the connection to the SQLite database."""
        if self.engine:
            self.engine.commit()
            self.engine.close()
    
    def get_data_reference(self):
        """Extracts the data reference date from one of the files."""
        emp_files = glob.glob(os.path.join(self.output_folder, '*.EMPRECSV'))
        if emp_files:
            data_ref_str = os.path.basename(emp_files[0]).split('.')[2]
            if len(data_ref_str) == len('D30610') and data_ref_str.startswith('D'):
                self.data_reference = f"{data_ref_str[4:6]}/{data_ref_str[2:4]}/202{data_ref_str[1]}"
            else:
                self.data_reference = 'Unknown'
        else:
            self.data_reference = 'Unknown'
    
    def load_code_tables(self):
        """Loads smaller code tables into the database and creates indexes."""
        code_files = {
            '.CNAECSV': 'cnae',
            '.MOTICSV': 'motivo',
            '.MUNICCSV': 'municipio',
            '.NATJUCSV': 'natureza_juridica',
            '.PAISCSV': 'pais',
            '.QUALSCSV': 'qualificacao_socio'
        }
        for extension, table_name in code_files.items():
            file_path = glob.glob(os.path.join(self.output_folder, f'*{extension}'))
            if file_path:
                file_path = file_path[0]
                print(f'Loading code table from {file_path} into {table_name}')
                df = pd.read_csv(file_path, sep=';', encoding='latin1', header=None, names=['codigo','descricao'], dtype=str)
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                self.engine.execute(f'CREATE INDEX idx_{table_name} ON {table_name}(codigo);')
                if self.delete_unzipped_files:
                    print(f'Deleting file {file_path}')
                    os.remove(file_path)
            else:
                print(f'File with extension {extension} not found.')
    
    def create_database_tables(self):
        """Defines the schemas and creates the main tables in the database."""
        # Define the schema for the main tables
        self.colunas_empresas = ['cnpj_basico', 'razao_social',
               'natureza_juridica',
               'qualificacao_responsavel',
               'capital_social_str',
               'porte_empresa',
               'ente_federativo_responsavel']
        self.colunas_estabelecimento = ['cnpj_basico','cnpj_ordem', 'cnpj_dv','matriz_filial', 
                  'nome_fantasia',
                  'situacao_cadastral','data_situacao_cadastral', 
                  'motivo_situacao_cadastral',
                  'nome_cidade_exterior',
                  'pais',
                  'data_inicio_atividades',
                  'cnae_fiscal',
                  'cnae_fiscal_secundaria',
                  'tipo_logradouro',
                  'logradouro', 
                  'numero',
                  'complemento','bairro',
                  'cep','uf','municipio',
                  'ddd1', 'telefone1',
                  'ddd2', 'telefone2',
                  'ddd_fax', 'fax',
                  'correio_eletronico',
                  'situacao_especial',
                  'data_situacao_especial']
        self.colunas_socios = [
                'cnpj_basico',
                'identificador_de_socio',
                'nome_socio',
                'cnpj_cpf_socio',
                'qualificacao_socio',
                'data_entrada_sociedade',
                'pais',
                'representante_legal',
                'nome_representante',
                'qualificacao_representante_legal',
                'faixa_etaria'
              ]
        self.colunas_simples = [
            'cnpj_basico',
            'opcao_simples',
            'data_opcao_simples',
            'data_exclusao_simples',
            'opcao_mei',
            'data_opcao_mei',
            'data_exclusao_mei']
        # Create the tables in the database
        self.create_table('empresas', self.colunas_empresas)
        self.create_table('estabelecimento', self.colunas_estabelecimento)
        self.create_table('socios_original', self.colunas_socios)
        self.create_table('simples', self.colunas_simples)
    
    def create_table(self, table_name, columns):
        """Creates a table in the database with the specified columns."""
        columns_sql = ',\n'.join([f'"{col}" TEXT' for col in columns])
        sql = f'CREATE TABLE "{table_name}" (\n{columns_sql}\n);'
        self.engine.execute(sql)
    
    def load_large_tables(self):
        """Loads the larger tables into the database using Dask for efficiency."""
        self.load_large_table('empresas', '.EMPRECSV', self.colunas_empresas)
        self.load_large_table('estabelecimento', '.ESTABELE', self.colunas_estabelecimento)
        self.load_large_table('socios_original', '.SOCIOCSV', self.colunas_socios)
        self.load_large_table('simples', '.SIMPLES.CSV.*', self.colunas_simples)
    
    def load_large_table(self, table_name, file_extension, columns):
        """Loads data from CSV files into the specified table."""
        file_paths = glob.glob(os.path.join(self.output_folder, f'*{file_extension}'))
        for file_path in file_paths:
            print(f'Loading data from {file_path} into table {table_name}')
            ddf = dd.read_csv(file_path, sep=';', header=None, names=columns, encoding='latin1', dtype=str, na_filter=False)
            ddf.to_sql(table_name, self.engine_url, if_exists='append', index=False, dtype=sqlalchemy.types.Text)
            if self.delete_unzipped_files:
                print(f'Deleting file {file_path}')
                os.remove(file_path)
    
    def adjust_tables_and_create_indexes(self):
        """Performs adjustments on tables and creates necessary indexes."""
        sql_commands = [
            # Adjust capital_social
            'ALTER TABLE empresas ADD COLUMN capital_social REAL;',
            "UPDATE empresas SET capital_social = CAST(REPLACE(capital_social_str, ',', '.') AS REAL);",
            'ALTER TABLE empresas DROP COLUMN capital_social_str;',
            # Create cnpj field in estabelecimento
            'ALTER TABLE estabelecimento ADD COLUMN cnpj TEXT;',
            "UPDATE estabelecimento SET cnpj = cnpj_basico || cnpj_ordem || cnpj_dv;",
            # Create indexes
            'CREATE INDEX idx_empresas_cnpj_basico ON empresas(cnpj_basico);',
            'CREATE INDEX idx_empresas_razao_social ON empresas(razao_social);',
            'CREATE INDEX idx_estabelecimento_cnpj_basico ON estabelecimento(cnpj_basico);',
            'CREATE INDEX idx_estabelecimento_cnpj ON estabelecimento(cnpj);',
            'CREATE INDEX idx_estabelecimento_nomefantasia ON estabelecimento(nome_fantasia);',
            'CREATE INDEX idx_socios_original_cnpj_basico ON socios_original(cnpj_basico);',
            # Create socios table
            '''
            CREATE TABLE socios AS 
            SELECT te.cnpj AS cnpj, ts.*
            FROM socios_original ts
            LEFT JOIN estabelecimento te ON te.cnpj_basico = ts.cnpj_basico
            WHERE te.matriz_filial = '1';
            ''',
            'DROP TABLE IF EXISTS socios_original;',
            # Indexes on socios
            'CREATE INDEX idx_socios_cnpj ON socios(cnpj);',
            'CREATE INDEX idx_socios_cnpj_cpf_socio ON socios(cnpj_cpf_socio);',
            'CREATE INDEX idx_socios_nome_socio ON socios(nome_socio);',
            'CREATE INDEX idx_socios_representante ON socios(representante_legal);',
            'CREATE INDEX idx_socios_representante_nome ON socios(nome_representante);',
            # Index on simples
            'CREATE INDEX idx_simples_cnpj_basico ON simples(cnpj_basico);',
            # Create reference table
            '''
            CREATE TABLE "_referencia" (
                "referencia" TEXT,
                "valor" TEXT
            );
            '''
        ]
        print('Adjusting tables and creating indexes:', time.asctime())
        for sql in sql_commands:
            print(f'Executing SQL command: {sql}')
            self.engine.executescript(sql)
        print('Finished adjusting tables and creating indexes:', time.asctime())
    
    def insert_reference_data(self):
        """Inserts reference data into the database."""
        qtde_cnpjs = self.engine.execute('SELECT COUNT(*) FROM estabelecimento;').fetchone()[0]
        self.engine.execute('INSERT INTO _referencia (referencia, valor) VALUES (?, ?);', ('CNPJ', self.data_reference))
        self.engine.execute('INSERT INTO _referencia (referencia, valor) VALUES (?, ?);', ('cnpj_qtde', str(qtde_cnpjs)))
    
    def cleanup(self):
        """Cleans up resources and closes the database connection."""
        self.disconnect_database()
        print(f'Database created at {self.db_path}')
        print('Process completed:', time.asctime())
    
    def build_database(self):
        """Orchestrates the steps to build the database."""
        self.check_and_prepare_output()
        self.unzip_files()
        self.connect_to_database()
        self.get_data_reference()
        self.load_code_tables()
        self.create_database_tables()
        self.load_large_tables()
        self.adjust_tables_and_create_indexes()
        self.insert_reference_data()
        self.cleanup()