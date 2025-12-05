import os
import psycopg2
import pandas as pd
from pathlib import Path


class PostgreSQLImporter:
    def __init__(self, host="localhost", port=5432, database="postgres", 
                 user="postgres", password="postgres"):
        """Inicializa a conexão com PostgreSQL"""
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Estabelece conexão com o banco de dados"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            print(f"✓ Conectado ao PostgreSQL em {self.host}:{self.port}")
        except psycopg2.Error as e:
            print(f"✗ Erro ao conectar: {e}")
            raise
    
    def disconnect(self):
        """Fecha a conexão com o banco de dados"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("✓ Desconectado do PostgreSQL")
    
    def create_table_from_dataframe(self, table_name, df):
        """Cria tabela no PostgreSQL baseado no DataFrame"""
        # Mapear tipos de dados pandas para PostgreSQL
        type_mapping = {
            'int64': 'BIGINT',
            'int32': 'INTEGER',
            'float64': 'DECIMAL(15, 4)',
            'object': 'TEXT',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP'
        }
        
        # Construir SQL CREATE TABLE
        columns = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            pg_type = type_mapping.get(dtype, 'TEXT')
            # Sanitizar nome da coluna - converter para string primeiro
            col_name = str(col).lower().replace(' ', '_').replace('-', '_').replace('.', '_')
            columns.append(f'"{col_name}" {pg_type}')
        
        create_table_sql = f"""
        DROP TABLE IF EXISTS "{table_name}";
        CREATE TABLE "{table_name}" (
            id SERIAL PRIMARY KEY,
            {', '.join(columns)}
        );
        """
        
        try:
            self.cursor.execute(create_table_sql)
            self.connection.commit()
            print(f"✓ Tabela '{table_name}' criada com sucesso")
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"✗ Erro ao criar tabela: {e}")
            raise
    
    def insert_data_from_dataframe(self, table_name, df):
        """Insere dados do DataFrame na tabela"""
        try:
            # Sanitizar nomes das colunas uma vez - converter para string primeiro
            columns = [str(col).lower().replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f"""
            INSERT INTO "{table_name}" ({', '.join([f'"{col}"' for col in columns])})
            VALUES ({placeholders})
            """
            
            # Inserir cada linha
            for _, row in df.iterrows():
                # Converter valores, tratando NaN e None
                values = []
                for val in row:
                    if pd.isna(val):
                        values.append(None)
                    else:
                        values.append(val)
                self.cursor.execute(insert_sql, tuple(values))
            
            self.connection.commit()
            print(f"✓ {len(df)} registros inseridos em '{table_name}'")
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"✗ Erro ao inserir dados: {e}")
            raise
    
    def import_file(self, file_path, table_name=None):
        """Importa um arquivo (CSV ou XLSX) para o PostgreSQL"""
        try:
            file_path = Path(file_path)
            file_extension = file_path.suffix.lower()
            
            # Gerar nome da tabela se não fornecido
            if table_name is None:
                file_name = file_path.stem  # Nome sem extensão
                table_name = f"TAB_{file_name.upper()}"
            
            # Ler arquivo baseado na extensão
            print(f"→ Lendo arquivo: {file_path.name}")
            
            if file_extension == '.csv':
                df = pd.read_csv(file_path)
            elif file_extension == '.xlsx':
                df = pd.read_excel(file_path)
            else:
                print(f"✗ Formato de arquivo não suportado: {file_extension}")
                return False
            
            print(f"→ Linhas encontradas: {len(df)}")
            
            # Criar tabela
            self.create_table_from_dataframe(table_name, df)
            
            # Inserir dados
            self.insert_data_from_dataframe(table_name, df)
            
            return True
        except Exception as e:
            print(f"✗ Erro ao importar {file_path}: {e}")
            return False
    
    def import_csv(self, csv_path, table_name=None):
        """Importa um arquivo CSV para o PostgreSQL (compatibilidade)"""
        return self.import_file(csv_path, table_name)
    
    def drop_all_tables(self):
        """Apaga todas as tabelas do banco de dados"""
        try:
            # Obter lista de todas as tabelas
            self.cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            tables = self.cursor.fetchall()
            
            if not tables:
                print("✓ Nenhuma tabela encontrada para apagar")
                return
            
            print(f"\n{'='*60}")
            print(f"Apagando {len(tables)} tabela(s)...")
            print(f"{'='*60}\n")
            
            # Desabilitar referência de chaves estrangeiras
            self.cursor.execute("SET session_replication_role = 'replica'")
            
            # Apagar todas as tabelas
            for table in tables:
                table_name = table[0]
                self.cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                print(f"✓ Tabela '{table_name}' apagada")
            
            # Reabilitar referência de chaves estrangeiras
            self.cursor.execute("SET session_replication_role = 'origin'")
            self.connection.commit()
            
            print(f"\n{'='*60}")
            print(f"✓ Todas as {len(tables)} tabela(s) foram apagadas com sucesso")
            print(f"{'='*60}\n")
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"✗ Erro ao apagar tabelas: {e}")
            raise
    
    def import_all_files_from_folder(self, folder_path):
        """Importa todos os arquivos (CSV e XLSX) de uma pasta"""
        folder = Path(folder_path)
        
        if not folder.exists():
            print(f"✗ Pasta não encontrada: {folder_path}")
            return
        
        # Buscar arquivos CSV e XLSX
        csv_files = list(folder.glob("*.csv"))
        xlsx_files = list(folder.glob("*.xlsx"))
        all_files = csv_files + xlsx_files
        
        if not all_files:
            print(f"✗ Nenhum arquivo CSV ou XLSX encontrado em: {folder_path}")
            return
        
        print(f"\n{'='*60}")
        print(f"Iniciando importação de {len(all_files)} arquivo(s)")
        print(f"  - {len(csv_files)} arquivo(s) CSV")
        print(f"  - {len(xlsx_files)} arquivo(s) XLSX")
        print(f"{'='*60}\n")
        
        success_count = 0
        for file in all_files:
            print(f"\nProcessando: {file.name}")
            if self.import_file(str(file)):
                success_count += 1
        
        print(f"\n{'='*60}")
        print(f"Importação concluída: {success_count}/{len(all_files)} arquivo(s) importados com sucesso")
        print(f"{'='*60}\n")
    
    def import_all_csv_from_folder(self, folder_path):
        """Importa todos os arquivos CSV de uma pasta (compatibilidade)"""
        return self.import_all_files_from_folder(folder_path)


def main(drop_tables=False):
    """Função principal
    
    Args:
        drop_tables (bool): Se True, apaga todas as tabelas antes de importar. Padrão: False
    """
    # Configurações do PostgreSQL (do docker-compose)
    importer = PostgreSQLImporter(
        host="localhost",
        port=5432,
        database="postgres",
        user="postgres",
        password="postgres"
    )
    
    try:
        importer.connect()
        
        # Apagar tabelas se solicitado
        if drop_tables:
            importer.drop_all_tables()
        
        # Importar todos os arquivos (CSV e XLSX) da pasta src
        # Ir para a pasta pai (PowerBIStudy) e depois para src
        src_folder = Path(__file__).parent.parent / "src"
        importer.import_all_files_from_folder(src_folder)
        
    finally:
        importer.disconnect()


if __name__ == "__main__":
    main(drop_tables=True)


# DROP ALL TABLLES
# python -c "from src.import_source import PostgreSQLImporter; i = PostgreSQLImporter(); i.connect(); i.drop_all_tables(); i.disconnect()"