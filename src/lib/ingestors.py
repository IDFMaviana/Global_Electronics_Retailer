import re
import delta
class importfromcsv:
    def __init__(self, config):
        # Desempacota os parâmetros do dicionário para os atributos da classe
        self.catalog = config.get("catalog")
        self.schema = config.get("schema")
        self.volume = config.get("volume")
        self.data_format = config.get("data_format")
        self.dest_catalog = config.get("dest_catalog")
        self.dest_format = config.get("dest_format")
        self.dest_schema = config.get("dest_schema")
        # Verifica se algum valor está ausente
        if not all([self.catalog, self.schema, self.volume, self.data_format, self.dest_catalog, self.dest_format, self.dest_schema]):
            raise ValueError("Faltam parâmetros necessários no dicionário de configuração.")
    def limpa_caracteres(self, col):
        # Limpa caracteres especiais, substituindo-os por underscores
        return re.sub(r'[:;,\{\}\(\)\n\t=\s]', '_', col)
    #Define os arquivos no diretorio
    def set_files(self):
        # Lista os arquivos do diretório específico no DBFS (Databricks File System)
        files = dbutils.fs.ls(f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/")
        return files
    
    #Carrega arquivos
    def load_data(self, nm_arquivo):
        # Carrega o arquivo especificado utilizando o formato de dados desejado
        file_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{nm_arquivo}"
        # Tente carregar o arquivo com o formato especificado
        df = (spark.read
                   .format(self.data_format)
                   .option("header", "true")
                   .load(file_path))
        return df
    
    #Salva arquivos no formato e diretorio de destino
    def save_data(self, df, nm_arquivo):
        # Salva o DataFrame no formato e diretório de destino
        df.write.format(self.dest_format).mode("overwrite").save(f"/Volumes/{self.dest_catalog}/{self.dest_schema}/{nm_arquivo}")
    
    #limpa diretorios de log do spark    
    def clear_log(self,nm_arquivo):
        caminho_destino = f"/Volumes/{self.dest_catalog}/{self.dest_schema}/{nm_arquivo}/"
        arquivos_temp = dbutils.fs.ls(caminho_destino)
        # Filtra e exclui arquivos que começam com _started_ ou _committed_
        for arquivo in arquivos_temp:
            if arquivo.name.startswith("_started_") or arquivo.name.startswith("_committed_") or arquivo.name.startswith("_SUCCESS")        :
                dbutils.fs.rm(arquivo.path)
    #Executa
    def execute(self):
        # Executa a ingestão dos arquivos no diretório configurado
        files = self.set_files()  # Obtém a lista de arquivos
        for file in files:
            nm_arquivo = file.name  # Nome do arquivo
            df = self.load_data(nm_arquivo)  # Carrega o arquivo

            # Limpa os nomes das colunas
            nm_nova_colunas = [self.limpa_caracteres(col) for col in df.columns]
            df_colunas = df.toDF(*nm_nova_colunas)  # Renomeia as colunas

            # Remove a extensão '.csv' do nome do arquivo
            nm_arquivo = nm_arquivo.replace(".csv", "")

            # Salva os dados processados
            self.save_data(df_colunas, nm_arquivo)
            #limpa diretorio de log do spark
            self.clear_log(nm_arquivo)

            #print(f"Arquivo {nm_arquivo} processado com sucesso!")

class importfromraw:
    def __init__(self, config):
        # Desempacota os parâmetros do dicionário para os atributos da classe
        self.catalog = config.get("catalog")
        self.schema = config.get("schema")
        #self.volume = config.get("volume")
        self.data_format = config.get("data_format")
        self.dest_catalog = config.get("dest_catalog")
        self.dest_format = config.get("dest_format")
        self.dest_schema = config.get("dest_schema")
        self.ref_path = config.get("ref_path")
    
    def set_files(self):
        # Lista os arquivos do diretório
        files = dbutils.fs.ls(self.ref_path)
        return files
    
    def load_data(self, nm_arquivo):
        file_path = f"/Volumes/{self.catalog}/{self.schema}/{nm_arquivo}"
        df = spark.read.format(self.data_format).load(file_path)
        return df
    def save_data(self, df, nm_arquivo):
        df.write.format(self.dest_format).mode("overwrite").saveAsTable(f"{self.dest_catalog}.{self.dest_schema}.{nm_arquivo}")
    #Executa
    def execute(self):
        files = self.set_files()  # Obtém a lista de arquivos
        for file in files:
            #remove a extensão original do arquivo
            nm_arquivo = file.name.replace(".csv","")
            #carrega o arquivo no formato parquet
            df = self.load_data(nm_arquivo)  # Carrega o arquivo
            #Salva o arquivo no formato delta na camada bronze
            self.save_data(df,nm_arquivo)