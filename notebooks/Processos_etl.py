# Databricks notebook source
# MAGIC %md
# MAGIC #### Conferindo se os dados foram montados e se temos acesso a pasta inbound

# COMMAND ----------

dbutils.fs.ls('/mnt/dados/inbound')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lendo os dados na camada de inbound

# COMMAND ----------

file_path = 'dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json'
data = spark.read.json(file_path)


# COMMAND ----------

# Vizualindo a base de dados
display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Removendo colunas

# COMMAND ----------

dados_anuncios = data.drop('imagens', 'usuario')
display(dados_anuncios)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Criando uma coluna de identificação

# COMMAND ----------

# Importando biblioteca 
from pyspark.sql.functions import col

# COMMAND ----------

df_bronze = dados_anuncios.withColumn('id', col('anuncio.id'))
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Salvando na camada bronze

# COMMAND ----------

df_bronze.write.format("delta").mode('Overwrite').save('dbfs:/mnt/dados/bronze/dataset_imoveis')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze to silve

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lendo dados da camada bronze

# COMMAND ----------

dbutils.fs.ls("/mnt/dados/bronze")

# COMMAND ----------

# Criando um data frame spark do arquivo que esta na camada bronze  
df_silver = spark.read.format('delta').load('dbfs:/mnt/dados/bronze/dataset_imoveis/')

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformando os campos do json em colunas

# COMMAND ----------

display(df_silver.select('anuncio.*'))

# COMMAND ----------

# Abrindo coluna endereço
display(df_silver.select('anuncio.*', 'anuncio.endereco.*'))

# COMMAND ----------

dados_silver = df_silver.select('anuncio.*', 'anuncio.endereco.*')
display(dados_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Removendo colunas

# COMMAND ----------

df_silver_new = (dados_silver.drop('caracteristicas', 'endereco'))
display(df_silver_new)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Salvando na camada silver

# COMMAND ----------

df_silver_new.write.format('delta').mode('overwrite').save('/mnt/dados/silver/dataset_imoveis/')
