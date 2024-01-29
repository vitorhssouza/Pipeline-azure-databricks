# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze to silve

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

# Salvando em uma variavel 
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
