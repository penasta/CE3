#-------------------------------------------------------------------------------

# TLDR: A resolução da lista começa de fato a partir da linha 133. (é necessário
# executar o que está na linha 1 a 133 para funcionar o restante.)

#-------------------------------------------------------------------------------

# 0.0 - Do autor

# Universidade de Brasília - Unb
# Instituto de ciências exatas - IE
# Departamento de Estatística - EST
# Tópicos em estatística 1 - Dados massivos (CE3)
# Prof. Dr. Guilherme Rodrigues
# Lista 2
# Aluno: Bruno Gondim toledo | Matrícula: 15/0167636
# 1/2022 | 16/08/2022
# github do projeto: https://github.com/penasta/CE3
# email do autor: bruno.gondim@aluno.unb.br

#-------------------------------------------------------------------------------

# 0.1 - Do software 

# linguagem de programação utilizada: R (versão 4.2.1).
# IDE utilizada: RStudio Desktop 2022.07.1+554
# Pacotes R utilizados: 
# installr,tidyverse,vroom,data.table,geobr,RSQLite,mongolite,sparklyr,microbenchmark

if (!require("pacman")) install.packages("pacman")
# p_load(installr)
# updateR()

# Irei utilizar o pacote pacman para instalar e carregar os pacotes no
# decorrer do código.
# Optarei por fazer o carregamento dos pacotes no decorrer do código, conforme
# a necessidade. Com isso, viso deixar mais claro a partir de qual etapa é
# necessário cada um dos pacotes utilizados.

#-------------------------------------------------------------------------------

# 0.2: Do hardware

# Estou utilizando uma máquina cujo processador têm 16 threads. Esse será
# meu default nos comandos que explicitarei o número de thread à serem utilizadas.
# caso queira utilizar um número diferente de threads, basta trocar o número
# pelo desejado no comando abaixo

threads <- 16

# Especificações da máquina utilizada para fazer e rodar o código:

# Lenovo Ideapad 3
# Windows 11
# CPU AMD Ryzen 7 5700u (16 threads)
# RAM 20(16 + 4)GB DDR4 3200 Mhz
# GPU AMD Radeon Vega 8 (Integrada)
# 256 GB SSD M.2 2242 PCIe NVMe

#-------------------------------------------------------------------------------

# Por vezes, mesmo fazendo seleção de colunas e filtragem de linhas, o tamanho 
# final da tabela extrapola oespaço disponível na memória RAM. Nesses casos, 
# precisamos realizar as operações de manipulação fora do R, em um banco de dados
# ou em um sistema de armazenamento distribuído. Outas vezes, os dados já estão 
# armazenados em algum servidor/cluster e queremos carregar para o R parte dele, 
# possivelmenteapós algumas manipulações.

# Nessa lista repetiremos parte do que fizemos na Lista 1.  
# Se desejar, use o gabarito da Lista 1 em substituição à sua própria solução 
# dos respectivos itens.

#-------------------------------------------------------------------------------

# Questão 1: Criando bancos de dados.

# a)Crie um banco de dados SQLite e adicione as tabelas consideradas no item 
# 2a) da Lista 1.

#-------------------------------------------------------------------------------

# L1-1e) Carregue para o R todos os arquivos da pasta de uma única vez 
# (usando apenas um comando R, sem métodos iterativos).

#-------------------------------------------------------------------------------

# L1-2a) Repita o procedimento do item 1e) agora mantendo, durante a leitura, 
# apenas as 3 primeiras colunas. Use o pacote `geobr` para obter os dados sobre 
# as regiões de saúde do Brasil. Junte (join) os dados da base de vacinações 
# com o das regiões de saúde.

#-------------------------------------------------------------------------------
# Começando gerando a tabela da L1-2a)
p_load(tidyverse,vroom,data.table,geobr)

rs <- read_health_region(
  year = 2013,
  macro = FALSE,
  simplified = FALSE,
  showProgress = TRUE
  )

index <- fread("./index.csv",select=(4:5),nThread = threads)

pasta_arquivos <- "./dados/"
nomes_arquivos <- list.files(pasta_arquivos)
nomes_arquivos <- str_c(pasta_arquivos, nomes_arquivos)

df <- nomes_arquivos %>%
  map(fread,
      drop=c(4:17,19:28,30:32),
      nThread=threads) %>%
  rbindlist()

colnames(df)[4] <- "regiao ibge"
df$`regiao ibge` <- as.factor(df$`regiao ibge`)
colnames(index) <- c("regiao ibge","regiao saude")
index$`regiao ibge`<- as.factor(index$`regiao ibge`)
index$`regiao saude` <- as.factor(index$`regiao saude`)
df <- merge(df,index, by="regiao ibge")
colnames(rs)[1] <- "regiao saude"
rs$`regiao saude` <- as.factor(rs$`regiao saude`)
junto <- merge(df, rs, by = "regiao saude")
junto <- subset(junto, vacina_descricao_dose == "2ª Dose")
rm(df,index,rs)
df <- junto
rm(junto)
df <- df[,1:10]

# Removendo espaços nos nomes das colunas pois está atrapalhando lá na frente
# a manipulação em SQL
colnames(df)[1:2] <- c("regiao_saude","regiao_ibge")

#-------------------------------------------------------------------------------
# Criando a DataBase (NÃO INCLUIR ESTA PARTE NO MARKDOWN)
p_load(RSQLite)
SQL <- dbConnect(RSQLite::SQLite(), "dfDB.db")

# Criando a tabela dentro da database

dbWriteTable(SQL, "df_dataDB", df)

# Error: Can only bind lists of raw vectors (or NULL)
# A coluna geometria impede o comando de funcionar. Removerei então essa coluna

dbListTables(SQL)

#-------------------------------------------------------------------------------

# L2-1b) Refaça as operações descritas no item 2b) da Lista 1 executando códigos
# sql diretamente no banco de dados criado no item a). Ao final, importe a 
# tabela resultante para o R. 

#-------------------------------------------------------------------------------

# L1-2b) No datatable obtido no item a), crie as variáveis descritas abaixo 
# considerando apenas os pacientes registrados para a segunda dose:

# 1. Quantidade de vacinados por região de saúde;
# 2. Condicionalmente, a faixa de vacinação por região de saúde (alta ou baixa, 
# em relação à mediana da distribuição de vacinações). 

# Crie uma tabela com as 5 regiões de saúde com menos vacinados em cada 
# faixa de vacinação.

#-------------------------------------------------------------------------------

# 1. Quantidade de vacinados por região de saúde;
qvrs <- dbGetQuery(SQL,"
                   SELECT regiao_saude,
                   COUNT(*) AS 'quantidade'
                   FROM df_dataDB
                   GROUP BY regiao_saude
                   ORDER BY quantidade DESC
                   ;")

dbWriteTable(SQL, "qvrs", qvrs)
dbListTables(SQL)
#-------------------------------------------------------------------------------

# 2. Condicionalmente, a faixa de vacinação por região de saúde (alta ou baixa, 
# em relação à mediana da distribuição de vacinações). 
# Aparentemente o SQL não tem uma função built-in para mediana. 
# Calculando a mediana usando SQL

#-------------------------------------------------------------------------------

dbGetQuery(SQL, 'SELECT * FROM qvrs LIMIT 5')

mediana <- dbGetQuery(SQL,"
                      SELECT AVG(quantidade) AS 'mediana'
                      FROM (
                      SELECT quantidade
                      FROM qvrs
                      ORDER BY quantidade
                      LIMIT 2
                      OFFSET (SELECT (COUNT(*) - 1) / 2
                      FROM qvrs))
                      ;")

mediana <- as.numeric(mediana)

dbExecute(SQL,
          'ALTER TABLE qvrs
          ADD faixa_de_vacinacao varchar AS
          (CASE WHEN quantidade > 2957 THEN "Alto" ELSE "Baixo" END)')

baixo <- dbGetQuery(SQL, 'SELECT * FROM qvrs 
           ORDER BY quantidade ASC', n = 5)

alto <- dbGetQuery(SQL, 'SELECT * FROM qvrs
           WHERE faixa_de_vacinacao = "Alto"
           ORDER BY quantidade ASC', n = 5)

tabelasql <- rbind(alto,baixo)
tabelasql

rm(alto,baixo,qvrs)

#-------------------------------------------------------------------------------

# c) Refaça os itens a) e b), agora com um banco de dados MongoDB.

#-------------------------------------------------------------------------------
# a)Crie um banco de dados MongoDB e adicione as tabelas consideradas no item 2a) da Lista 1.
# Criando o banco MongoDB.
p_load(mongolite)

mongodf <- mongo(collection = "mongo_df",
            db = "tab",
            url ="mongodb://localhost")

#adicionando as tabelas consideradas no item 2a) da Lista 1.

mongodf$insert(df)

#-------------------------------------------------------------------------------

# b)Refaça as operações descritas no item 2b) da Lista 1 executando códigos
# mongo diretamente no banco de dados criado no itema). Ao final, importe a 
# tabela resultante para o R.

# L1-2b) No datatable obtido no item a), crie as variáveis descritas abaixo 
# considerando apenas os pacientes registrados para a segunda dose:

# 1. Quantidade de vacinados por região de saúde;
# 2. Condicionalmente, a faixa de vacinação por região de saúde (alta ou baixa, 
# em relação à mediana da distribuição de vacinações). 

# Crie uma tabela com as 5 regiões de saúde com menos vacinados em cada 
# faixa de vacinação.

#-------------------------------------------------------------------------------
# Testes

# mongodf$find()
# mongodf$count('{}')
#mongodf$count('{regiao_saude}:')
#Error: Invalid JSON object: {regiao_saude}
#mongodf$count('{"regiao_saude"}:')
##Error: Invalid JSON object: {"regiao_saude"}
#mongodf$count("regiao_saude")
#Error: Invalid JSON object: regiao_saude 
#mongodf$count('{"regiao_saude": ""}')



mongoqvrs <- mongodf$aggregate('[{"$group": {"_id":"$regiao_saude","n": {"$sum":1}}}]')

mongodf$insert(mongoqvrs)


# Tentando calcular a mediana
#teste <- mongoqvrs$run(command = '[{count = db.coll.count();,db.coll.find().sort( {"a":1} ).skip(count / 2 - 1).limit(1)}]')
#teste

#db._id.find().sort( {"n":1} ).skip(db._id.count() / 2).limit(1);
#mongoqvrs.find().sort( {"n":1} ).skip(db.teams.count() / 2).limit(1);
#count = db.coll.count();
#mongodf$find().sort( {"n":1} ).skip(count / 2 - 1).limit(1);

#mongodf$find().sort( {"n":1} ).skip(count() / 2).limit(1);

# não consegui!!

rm(mediana,mongodf,mongoqvrs,SQL)

#-------------------------------------------------------------------------------

# d) Refaça os itens c), agora usando o Apache Spark.

#-------------------------------------------------------------------------------
# a) Crie um banco de dados Apache Spark e adicione as tabelas consideradas
# no item 2a) da Lista 1.
p_load(sparklyr)

config <- spark_config()
config$`sparklyr.shell.driver-memory` <- "4G"
config$`sparklyr.shell.executor-memory` <- "4G"
config$spark.yarn.executor.memoryOverhead <- "1g"

sc <- spark_connect(master = "local", config = config)

sparkdf <- copy_to(sc, df)
# spark_web(sc)
#-------------------------------------------------------------------------------

# b) Refaça as operações descritas no item 2b) da Lista 1 executando códigos
# Apache Spark diretamente no banco de dados criado no itema). Ao final, 
# importe a tabela resultante para o R.

# L1-2b) No datatable obtido no item a), crie as variáveis descritas abaixo 
# considerando apenas os pacientes registrados para a segunda dose:

# 1. Quantidade de vacinados por região de saúde;
# 2. Condicionalmente, a faixa de vacinação por região de saúde (alta ou baixa, 
# em relação à mediana da distribuição de vacinações). 

# Crie uma tabela com as 5 regiões de saúde com menos vacinados em cada 
# faixa de vacinação.

#-------------------------------------------------------------------------------

sparktable <- sparkdf %>%
  count(regiao_saude) %>%
  show_query()%>%
  collect()

sparktable
mediana <- median(sparktable$n)
# não consigo colocar todas as transformações em uma unica sequencia de pipes.
# portanto terei que ficar coletando, levando pro spark, fazer a operação e repetir.

# Testando antes rodar no R

tabela <- sparktable %>%
  mutate(faixa_vacinacao = ifelse (sparktable$n >= median(sparktable$n),"alto","baixo"))
tabela<-tabela[order(tabela$n),]
tabela <- rbind(tabela[1:5,],tabela[102:106,])

# Funciona

# Agora mandando para rodar no Spark...

sparktable <- copy_to(sc, sparktable,overwrite = TRUE)

teste <- sparktable %>%
  mutate(faixa_vacinacao = ifelse (sparktable$n >= median(sparktable$n),"alto","baixo"))%>%
  show_query()%>%
  collect()

# Não funciona, não mostra a query e não coleta (?????)

tabelaspark <- copy_to(sc, tabela,overwrite = TRUE)

#-------------------------------------------------------------------------------

# e) Compare o tempo de processamento das 3 abordagens (SQLite, MongoDB e Spark),
# desde o envio do comando sql até o recebimento dos resultados no R. Comente os
# resultados incluindo na análise os resultados obtidos no item 2d) da Lista 1.

# Cuidado: A performance pode ser completamente diferente em outros cenários 
# (com outras operações,diferentes tamanhos de tabelas, entre outros aspectos).

#-------------------------------------------------------------------------------
# Comparando o tempo de processamento
p_load(microbenchmark)

# não seria justo falar de comparação entre os itens visto que somente na parte
# de realizar as operações em SQL eu consegui fazer o procedimento por inteiro.
# Portanto, o que dá para apresentar é o microbenchmark desse item, comparado
# ao microbenchmark do equivalente na lista 1.

# mbmsql <- microbenchmark({

SQL <- dbConnect(RSQLite::SQLite(), "dfDB.db")

dbWriteTable(SQL, "df_dataDB", df,overwrite=T)

qvrs <- dbGetQuery(SQL,"
                   SELECT regiao_saude,
                   COUNT(*) AS 'quantidade'
                   FROM df_dataDB
                   GROUP BY regiao_saude
                   ORDER BY quantidade DESC
                   ;")

dbWriteTable(SQL, "qvrs", qvrs,overwrite=T)

dbGetQuery(SQL, 'SELECT * FROM qvrs LIMIT 5')

mediana <- dbGetQuery(SQL,"
                      SELECT AVG(quantidade) AS 'mediana'
                      FROM (
                      SELECT quantidade
                      FROM qvrs
                      ORDER BY quantidade
                      LIMIT 2
                      OFFSET (SELECT (COUNT(*) - 1) / 2
                      FROM qvrs))
                      ;")

mediana <- as.numeric(mediana)

dbExecute(SQL,
          'ALTER TABLE qvrs
          ADD faixa_de_vacinacao varchar AS
          (CASE WHEN quantidade > 2957 THEN "Alto" ELSE "Baixo" END)')

baixo <- dbGetQuery(SQL, 'SELECT * FROM qvrs 
           ORDER BY quantidade ASC', n = 5)

alto <- dbGetQuery(SQL, 'SELECT * FROM qvrs
           WHERE faixa_de_vacinacao = "Alto"
           ORDER BY quantidade ASC', n = 5)

tabelasql <- rbind(alto,baixo)

rm(alto,baixo,qvrs,SQL,tabelasql,mediana)

})

# Resultados:

# mbmsql$expr <- NA
# mbmsql$expr <- "SQLite"

# saveRDS(mbmsql,file="mbmsql.rds")

mbml1 <- readRDS('mbm.rds')

# mbmsql <- readRDS('mbmsql.rds')

mbm <- rbind(mbml1,mbmsql)

autoplot(mbm)

rm(mbml1,mbmsql)

# saveRDS(mbm,file="mbml2.rds")

#-------------------------------------------------------------------------------

# Comentários dos resultados

# Comparando o microbenchmark do SQL com os microbenchmarks do DTplyr e Dplyr (lista 1),
# percebemos que o SQLite aparentemente foi mais lento que o DTplyr e mais rápido que
# o Dplyr, porém foi o mais consistente nos tempos de execução.

#-------------------------------------------------------------------------------