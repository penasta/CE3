#-------------------------------------------------------------------------------

# TLDR: Ctrl + f --> remover ### do código todo p/ rodar pela primeira vez

#-------------------------------------------------------------------------------

# 0.0 - Do autor

# Universidade de Brasília - Unb
# Instituto de ciências exatas - IE
# Departamento de Estatística - EST
# Tópicos em estatística 1 - Dados massivos (CE3)
# Prof. Dr. Guilherme Rodrigues
# Lista 3
# Aluno: Bruno Gondim toledo | Matrícula: 15/0167636
# 1/2022 | 28/08/2022
# github do projeto: https://github.com/penasta/CE3
# email do autor: bruno.gondim@aluno.unb.br

#-------------------------------------------------------------------------------

# 0.1 - Do software 

# linguagem de programação utilizada: R (versão 4.2.1).
# IDE utilizada: RStudio Desktop 2022.07.1+554
# Pacotes R utilizados: 
# pacman,installr,tidyverse,sparklyr,doParallel,readr,foreach,arrow,read.dbc,
# vroom,fs,data.table,microbenchmark

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

# Estou utilizando uma máquina cujo processador têm 16 threads. Meu default nos 
# comandos que explicitarei o número de thread à serem utilizadas será 80% das
# threads disponíveis, arredondado. Caso queira utilizar um número diferente de
# threads, basta alterar o comando abaixo conforme conveniência.

p_load(doParallel)
threads=round(as.numeric(detectCores()*0.8))

# Especificações da máquina utilizada para fazer e rodar o código:

# Lenovo Ideapad 3
# Windows 11
# CPU AMD Ryzen 7 5700u (16 threads)
# RAM 20(16 + 4)GB DDR4 3200 Mhz
# GPU AMD Radeon Vega 8 (Integrada)
# 256 GB SSD M.2 2242 PCIe NVMe

#-------------------------------------------------------------------------------

# Questão 1: Criando o cluster spark.
#
# a) Crie uma pasta (chamada datasus) em seu computador e faça o download dos 
# arquivos referentes ao Sistema de informação de Nascidos Vivos (SINASC), os 
# quais estão disponíveis em https://datasus.saude.gov.br/transferencia-de-arquivos/.
#
# Atenção: Considere apenas os Nascidos Vivos no Brasil (sigla DN) entre 1994 e 
# 2020, incluindo os dados estaduais e excluindo os arquivos referentes ao Brasil 
# (sigla BR). Use wi-fi para fazer os downloads!
#
# Dica: O  endereço  ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/1996_/Dados/DNRES/
# permite a imediata identificação dos endereços e arquivos a serem baixados.

#-------------------------------------------------------------------------------

pasta <- "datasus"

if (file.exists(pasta)) {
  
  print("A pasta já existe")
  rm(pasta)
  
} else {
  
  dir.create(pasta)
  rm(pasta)
  
}

# OBS: as linhas cujo comando envolve baixar (ou exportar) arquivos, colocarei 
# três # ( ### ) {Para diferenciar das linhas que são somente comentário} após 
# rodar pela primeira vez para evitar baixar (e exportar) desnecessariamente 
# os arquivos novamente.

### download.file("ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/1996_/Dados/DNRES/", "./datasus/referencia.txt")

p_load(readr)

referencia <- read_table("datasus/referencia.txt",col_names = FALSE)

nome <- referencia$X4
rm(referencia)

link <- paste("ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/1996_/Dados/DNRES/" , nome, sep="")
destino <- paste("./datasus/",nome,sep="")

#-------------------------------
# Testes:

# download.file(link, destino)
# por algum motivo ele não permite baixar tudo de uma vez. criarei um loop portanto para baixar um a um

# for (i in 1:686){download.file(link[i], destino[i])}
# rm(i)

# demorou cerca de 1h para baixar tudo, numa conexão estável de 500Mbps!!!!

# Testando para ver se funcionou
# p_load(read.dbc)
# teste <- read.dbc(destino[1])
# não funcionou, os arquivos vieram corrompidos!!
# adicionando o comando mode="wb" para corrigir

#teste:
#download.file(link[1], destino[1],mode="wb")
#teste <- read.dbc(destino[1])
# funciona
#-------------------------------------------------------------------------------

# Testando uma abordagem paralelizada

p_load(foreach)

### foreach(i = 1:686) %dopar% {download.file(link[i], destino[i],mode="wb")}

### teste <- read.dbc(destino[1])
### rm(teste)

# Funcionou (baixou os arquivos, porém a paralelização não funcionou. Devo ter feito algo errado!!)

#-------------------------------------------------------------------------------

#b) Usando a função p_load(do pacote pacman), carregue os pacotes arrow e read.dbc
# e converta os arquivos baixados no item a) para formato o .parquet. Em seguida,
# converta para .csv apenas os arquivos referentes aos estados GO, MS e ES. 
# Considerando apenas os referidos estados, compare o tamanho ocupado pelos 
# arquivos nos formatos .parquet e .csv (use a função file.size).

#-------------------------------------------------------------------------------

p_load(arrow,read.dbc)

banco <- list()
for (i in 1:686){
  banco[[i]] <- read.dbc(destino[i])
}
rm(i)

p_load(tidyverse)
nome2 <- str_sub(nome,start=1,end=8)
nomep <- paste(nome2,".parquet",sep="")
nomep <- paste("./datasus/",nomep,sep="")

#-------------------------------
# Testes:

# Tentando novamente a paralelização

# cluster <- makeCluster(threads)
# registerDoParallel(cluster)

# foreach(i=1:686) %dopar% {write_parquet(as.data.frame(banco[i]),nomep[i])}

# stopCluster(cluster)

# Interessante; na primeira tentativa, obtive o seguinte erro:
# Error in serialize(data, node$con) : error writing to connection
# Aparentemente o sistema ficou sem memória e automaticamente "matou" um ou mais
# workers do cluster, fazendo a função parar. Estava utilizando todos as threads.
# portanto, diminui o número de threads a utilizar.
# (...)
# testando com algumas threads "ociosas", o mesmo erro ocorre.

# Portanto, farei a "moda antiga"

# rm(cluster)

#-------------------------------

### for (i in 1:686){
###   write_parquet(as.data.frame(banco[i]),nomep[i])
### }
### rm(i)

# Demorou um pouco (~8 minutos), mas funcionou...

# Preparando a seleção dos estados solicitados: GO, MS e ES.
rm(banco)

nomet <- as.tibble(nome)
nometGO <- nomet %>%
  filter(str_detect(value, "GO"))
nometMS <- nomet %>%
  filter(str_detect(value, "MS"))
nometES <- nomet %>%
  filter(str_detect(value, "ES"))

nomet <- rbind(nometES,nometGO,nometMS)

selecao <- as.vector(nomet)
selecao <- selecao$value

selecaod <- paste("./datasus/",selecao,sep="")

rm(nomet,nometES,nometGO,nometMS)

# Exportanto os arquivos em .csv
banco <- list()
for (i in 1:75){
  banco[[i]] <- read.dbc(selecaod[i])
}
rm(i)

selecao2 <- str_sub(selecao,start=1,end=8)
selecao2 <- paste(selecao2,".csv",sep="")
selecao2p <- paste("./datasus/",selecao2,sep="")

p_load(vroom)

for (i in 1:75){
  vroom_write(as.data.frame(banco[i]),selecao2p[i])
}
rm(i,banco)

# Comparando agora o tamanho dos arquivos .parquet e .csv utilizando a função 
# file.size, conforme solicitado pelo exercicio

selecao3 <- str_sub(selecao,start=1,end=8)
selecao3 <- paste(selecao3,".parquet",sep="")
selecao3 <- paste("./datasus/",selecao3,sep="")

arquivosCSV <- selecao2p
arquivosPARQUET <- selecao3
rm(selecao2p,selecao3)

file.size(arquivosCSV)
file.size(arquivosPARQUET)

# Utilizando a função file_size, do pacote fs (eu prefiro)
p_load(fs)

file_size(arquivosCSV)
file_size(arquivosPARQUET)

# Diferença de tamanho entre os arquivos:
diferenca <- file_size(arquivosCSV) - file_size(arquivosPARQUET)

# soma das diferenças:
sum(diferenca)
# 594M
# ou seja, os 75 arquivos selecionados em .parquet são praticamente 600M mais 
# 'leve' que em .csv

#-------------------------------------------------------------------------------

# c) Crie uma conexão Spark, carregue para ele os dados em formato .parquet e .csv
# e compare os respectivos tempos computacionais. Se desejar, importe apenas as 
# colunas necessárias para realizar a Questão2.

# OBS: Lembre-se de que quando indicamos uma pasta na conexão, as colunas 
# escolhidas para a análise precisam existir em todos os arquivos.

# * No caso, as colunas que constam no arquivo de 1996 *

#-------------------------------------------------------------------------------
# selecionando colunas e juntando tudo num só arquivo para mandar para o Spark

# poderia fazer uma vez só e exportar nos dois formatos, mas farei a leitura
# em .csv e em .parquet e exportarei um a um por fins didátivos

p_load(data.table)
###df <- arquivosCSV %>%
###  map(fread,
###      nThread=threads,
###      fill=TRUE) %>%
###  rbindlist(fill=TRUE)
###df <- df[,1:21]

###vroom_write(df,"./datasus/dfcsv")

###dfparquet <- arquivosPARQUET %>%
###  map(read_parquet) %>%
###  rbindlist(fill=TRUE)
###dfparquet <- dfparquet[,1:21]

###write_parquet(dfparquet,"./datasus/dfparquet")

# Limpando o ambiente...

rm(arquivosCSV,arquivosPARQUET,destino,i,link,nome,nome2,nomep,selecao,selecao2,
   selecaod,df,dfparquet)

# Criando a conexão Spark

p_load(sparklyr)

config <- spark_config()
config$spark.executor.cores <- threads
config$spark.executor.memory <- "12G"
sc <- spark_connect(master = "local", config = config)
rm(config)

spark_web(sc)

# carregando os dados em formato .parquet e .csv, e comparando o tempo computacional

dfcsv = spark_read_csv(sc=sc, 
                       name = "dfcsv",
                       path = "./datasus/dfcsv", 
                       header = TRUE, 
                       delimiter = "\\t", 
                       charset = "latin1",
                       infer_schema = T,
                       overwrite = T)

dfparquet = spark_read_parquet(sc=sc, 
                               name = "dfparquet",
                               path = "./datasus/dfparquet", 
                               header = TRUE, 
                               delimiter = "\\t", 
                               charset = "latin1",
                               infer_schema = T,
                               overwrite = T)

p_load(microbenchmark)

### mbmcsv <- microbenchmark(
###   {dfcsv = spark_read_csv(sc=sc, 
###                        name = "dfcsv",
###                        path = "./datasus/dfcsv", 
###                        header = TRUE, 
###                        delimiter = "\\t", 
###                        charset = "latin1",
###                        infer_schema = T,
###                        overwrite = T)
###   }, times = 10)
### 
### mbmparquet <- microbenchmark(
###   {dfparquet = spark_read_parquet(sc=sc, 
###                        name = "dfparquet",
###                        path = "./datasus/dfparquet", 
###                        header = TRUE, 
###                        delimiter = "\\t", 
###                        charset = "latin1",
###                        infer_schema = T,
###                        overwrite = T)
###   },times = 10)

# Resultados:

# Primeira execução:
### mbmcsv$expr <- NA
### mbmcsv$expr <- ".csv"

### mbmparquet$expr <- NA
### mbmparquet$expr <- ".parquet"

### mbm <- rbind(mbmcsv,mbmparquet)

### rm(mbmcsv,mbmparquet)

### saveRDS(mbm,file="mbmsparkload.rds")

# Demais execuções:
mbm <- readRDS("mbmsparkload.rds")

autoplot(mbm)

#-------------------------------------------------------------------------------

# Questão 2: Preparando e modelando os dados.

# Atenção: Elabore seus comandos dando preferência as funcionalidades do pacote sparklyr.
  
#-------------------------------------------------------------------------------

# a) Faça uma breve análise exploratória dos dados (tabelas e gráficos) com base
# somente nas colunas existente nos arquivos de 1996. O dicionário das variaveis
# encontra-se no mesmo site do item "1-a)", na parte de documentação. Corrija 
# eventuais erros encontrados; por exemplo, na variavel sexo são apresentados 
# rótulos distintos para um mesmo significado.

#-------------------------------------------------------------------------------
