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
# vroom,fs,data.table,microbenchmark,dbplot,corrr

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

###for (i in 1:75){
###  vroom_write(as.data.frame(banco[i]),selecao2p[i])
###}
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
config$spark.executor.memory <- "9G"
sc <- spark_connect(master = "local", config = config)
rm(config)

spark_web(sc)

# carregando os dados em formato .parquet e .csv, e comparando o tempo computacional

dfparquet = spark_read_parquet(sc=sc, 
                               name = "dfparquet",
                               path = "./datasus/dfparquet", 
                               header = TRUE, 
                               delimiter = "\\t", 
                               charset = "latin1",
                               infer_schema = T,
                               overwrite = T)

###dfcsv = spark_read_csv(sc=sc, 
###                       name = "dfcsv",
###                       path = "./datasus/dfcsv", 
###                       header = TRUE, 
###                       delimiter = "\\t", 
###                       charset = "latin1",
###                       infer_schema = T,
###                       overwrite = T)


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

# rm(mbm)

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

# Coletando informações sobre o banco
dim <- sdf_dim(dfparquet) 

glimpse <- glimpse(dfparquet)

na <- dfparquet %>%
  summarise_all(~sum(as.integer(is.na(.)))) %>%
  collect()

summary <- sdf_describe(dfparquet) %>%
  collect ()

#### Corrigindo erros 

###dfparquet %>%
###  group_by(SEXO) %>% 
###  tally() 

# Arrumando a variável:

dfparquet <- dfparquet %>%
  mutate(SEXO = case_when(SEXO == 'I' ~ 0,
                          SEXO == 0 ~ 0,
                          SEXO == 9 ~ 0,
                          SEXO == 'F' ~ 2,
                          SEXO == 2 ~ 2,
                          SEXO == 1 ~ 1,
                          SEXO == 'M' ~ 1))

###dfparquet %>%
###  group_by(SEXO) %>% 
###  tally() 

# Conferindo outras variáveis

###dfparquet %>%
###  group_by(LOCNASC) %>% 
###  tally() 

###dfparquet %>%
###  group_by(ESTCIVMAE) %>% 
###  tally() 

###dfparquet %>%
###  group_by(ESCMAE) %>% 
###  tally() %>% 
###  print(n = Inf)

# No caso dessa variável ESCMAE (Escolaridade da mãe em anos de estudo concluídos),
# a documentação apresenta 6 categorias diferentes. No banco, há mais, no caso, as
# categorias '7', '6' e '8'. Como não estão documentadas, não é possível inferir nada
# sobre elas.

###dfparquet %>%
###  group_by(GESTACAO) %>% 
###  tally() 

# Essa varíável GESTACAO (Semanas de gestação, conforme tabela) ocorre um problema
# parecido com a variável anterior. consta uma categoria '8' que não está documentada.

###dfparquet %>%
###  group_by(GRAVIDEZ) %>% 
###  tally() 

###dfparquet %>%
###  group_by(PARTO) %>% 
###  tally() 

###dfparquet %>%
###  group_by(CONSULTAS) %>% 
###  tally() 

# A variável CONSULTAS (Número de consultas de pré-natal) também apresenta uma
# categoria não documentada '8'

###dfparquet %>%
###  group_by(APGAR1) %>% 
###  tally() %>% 
###  print(n = Inf)

# Essa variável também precisa de ajuste..

dfparquet <- dfparquet %>%
  mutate(APGAR1 = case_when(APGAR1 == '07' ~ '07',
                            APGAR1 == '01' ~ '01',
                            APGAR1 == '.' ~ NA,
                            APGAR1 == '08' ~ '08',
                            APGAR1 == '09' ~ '09',
                            APGAR1 == '8' ~ '08',
                            APGAR1 == '10' ~ '10',
                            APGAR1 == '0' ~ '00',
                            APGAR1 == '9' ~ '09',
                            APGAR1 == '05' ~ '05',
                            APGAR1 == '06' ~ '06',
                            APGAR1 == '<e8>' ~ NA,
                            APGAR1 == NA ~ NA,
                            APGAR1 == '03' ~ '03',
                            APGAR1 == '7' ~ '07',
                            APGAR1 == '04' ~ '04',
                            APGAR1 == '00' ~ '00',
                            APGAR1 == '--' ~ NA,
                            APGAR1 == '..' ~ NA,
                            APGAR1 == '99' ~ NA,
                            APGAR1 == '02' ~ '02'
                            ))

###dfparquet %>%
###  group_by(APGAR1) %>% 
###  tally() %>% 
###  print(n = Inf)

###dfparquet %>%
###  group_by(APGAR5) %>% 
###  tally() %>% 
###  print(n = Inf)

dfparquet <- dfparquet %>%
  mutate(APGAR5 = case_when(APGAR5 == '07' ~ '07',
                            APGAR5 == '01' ~ '01',
                            APGAR5 == '.' ~ NA,
                            APGAR5 == '08' ~ '08',
                            APGAR5 == '\u0017' ~ NA,
                            APGAR5 == '09' ~ '09',
                            APGAR5 == '8' ~ '08',
                            APGAR5 == '10' ~ '10',
                            APGAR5 == '0' ~ '00',
                            APGAR5 == '9' ~ '09',
                            APGAR5 == '4' ~ '04',
                            APGAR5 == '05' ~ '05',
                            APGAR5 == '06' ~ '06',
                            APGAR5 == '5' ~ '05',
                            APGAR5 == NA ~ NA,
                            APGAR5 == '03' ~ '03',
                            APGAR5 == '04' ~ '04',
                            APGAR5 == '00' ~ '00',
                            APGAR5 == '--' ~ NA,
                            APGAR5 == '0-' ~ '00',
                            APGAR5 == '..' ~ NA,
                            APGAR5 == '99' ~ NA,
                            APGAR5 == '02' ~ '02',
                            APGAR5 == '39' ~ NA
  ))

###dfparquet %>%
###  group_by(RACACOR) %>% 
###  tally() %>% 
###  print(n = Inf)

# Neste caso acho correto imputar NA no valor '9', visto que nas demais variáveis
# este valor significa 'ignorado'

dfparquet <- dfparquet %>%
  mutate(RACACOR = case_when(RACACOR == 3 ~ 3,
                             RACACOR == 9 ~ NA,
                             RACACOR == 2 ~ 2,
                             RACACOR == 4 ~ 4,
                             RACACOR == 1 ~ 1,
                             RACACOR == 5 ~ 5,
                             RACACOR == NA ~ NA

  ))

###dfparquet %>%
###  group_by(IDADEMAE) %>% 
###  tally() %>% 
###  print(n = Inf)

# Removendo valores estúpidos:

dfparquet <- dfparquet %>%
  mutate(IDADEMAE = ifelse(IDADEMAE == '00' | IDADEMAE == '99', NA, IDADEMAE))

#### Escolha das variáveis para análise exploratória

# É possível fazer mais de 300 cruzamentos dois a dois entre as variáveis 
# disponíveis. Portanto, é necessária uma análise crítica da escolha de variáveis
# a se comparar. 

## uma análise exploratória descritiva; quantidade de nascidos por sexo e raça:

sexoraca <- dfparquet %>%
  group_by(RACACOR,SEXO) %>% 
  filter(RACACOR %in% 1:5 & SEXO %in% 1:2) %>%
  tally() %>% 
#  print(n = Inf) %>%
  collect()

## Quantidade de nascidos por local de nascimento por tipo de parto:

tipolocal <- dfparquet %>%
  group_by(LOCNASC,PARTO) %>% 
  filter(LOCNASC %in% 1:3 & PARTO %in% 1:2) %>%
  tally() %>% 
#  print(n = Inf) %>%
  collect ()

## Analisando dias da semana;

#dfparquet %>%
#  select(DTNASC) %>%
#  mutate(dia = lubridate::wday(lubridate::dmy(DTNASC)))

# Error in `wday()`:
# ! wday() is not available in this SQL variant
# Run `rlang::last_error()` to see where the error occurred.

# Infelizmente, aparentemente o sparklyr ainda não tem uma tradução para as 
# funções do lubridate. Terei então de trapacear e trazer como vetor para 
# o ambiente do R.

diasemana <-dfparquet %>%
  select(DTNASC) %>%
  collect()

diasemana <- as.vector(diasemana$DTNASC)
diasemana <- lubridate::wday(lubridate::dmy(diasemana),  label = TRUE)
diasemana <- factor(diasemana)

# Um gráfico bem simples:
###plot(diasemana)

# Uma tabela bem simples:
###summary(diasemana)

## Agora, analisando o peso...
p_load(dbplot)

# Teste
#dbplot_histogram(dfparquet, PESO)     [funciona]

# Contando os absurdos (esse deixei passar da parte dos ajustes de propósito,
# para analisar o caso particular)
###dfparquet %>%
###  select(PESO) %>%
###  filter(PESO >= '7.500') %>%
###  tally()

###dfparquet %>%
###  select(PESO) %>%
###  filter(PESO <= '7.500') %>% # removendo valores absurdos
# OBS: o valor de corte não foi aleatório. Uma rápida pesquisa no Google mostra
# que esse foi o peso do maior bebê já nascido no Brasil.
# no banco, constam 2884 observações superiores a este número.
###  dbplot_histogram(PESO, bins = 15)

#dfparquet %>%
#  dbplot_boxplot(PESO)
#
#Error in dbplot_boxplot(., PESO) : 
#  could not find function "dbplot_boxplot"
# ??????????
# Procurei online, nada.
#
#dbplot::dbplot_boxplot(dfparquet, var=PESO)
# Error: 'dbplot_boxplot' is not an exported object from 'namespace:dbplot'
#
#Bom, ficarei devendo o boxplot.

## Analisando agora o estado civil...

estcivil <- dfparquet %>%
  group_by(ESTCIVMAE) %>% 
  filter(ESTCIVMAE %in% 1:4) %>%
  tally() %>%
  collect()

p_load(corrr)
## Testando uma correlação;
## Idade da mãe X Peso do bebê ao nascer

correlacao1 <- dfparquet %>%
  select(IDADEMAE,PESO) %>%
  filter(PESO <= '7.500') %>%
  mutate(IDADEMAE = as.integer(IDADEMAE)) %>%
  mutate(PESO = as.numeric(PESO)) %>%
#  correlate() # A função promete funcionar no Spark. Infelizmente, dá erro.
  collect() 

correlacao1 <- correlate(correlacao1)
correlacao1 <- as.numeric(correlacao1$IDADEMAE[2])

#-------------------------------------------------------------------------------

# b) Utilizando as funções do sparklyr, preencha os dados faltantes na idade da
# mãe com base na mediana. Se necessário, faça imputação de dados também nas demais váriaveis.

#-------------------------------------------------------------------------------

###dfparquet %>%
###  group_by(IDADEMAE) %>% 
###  tally() %>% 
###  print(n = Inf)

# Inputando a mediana para os dados faltantes da variável IDADEMAE

# Novamente, o fantasma de calcular a mediana no SQL
# com a média, o ifelse sai direto
# portanto, farei o calculo da mediana e colocarei artificialmente no ifelse

###dfparquet %>%
###  mutate(IDADEMAE = as.integer(IDADEMAE)) %>%
###  sdf_quantile(
###    column = "IDADEMAE",
###    probabilities = c(0.5),
###    relative.error = 0.01)

# O valor é 25.

dfparquet <- dfparquet %>%
  mutate(IDADEMAE = as.integer(IDADEMAE)) %>%
  mutate(IDADEMAE = if_else(is.na(IDADEMAE), 25 , IDADEMAE)) 

# Bom, já que estamos aqui, vamos fazer o mesmo para a variável PESO

###dfparquet %>%
###  mutate(PESO = as.integer(PESO)) %>%
###  sdf_quantile(
###    column = "PESO",
###    probabilities = c(0.5),
###    relative.error = 0.01)

# O valor é 3210 

dfparquet <- dfparquet %>%
  mutate(PESO = as.integer(PESO)) %>%
  mutate(PESO = if_else(is.na(PESO), 3210 , PESO)) 

#-------------------------------------------------------------------------------

# c) Novamente, utilizando as funções do sparklyr, normalize (retire a média e 
# divida pelo desvio padrão) as variáveis quantitativas do banco.

#-------------------------------------------------------------------------------
# Quantitativas: IDADEMAE, PESO
# Poderia se incluir também nessa categoria as variáveis QTDFILVIVO e QTDFILMORT,
# que não irei incluir pelo simples fato de que não irei trabalhar com elas.
# Também poderia-se, talvez, incluir as variáveis APGAR1 e APGAR5, porém elas não
# fazem sentido sem ser inteiros, e também não irei trabalhar com elas.
#
# Portanto, normalizarei apenas as variáveis IDADEMAE e PESO

# (sim, essa parte é um copi-cola descarado do HTML)

# Calculando as medidas necessárias para a normalização.
scale_values <- dfparquet %>%
  summarize(
    mean_PESO = mean(PESO),
    mean_IDADEMAE = mean(IDADEMAE),
    sd_PESO = sd(PESO),
    sd_IDADEMAE = sd(IDADEMAE)
  ) %>%
  collect()

# Normalizando as variáveis PESO e IDADEMAE
## Usamos !! ou local() para calcular os valores no R
dfparquet <- dfparquet %>%
  mutate(scaled_PESO = (PESO - local(scale_values$mean_PESO)) / 
           !!scale_values$sd_PESO,
         scaled_IDADEMAE = (IDADEMAE - !!scale_values$mean_IDADEMAE) /
           !!scale_values$sd_IDADEMAE)

# Testando usar a função ft_standard_scaler ou ft_normalizer() 
## teste <- dfparquet %>% 
##   ft_vector_assembler(input_col = "PESO", 
##                       output_col = "PESO_temp") %>% 
##   ft_standard_scaler(input_col = "PESO_temp", 
##                 output_col = "PESO_scaled2",
##                 with_mean = T) %>% 
##   select(PESO, PESO_temp, scaled_PESO, PESO_scaled2) %>% 
##   collect()
# Crashou o computador e não funcionou kkkk

#-------------------------------------------------------------------------------

# d) Crie variáveis dummy (one-hot-encoding) que conjuntamente indiquem o dia 
# da semana do nascimento(SEG, TER, . . . ). Em seguida,binarize o número de 
# consultas pré-natais de modo que “0” represente “até 5 consultas” e “1” 
# indique “6 ou mais consultas”. (Ultilize as funções ft_)

#-------------------------------------------------------------------------------

# Por acaso, já havia extraído o dia da semana do nascimento na parte de análise
# exploratória. Portanto, seguirei a partir do que já tenho.

# dfparquet <- dfparquet %>%
#   mutate(diasemana = local(diasemana))
#
# Crasha o computador. Tentando de outra forma...

diasemana <- as.data.frame(diasemana)

diasemana <- tibble::rowid_to_column(diasemana, "id")

dfparquet <- sdf_with_sequential_id(dfparquet, id = "id", from = 1L)

diasemana <- copy_to(sc, as.data.frame(diasemana), name = deparse(substitute(diasemana)),
        memory = TRUE, repartition = 0, overwrite = TRUE)

dfparquet <- left_join(dfparquet,diasemana)

###dfparquet %>%
###  group_by(diasemana) %>% 
###  tally() %>% 
###  print(n = Inf)

###sdf_dim(dfparquet) 

# Bom, não será possível binarizar da forma que o exercício pede, visto que
# o número de consultas pré natais está agregado da seguinte forma:
# | 1: Nenhuma | 2: de 1 a 3 | 3: de 4 a 6 | 4: 7 e mais | 9: Ignorado | 
# Portanto, não dá para colocar a separatriz no 5. Arbitrariamente, portanto,
# colocarei da seguinte forma: |  0: 3 ou menos | 1: 4 ou mais |

dfparquet <- dfparquet %>%
  mutate(CONSULTAS = ifelse(CONSULTAS == '8' | CONSULTAS == '9', NA, CONSULTAS))

###dfparquet %>%
###  group_by(CONSULTAS) %>% 
###  tally() 

dfparquet <- dfparquet %>%
  mutate(CONSULTAS = as.numeric(CONSULTAS)) %>%
  ft_binarizer(
    input_col = "CONSULTAS",
    output_col = "CONSULTAS_bin",
    threshold = 2
    
  ) 

###dfparquet %>%
###  group_by(CONSULTAS_bin) %>% 
###  tally() 

#-------------------------------------------------------------------------------

# e) Particione os dados aleatoriamente em bases de treinamento e teste. Ajuste,
# sobre a base de treinamento, um modelo de regressão logistica em que a 
# variável resposta (y), indica se o parto foi ou não cesáreo. Analise o 
# desempenho preditivo do modelo com base na matriz de confusão obtida no
# conjunto de teste.

#-------------------------------------------------------------------------------

# Particionando o conjunto de dados em treino e teste
partition <- dfparquet %>%
  sdf_random_split(training = 0.85, test = 0.15, seed = 150167636)

# Create table references
data_training <- sdf_register(partition$train, "df_train")
data_test <- sdf_register(partition$test, "df_test")

# Cache
tbl_cache(sc, "df_train")
tbl_cache(sc, "df_test")

formula <- ('PARTO ~ LOCNASC + IDADEMAE + ESCMAE + SEXO +
            APGAR1 + APGAR5 + RACACOR + PESO + CONSULTAS_bin')

lr_model <- glm(formula,
                family = binomial(link='logit'),
                data = data_training)
summary(lr_model)

#-------------------------------------------------------------------------------
# Rodar daqui p baixo, caso crashe, p/ continuar

if (!require("pacman")) install.packages("pacman")
p_load(pacman,installr,tidyverse,sparklyr,doParallel,readr,foreach,arrow,read.dbc,vroom,fs,data.table,microbenchmark,dbplot,corrr)

config <- spark_config()
config$spark.executor.cores <- threads
config$spark.executor.memory <- "9G"
sc <- spark_connect(master = "local", config = config)
rm(config)

dfparquet = spark_read_parquet(sc=sc, 
                               name = "dfparquet",
                               path = "./datasus/dfparquet", 
                               header = TRUE, 
                               delimiter = "\\t", 
                               charset = "latin1",
                               infer_schema = T,
                               overwrite = T)
