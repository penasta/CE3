#-------------------------------------------------------------------------------

# TLDR: 

#-------------------------------------------------------------------------------

# 0.0 - Do autor

# Universidade de Brasília - Unb
# Instituto de ciências exatas - IE
# Departamento de Estatística - EST
# Tópicos em estatística 1 - Dados massivos (CE3)
# Prof. Dr. Guilherme Rodrigues
# Lista 2
# Aluno: Bruno Gondim toledo | Matrícula: 15/0167636
# 1/2022 | 03/08/2022
# github do projeto: https://github.com/penasta/CE3
# email do autor: bruno.gondim@aluno.unb.br

#-------------------------------------------------------------------------------

# 0.1 - Do software 

# linguagem de programação utilizada: R (versão 4.2.1).
# IDE utilizada: RStudio Desktop 2022.07.1+554
# Pacotes R utilizados: 
# installr,vroom,fs,tidyfst,tidyverse,data.table,geobr,dtplyr,microbenchmark

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
# precisamos realizar as operações de manipulaçãoforadoR, em um banco de dados ou 
# em um sistema de armazenamento distribuído. Outas vezes, os dadosjá estão 
# armazenados em algum servidor/cluster e queremos carregar para oRparte dele, 
# possivelmenteapós algumas manipulações.

# Nessa lista repetiremos parte do que fizemos na Lista 1.  
# Se desejar, use o gabarito da Lista 1 emsubstituição à sua própria solução 
# dos respectivos itens.

#-------------------------------------------------------------------------------

# Questão 1: Criando bancos de dados.

# a) Crie um banco de dados SQLite e adicione a tabela gerada no item 1e) 
# da Lista 1.

#-------------------------------------------------------------------------------

# L1-1e) Carregue para o R todos os arquivos da pasta de uma única vez 
# (usando apenas um comando R, sem métodos iterativos).

#-------------------------------------------------------------------------------
# Gerando a tabela do item 1e) da lista 1
p_load(tidyverse,vroom)
pasta_arquivos <- "./dados/"
nomes_arquivos <- list.files(pasta_arquivos)
nomes_arquivos <- str_c(pasta_arquivos, nomes_arquivos)

df1 <- vroom(nomes_arquivos,
            locale = locale("br", encoding = "UTF-8"),
            num_threads = threads)

#-------------------------------------------------------------------------------

p_load(RSQLite)

# Criando a DataBase (NÃO INCLUIR ESTA PARTE NO MARKDOWN)
SQL <- dbConnect(RSQLite::SQLite(), "dfDB.db")

# Criando a tabela dentro da database
dbWriteTable(SQL, "df_DATA", df1)

dbListTables(SQL)

#-------------------------------------------------------------------------------

# L1-2a) Repita o procedimento do item 1e) agora mantendo, durante a leitura, 
# apenas as 3 primeiras colunas. Use o pacote `geobr` para obter os dados sobre 
# as regiões de saúde do Brasil. Junte (join) os dados da base de vacinações 
# com o das regiões de saúde.

#-------------------------------------------------------------------------------
# Começando gerando a tabela da L1-2a)
p_load(data.table,geobr)

rs <- read_health_region(
  year = 2013,
  macro = FALSE,
  simplified = FALSE,
  showProgress = TRUE
  )

index <- fread("./index.csv",select=(4:5),nThread = threads)

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

#-------------------------------------------------------------------------------
# Refazendo o item L2- 1a) para o df "correto"

# Criando a DataBase (NÃO INCLUIR ESTA PARTE NO MARKDOWN)
SQL2 <- dbConnect(RSQLite::SQLite(), "df2b.db")

# Criando a tabela dentro da database

dbWriteTable(SQL2, "df_data2b", df[,1:10])

# Error: Can only bind lists of raw vectors (or NULL)
# A coluna geometria impede o comando de funcionar. Removerei então essa coluna

dbListTables(SQL2)

#-------------------------------------------------------------------------------
# Removendo os objetos gerados no item L2-1a)

rm(SQL,df1)

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

