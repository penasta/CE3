#-------------------------------------------------------------------------------

# TLDR: Se quiser rodar o código inteiro, basta remover o # da linha 150 e rodar
# o código inteiro. AVISO: Demora cerca de 2h para rodar tudo, num computador com
# 20GB de RAM e 16 threads de processador.

#-------------------------------------------------------------------------------

# 0.0 - Do autor

# Universidade de Brasília - Unb
# Instituto de ciências exatas - IE
# Departamento de Estatística - EST
# Tópicos em estatística 1 - Dados massivos (CE3)
# Prof. Dr. Guilherme Rodrigues
# Lista 1
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

# Questão 1: leitura eficiente de dados

# a) Utilizando códigos R, crie uma pasta (chamada dados) em seu computador e faça 
# o download de todos os arquivos disponíveis no endereço eletrônico a seguir.
# https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/5093679f-12c3-4d6b-b7bd-07694de54173?inner_span=True


# Extra: Faça isso automatizando os downloads e direcionando-os para uma mesma pasta.


# Esta pasta deve ser criada com código R. Sugestão: faça com que a máquina 
# confira se a pasta existe e crie a pasta apenas se não existir.

#-------------------------------------------------------------------------------

pasta <- "dados"

if (file.exists(pasta)) {
  
  print("A pasta já existe")
  rm(pasta)
  
} else {
  
  dir.create(pasta)
  rm(pasta)
  
}

link <- c("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DAC/part-00000-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DAC/part-00001-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DAC/part-00002-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DAL/part-00000-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DAL/part-00001-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DAL/part-00002-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DAM/part-00000-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DAM/part-00001-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DAM/part-00002-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DAP/part-00000-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DAP/part-00001-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DAP/part-00002-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DBA/part-00000-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DBA/part-00001-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DBA/part-00002-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DCE/part-00000-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DCE/part-00001-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DCE/part-00002-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DDF/part-00000-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DDF/part-00001-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DDF/part-00002-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DES/part-00000-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DES/part-00001-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DES/part-00002-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DGO/part-00000-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DGO/part-00001-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DGO/part-00002-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DMA/part-00000-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DMA/part-00001-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DMA/part-00002-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DMG/part-00000-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DMG/part-00001-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DMG/part-00002-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DMS/part-00000-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DMS/part-00001-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DMS/part-00002-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DMT/part-00000-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DMT/part-00001-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         ,"https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIPNI/COVID/uf/uf%3DMT/part-00002-8cc53ec0-4f0d-48d1-ab69-fc5dbebe4013.c000.csv"
         )

nome_destino <- c("./dados/AC1.csv","./dados/AC2.csv","./dados/AC3.csv",
              "./dados/AL1.csv","./dados/AL2.csv","./dados/AL3.csv",
              "./dados/AM1.csv","./dados/AM2.csv","./dados/AM3.csv",
              "./dados/AP1.csv","./dados/AP2.csv","./dados/AP3.csv",
              "./dados/BA1.csv","./dados/BA2.csv","./dados/BA3.csv",
              "./dados/CE1.csv","./dados/CE2.csv","./dados/CE3.csv",
              "./dados/DF1.csv","./dados/DF2.csv","./dados/DF3.csv",
              "./dados/ES1.csv","./dados/ES2.csv","./dados/ES3.csv",
              "./dados/GO1.csv","./dados/GO2.csv","./dados/GO3.csv",
              "./dados/MA1.csv","./dados/MA2.csv","./dados/MA3.csv",
              "./dados/MG1.csv","./dados/MG2.csv","./dados/MG3.csv",
              "./dados/MS1.csv","./dados/MS2.csv","./dados/MS3.csv",
              "./dados/MT1.csv","./dados/MT2.csv","./dados/MT3.csv")

#-------------------------------------------------------------------------------

# Rodar o comando abaixo apenas uma vez para baixar!!!!
# download.file(link, nome_destino)

#-------------------------------------------------------------------------------

rm(link,nome_destino)

#-------------------------------------------------------------------------------

# b)Usando a função `p_load` (do pacote `pacman`), carregue o pacote `vroom` 
# (que deve ser usado em toda a Questão 1) e use-o para carregar o primeiro dos 
# arquivos baixados para o R. Descreva brevemente o banco de dados.
# Extra: explore essa amostra sem o comando explícito de download.

p_load(vroom,fs,tidyfst)

sys_time_print({
  
  AC1 <- vroom("./dados/AC1.csv", 
                     locale = locale("br", encoding = "UTF-8"),
                     num_threads = threads)
  
})
# Finished in 0.440s elapsed (0.390s cpu)

# Descrição: (...)

# Extra: explore essa amostra sem o comando explícito de download.

# Não entendi o que era para fazer no extra (?)

#-------------------------------------------------------------------------------

# c) Quantos arquivos totalizam nossos dados? Qual é o tamanho total 
# (em Megabytes) de todos os arquivos?

p_load(tidyverse)

fs::file_size("./dados/AC1.csv")

arquivos<-list.files(path="./dados",full.names = T)
arquivos <- length(arquivos)

# Quantidade de arquivos
arquivos

# Tamanho total dos arquivos
info <- dir_info(path="./dados/")
info %>% 
  summarise_dt(size = sum(size))

#-------------------------------------------------------------------------------

# d) Repita o procedimento do item b), mas, dessa vez, carregue para a memória 
# apenas os casos em que a vacina aplicada foi a Astrazeneca. 
# Para tanto, faça a filtragem usando uma conexão `pipe()`. 
# Observe que a filtragem deve ser feita durante o carregamente, e não após ele.

filtro <- "findstr ASTRAZENECA/FIOCRUZ C:\\Users\\toled\\Documents\\GitHub\\CE3\\dados\\AC1.csv"


sys_time_print({
  
  AC1_AZ <- vroom(pipe(filtro),
                         locale = locale("br", encoding = "UTF-8"),
                num_threads = threads)

})
# Finished in 3.070s elapsed (2.970s cpu)


# Quantos megabites deixaram de ser carregados para a memória RAM 
# (ao fazer a filtragem durante a leitura, e não no próprio `R`)?

format(object.size(AC1)-object.size(AC1_AZ),units="auto")
# 49.4 Mb

#-------------------------------------------------------------------------------

# e) Carregue para o R todos os arquivos da pasta de uma única vez 
# (usando apenas um comando R, sem métodos iterativos). 

pasta_arquivos <- "./dados/"
nomes_arquivos <- list.files(pasta_arquivos)
nomes_arquivos <- str_c(pasta_arquivos, nomes_arquivos)

sys_time_print({
  
  df <- vroom(nomes_arquivos, 
                         locale = locale("br", encoding = "UTF-8"),
               num_threads = threads)
  
})
# Finished in 16.6s elapsed (18.7s cpu)

#-------------------------------------------------------------------------------

# testando com o read_csv ao invés

# sys_time_print({
#     teste <- read_csv(nomes_arquivos)
#   })

# Finished in 48.2s elapsed (50.9s cpu)

#-------------------------------------------------------------------------------

## Questão 2: manipulação de dados

# a) Utilizando o pacote data.table, repita o procedimento do item 1e), 
# agora mantendo, durante a leitura, apenas as 3 primeiras colunas. Use o pacote 
# geobr para obter os dados sobre as regiões de saúde do Brasil 
# (procure as funções do geobr). Junte (join) os dados da base de vacinações 
# com o das regiões de saúde.

# Descreva brevemente o que são as regiões
# (use documentação do governo, não se atenha à documentação do pacote).

#-------------------------------------------------------------------------------

p_load(data.table)

#lendo um csv para teste com o comando fread do data.table

sys_time_print({
  
  df_dt <- fread("./dados/AC1.csv",select=(1:3))
  
})
# Finished in 0.180s elapsed (0.200s cpu)

# teste <- fread(file=nomes_arquivos,select=c(1:3))
# Não lê todos os arquivos, apresenta erro.

# teste <- fread(input=list.files("./dados/", ".csv"),select=c(1:3))
# Não lê todos os arquivos, apresenta erro.

# Terei, portanto, de usar algum tipo de "recursividade". No caso, a função map do purrr
# para ler todos os arquivos numa lista, após isso, juntá-los com o comando rbindlist

rm(df_dt)

sys_time_print({
  
  df <- nomes_arquivos %>%
  map(fread,
      drop=(4:32),
      nThread=threads) %>%
  rbindlist()
  
})

# Finished in 27.1s elapsed (24.4s cpu)

# p_load(devtools)
# devtools::install_github("ipeaGIT/geobr", subdir = "r-package")

p_load(geobr)
# apesar de usar o pacman para carregar o pacote, a instalação teve de ser feita
# manualmente pelo github do pacote, visto que o mesmo não consta mais no CRAN

# Consta na documentação do geobr dados das regiões de saúde para os anos de 
# 1991, 1994, 1997, 2001, 2005, 2013. Como não foi especificado o ano pelo
# enunciado, irei optar pelo mais recente (2013)

sys_time_print({
  
  rs <- read_health_region(
  year = 2013,
  macro = FALSE,
  simplified = FALSE,
  showProgress = TRUE
  )
  
})

# Finished in 1.720s elapsed (0.250s cpu)

class(rs)
rs <- as.data.table(rs)

# Pelas 3 primeiras colunas que o enunciado pediu para ler, nenhuma das 3 variáveis
# apresenta possibilidade de indexação com o banco de dados do geobr.
# portanto, irei ler os dados novamente, incluindo a coluna que penso ser a correta
# para juntar as bases de dados
# no caso, a coluna estabelecimento_municipio_codigo (coluna 18)

sys_time_print({
  
  df <- nomes_arquivos %>%
    map(fread,
        drop=c(4:17,19:32),
        nThread=threads) %>%
    rbindlist()
  
})

# Finished in 24.8s elapsed (26.1s cpu)

# Agora, devemos indexar o município a região de saúde ao qual ele pertence.
# utizarei uma tabela disponível em https://sage.saude.gov.br/paineis/regiaoSaude/lista.php?output=html&
# para fazer a indexação.

sys_time_print({
  
  index <- fread("./index.csv",select=(4:5),nThread = threads)
  
})


colnames(df)[4] <- "regiao ibge"

colnames(index) <- c("regiao ibge","regiao saude")

index$`regiao ibge`<- as.factor(index$`regiao ibge`)
index$`regiao saude` <- as.factor(index$`regiao saude`)

df$`regiao ibge` <- as.factor(df$`regiao ibge`)

# Juntando o banco com o banco indexador utilizando o data.table

df <- merge(df,index, by="regiao ibge")

# Juntando os bancos utilizando o data.table

# Para utilizar o comando merge do data.table, é necessário que a coluna de ambos
# os bancos tenham o mesmo nome. Realizando o ajuste e juntando.

colnames(rs)[1] <- "regiao saude"

rs$`regiao saude` <- as.factor(rs$`regiao saude`)

junto <- merge(df, rs, by = "regiao saude")

# OBS: tentar visualizar esse data frame completo a partir daqui consome muita
# memória RAM. Meu computador com 20GB de RAM travou quando tentei ver.

head(junto)
nrow(df)-nrow(junto)
# por algum motivo, 153772 observações estão sumindo no merge.

# Descrição: (...)

# ------------------------------------------------------------------------------

# b) No datatable obtido no item a), crie as variáveis descritas abaixo considerando
# apenas os pacientes registrados para a segunda dose:
  
#  1. Quantidade de vacinados por região de saúde;
#  2. Condicionalmente, a faixa de vacinação por região de saúde 
# (alta ou baixa, em relação à mediana da distribuição de vacinações). 

# Crie uma tabela com as 5 regiões de saúde com menos vacinados em cada faixa de vacinação.

# Observação: os itens a) e b) podem ser executados de modo encadeado, usando o operador de pipe.

# ------------------------------------------------------------------------------

hj <- head(junto)
# view(hj)

# Às colunas anteriormente importadas não fazem referência ao registro de dose
# de vacina nos pacientes. Portanto, será necessário repetir os processos do item
# anterior, incluindo essa coluna.

# no caso, a coluna número 29

sys_time_print({
  
  df <- nomes_arquivos %>%
    map(fread,
        drop=c(4:17,19:28,30:32),
        nThread=threads) %>%
    rbindlist()
  
})

colnames(df)[4] <- "regiao ibge"

df$`regiao ibge` <- as.factor(df$`regiao ibge`)

df <- merge(df,index, by="regiao ibge")

colnames(rs)[1] <- "regiao saude"

rs$`regiao saude` <- as.factor(rs$`regiao saude`)

junto <- merge(df, rs, by = "regiao saude")

# Testando primeiramente em um pedaço do banco
hj <- head(junto)
# view(hj)

teste <- subset(hj, vacina_descricao_dose == "2ª Dose")

rm(teste)

# aplicando ao banco

sys_time_print({
  
junto <- subset(junto, vacina_descricao_dose == "2ª Dose")

})
# Finished in 0.360s elapsed (0.320s cpu)

# Apesar de consideravelmente menor, o view() desse novo banco ainda trava o meu computador

hj <- head(junto)
# view(hj)

#  1. Quantidade de vacinados por região de saúde

junto$`regiao saude` <- droplevels(junto$`regiao saude`)

#  2. Condicionalmente, a faixa de vacinação por região de saúde (alta ou baixa, em relação à mediana da distribuição de vacinações). 

vetor <- as.numeric(table(junto$`regiao saude`))
median(vetor)
vetor < median(vetor)
# 2957 é a mediana de aplicações/região. Portanto, igual ou maior que isso será alta, e menor será baixa.

n <- junto %>% 
  count(`regiao saude`)

junto <- merge(junto, n, by = "regiao saude")

junto[, faixa_de_vacinacao:= ifelse(n > 2957, 'alto', 'baixa')] 

hj <- head(junto)
# view(hj)

# Crie uma tabela com as 5 regiões de saúde com menos vacinados em cada faixa de vacinação.

# Os 5 menos vacinados da faixa "baixo"

tabela <- as.data.table(table(junto$`regiao saude`))
colnames(tabela) <- c("regiao saude","n")

tabela <- tabela[order(n)]
tabela <- tabela[1:5,]

tabela5 <- merge(tabela, rs, by = "regiao saude")
tabela5 <- tabela5[,1:6]

tabela5

# Os 5 menos vacinados da faixa "alto"

tabela <- as.data.table(table(junto$`regiao saude`))
colnames(tabela) <- c("regiao saude","n")

tabela <- tabela[order(n)]

tabela <- filter (tabela, n >= 2957)

tabela5a <- merge(tabela, rs, by = "regiao saude")
tabela5a <- as.data.table(tabela5a)
tabela5a <- tabela5a[order(n)]
tabela5a <- tabela5a[1:5,1:6]

tabela5a

#-------------------------------------------------------------------------------

# c) Utilizando o pacote `dtplyr`, repita o procedimento dos itens a) e b) 
# (lembre-se das funções `mutate`, `group_by`, `summarise`, entre outras). 
# Garanta que você conseguiu criar um objeto com lazy evaluation e outro 
# resgatado todos os dados para a memória. Exiba os resultados.

#-------------------------------------------------------------------------------

p_load(dtplyr)

dt_df <- lazy_dt(df)

# Verificando o lazy evaluation

p_load(lobstr)

obj_size(df)
obj_size(dt_df)
obj_size(df, dt_df)

# > obj_size(df)
# 1.52 GB
# > obj_size(dt_df)
# 1.52 GB
# > obj_size(df, dt_df)
# 1.52 GB

# De fato, o objetio lazy criado "dt_df" não está ocupando espaço extra em memória
# em relação ao objeto original "df".

# limpando um pouco do ambiente para ter memória para trabalhar

rm(AC1,AC1_AZ,hj,info,junto,n,tabela,tabela5,tabela5a,arquivos,filtro,nomes_arquivos,pasta_arquivos,vetor)
gc()
rm(dt_df)

#-------------------------------------------------------------------------------
# Por algum motivo não consegui passar esta parte do código por operadores pipe
# portanto, vamos linha a linha

dt_df <- left_join(df,index,by="regiao ibge") 
colnames(rs)[1] <- "regiao saude.x"
dt_df <- left_join(dt_df,rs,by="regiao saude.x")

dfc <- dt_df %>%
  collect()
dfc <- dfc[,1:6]

dfc_dt <- lazy_dt(dfc)
dfc_dt <- dfc_dt %>%
  filter(vacina_descricao_dose == "2ª Dose")
dfc <- dfc_dt %>%
  collect()
  
colnames(dfc)[6] <- "regiao saude"
dfc$`regiao saude` <- droplevels(dfc$`regiao saude`)
vetort <- as.numeric(table(dfc$`regiao saude`))
median(vetort)
vetort < median(vetort)

# 2507 é a mediana de aplicações/região. Portanto, igual ou maior que isso será alta, e menor será baixa.
# perceba que a mediana foi alterada, pois fazendo a conta programando com o dtplyr,
# não houve a perda de observações que aconteceu anteriormente. Logo, com um numero
# diferente de observações, a conta da mediana apresentou resultado diferente.

n <- dfc %>% 
  count(`regiao saude`)

dfc <- left_join(dfc, n, by = "regiao saude")

dfc <- dfc %>%
  mutate(faixa_de_vacinacao = ifelse(n >= 2957,'alto','baixo'))

#-------------------------------------------------------------------------------

# d) Com o pacote microbenchmark, comparare o tempo de execução do item c) 
# quando se adota as funções do dtplyr e do dplyr.

p_load(microbenchmark)
rm(dfc,dfc_dt,dt_df,n,vetort)

# Consolidando tudo feito anteriormente no dtplyr dentro de uma função benchmark;

mbm1 <- microbenchmark({
  dt_df <- left_join(df,index,by="regiao ibge")
  colnames(rs)[1] <- "regiao saude.x"
  dt_df <- left_join(dt_df,rs,by="regiao saude.x")
  dfc <- dt_df %>%
    collect()
  dfc <- dfc[,1:6]
  dfc_dt <- lazy_dt(dfc)
  dfc_dt <- dfc_dt %>%
    filter(vacina_descricao_dose == "2ª Dose")
  dfc <- dfc_dt %>%
    collect()
  colnames(dfc)[6] <- "regiao saude"
  dfc$`regiao saude` <- droplevels(dfc$`regiao saude`)
  vetort <- as.numeric(table(dfc$`regiao saude`))
  median(vetort)
  vetort < median(vetort)
  n <- dfc %>% 
    count(`regiao saude`)
  dfc <- left_join(dfc, n, by = "regiao saude")
  dfc <- dfc %>%
    mutate(faixa_de_vacinacao = ifelse(n >= 2957,'alto','baixo'))
  rm(dfc,dfc_dt,dt_df,n,vetort)
  }
)

# Agora, fazendo o microbenchmark para o dplyr;

rm(dfc,dfc_dt,dt_df,n,vetort)
detach("package:dtplyr", unload=TRUE)

df <- as_tibble(df)

mbm2 <- microbenchmark({
  dft <- left_join(df,index,by="regiao ibge")
  colnames(rs)[1] <- "regiao saude.x"
  dft <- left_join(dft,rs,by="regiao saude.x")
  dft <- dft[,1:6]
  dft <- dft %>%
    filter(vacina_descricao_dose == "2ª Dose")
  colnames(dft)[6] <- "regiao saude"
  dft$`regiao saude` <- droplevels(dft$`regiao saude`)
  vetort <- as.numeric(table(dft$`regiao saude`))
  median(vetort)
  vetort < median(vetort)
  n <- dft %>% 
    count(`regiao saude`)
  dft <- left_join(dft, n, by = "regiao saude")
  dft <- dft %>%
    mutate(faixa_de_vacinacao = ifelse(n >= 2957,'alto','baixo'))
  rm(dft,n,vetort)
})

# Resultados:

mbm1$expr <- NA
mbm1$expr <- "DTplyr"

mbm2$expr <- NA
mbm2$expr <- "Dplyr"

mbm <- rbind(mbm1,mbm2)

# saveRDS(mbm,file="mbm.rds")
autoplot(mbm)

#-------------------------------------------------------------------------------

# rm(list = ls())

#-------------------------------------------------------------------------------