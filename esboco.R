if (!require("pacman")) install.packages("pacman")
pacman::p_load(installr)
updateR()

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

download.file(link, nome_destino)
rm(link,nome_destino)

#-------------------------------------------------------------------------------
# b)Usando a função `p_load` (do pacote `pacman`), carregue o pacote `vroom` 
# (que deve ser usado em toda a Questão 1) e use-o para carregar o primeiro dos 
# arquivos baixados para o R. Descreva brevemente o banco de dados.
# Extra: explore essa amostra sem o comando explícito de download.

if (!require("pacman")) install.packages("pacman")
p_load(vroom)

AC1 <- vroom("./dados/AC1.csv", 
                     locale = locale("br", encoding = "UTF-8"),
                     num_threads = 16)

# Descrição: (...)

#-------------------------------------------------------------------------------

# c) Quantos arquivos totalizam nossos dados? Qual é o tamanho total 
# (em Megabytes) de todos os arquivos?

if (!require("pacman")) install.packages("pacman")
p_load(fs,tidyfst)

fs::file_size("./dados/AC1.csv")

arquivos<-list.files(path="./dados",full.names = T)
arquivos <- length(arquivos)
arquivos

sys_time_print({
  dir_info("./dados/") -> info
})

info %>% 
  summarise_dt(size = sum(size))

#-------------------------------------------------------------------------------

# d) Repita o procedimento do item b), mas, dessa vez, carregue para a memória 
# apenas os casos em que a vacina aplicada foi a Astrazeneca. 
# Para tanto, faça a filtragem usando uma conexão `pipe()`. 
# Observe que a filtragem deve ser feita durante o carregamente, e não após ele.

filtro <- "dir /B | findstr /R /C:"[ASTRAZENECA/FIOCRUZ  ./dados/AC1.csv]""
AC1_AZ <- vroom(pipe(filtro),
                         locale = locale("br", encoding = "UTF-8"),
                num_threads = 16)
