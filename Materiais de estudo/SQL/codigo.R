### Carregando os pacotes exigidos ----
if (!require("pacman")) install.packages("pacman")
p_load(tidyverse, rmdformats, stringr, vroom, mongolite, RSQLite, DBI)


### Criando um banco de dados local e connectando-o ao R ----
mydb <- dbConnect(RSQLite::SQLite(), "my-db.sqlite")
# dbDisconnect(mydb)
mydb

### Manipulando o banco de dados ----
names(iris)
dbSendStatement(
  mydb,
  'CREATE TABLE nome_tabela(
"Sepal.Length",
"Sepal.Width" ,
"Petal.Length",
"Petal.Width" ,
"Species")'
)
dbListTables(mydb)
dbRemoveTable(mydb,"nome_tabela")

names(iris)
dbSendStatement(
  mydb,
  str_c('CREATE TABLE nome_tabela(', str_c("\"", names(iris), collapse = "\", "), '\")')
)

dbListTables(mydb)
dbListFields(mydb, "nome_tabela")
dbAppendTable(mydb, "nome_tabela", iris)
iris2 <- dbReadTable(mydb, "nome_tabela") %>% head()
dbRemoveTable(mydb,"nome_tabela")



### Carregando uma tabela do R para o banco ----
(pasta_arquivos <- "../../../dados/")
nomes_arquivos <- list.files(pasta_arquivos, "combinada2021") %>% 
  str_c(pasta_arquivos, .)

dados <- vroom(nomes_arquivos[1],
      locale = locale("br", encoding = "latin1"),
      num_threads = 3) %>% 
  dbWriteTable(mydb, "dados", .)

dbGetQuery(mydb, "SELECT COUNT(*) FROM dados") 
dbGetQuery(mydb, "SELECT * FROM dados LIMIT 5") # olhando as 5 primeiras linhas e colunas para mostrar que funcionou


### Salvando vários arquivos em uma única tabela do banco ----
append_na_db <- function(arquivo, conn) {
  vroom(arquivo,
        locale = locale("br", encoding = "latin1"),
        num_threads = 3) %>% 
    dbAppendTable(conn, "dados", .)
}

walk(nomes_arquivos[-1],
     append_na_db, 
     conn = mydb)

### Usando o dbplyr para facilitar a manipulacao ----
dados <- tbl(mydb, "dados")
dados

dbGetQuery(mydb, 'SELECT "id_combinada", "id_empresa" FROM dados LIMIT 5')

dados %>% 
  group_by(nm_pais) %>%
  summarise("mediaKgPago"=mean(kg_carga_paga, na.rm = T)) %>%
  arrange(desc(`mediaKgPago`)) %>% 
  show_query() %>%
  collect()
