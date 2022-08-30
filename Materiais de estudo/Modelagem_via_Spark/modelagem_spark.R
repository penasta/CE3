### Criando a infraestrutura ----
if (!require("pacman")) install.packages("pacman")
p_load(rmdformats, stringr, vroom, sparklyr, tidyverse, kableExtra)

### Configurando a conexão ----

conf <- spark_config()
conf$`sparklyr.connect.cores.local` <- 6
sc <- spark_connect(master = "local", 
                    config = conf, 
                    version = "2.4.3")

#spark_disconnect(sc) # sempre!

### Carregando os dados no cluster ("memory = TRUE") ----
# dados *Titanic* disponivel em https://www.kaggle.com/competitions/titanic/data

path <- ".\\titanic\\"
list.files(path)

start <- Sys.time()
titanic_tbl = spark_read_csv(
  sc = sc,
  name = "titanic_tbl",
  path = paste0(path, "\\", "train.csv"),
  header = TRUE,
  delimiter = ",",
  charset = "latin1",
  infer_schema = T
)
spark_data_read_time <- Sys.time() - start

dados_head <- titanic_tbl %>% 
  head() %>% 
  collect


tamanho <- sum(file.size(paste0(path, "train.csv"))/(10^3))

print(paste("Levou", round(spark_data_read_time, digits = 2),
            units(spark_data_read_time),"para carregar o conjunto de dados de",
            round(tamanho),"KB no SparklyR"))



### Dicionario da base de dados ----

text_tbl <- data.frame(
Nome=c('PassengerID','Survived','PCclass',
       'Name','Sex','Age','SibSp','Parch',
       'Ticket','Fare','Cabin','Embarked'),
Descrição=c(
'Número de identificação do passageiro;',
'Informa se o passageiro sobreviveu ao naufrágio (0 = não e 1 = sim);',
'Classe do bilhete (1 = "1ª classe"; 2 = "2ª classe" e 3 = "3ª classe");',
'Nome do passageiro;',
'Sexo do passageiro;',
'Idade do passageiro;',
'Quantidade de cônjuges e/ou irmãos a bordo;',
'Quantidade de pais e filhos a bordo;',
'Número da passagem;',
'Preço da passagem;',
'Número da cabine do passageiro;',
'Porto de embarque: (C = Cherbourg; Q = Queenstown; S = Southampton).' 
))

kbl(text_tbl) %>%
  kable_paper(full_width = F) %>%
  column_spec(1, bold = T, border_right = T) %>%
  column_spec(2, width = "30em", background = "yellow")

### Análise exploratória: ----

#### Verificando a dimensão e o banco

sdf_dim(titanic_tbl) # igual a funçao dim() porém para o ambiente spark

glimpse(titanic_tbl)

sdf_describe(titanic_tbl) # igual a funçao summary() porém para o ambiente spark

#### Verificando a completude da informação (qtd de NA)

titanic_tbl %>%
  summarise_all(~sum(as.integer(is.na(.)), na.rm = TRUE)) %>% 
  show_query()

#### Distribuiçao de sexo por sobrevivente

titanic_tbl %>%
  group_by(Sex, Survived) %>% 
  tally() %>% 
  mutate(frac = round(n / sum(n),2)) %>% 
  arrange(Sex, Survived)

titanic_tbl %>%
  filter(!is.na(Embarked)) %>% 
  group_by(Embarked,Survived) %>% 
  tally() %>% 
  mutate(frac = round(n / sum(n),2)) %>% 
  arrange(Embarked,Survived)

### Feature Engineering ----
#### Preparando os dados para modelagem
#### ft_ é usado para transformadores de características

model_tbl <- titanic_tbl %>%
  select(Survived, Pclass, Sex, Age, Fare, SibSp, Parch, Name, Embarked) %>%
  filter(!is.na(Embarked)) %>%
  mutate(Survived = as.double(Survived),
         Age = if_else(is.na(Age), mean(Age), Age), #INPUT PELA MÉDIA NOS NA
         Pclass = as.double(Pclass))


# ou usando ft_
titanic_tbl %>%
  ft_imputer("Age", "Age_norm", strategy = "mean") %>% 
  pull(Age_norm) # olha para linha 6


glimpse(model_tbl)

#### Normalização
scale_values <- model_tbl %>%
  summarize(
    mean_Age = mean(Age),
    mean_Fare = mean(Fare),
    sd_Age = sd(Age),
    sd_Fare = sd(Fare)
  ) %>%
  collect()

model_tbl <- model_tbl %>%
  mutate(scaled_Age = (Age - local(scale_values$mean_Age)) / ## !! == local() pois não existe o valor no spark
           !!scale_values$sd_Age,
         scaled_Fare = (Fare - !!scale_values$mean_Fare) /
           !!scale_values$sd_Fare)


# Usando a função ft_standard_scaler
teste <- model_tbl %>% 
  ft_vector_assembler(input_col = "Age", 
                      output_col = "Age_temp") %>% 
  ft_standard_scaler(input_col = "Age_temp", 
                output_col = "Age_scaled2",
                with_mean = T) %>% 
  select(Age, Age_temp, scaled_Age, Age_scaled2) %>% 
  collect()


title_vars <- title %>% 
  map(~ expr(ifelse(rlike(Name, !!.x), 1, 0))) %>%
  set_names(str_c("title_", title))

#### Criando uma nova variavel

title <- c("Master", "Miss", "Mr", "Mrs")


model_tbl <- model_tbl %>% 
  mutate(local(title_vars),
    title_Mr = if_else(title_Mrs == 1, 0, title_Mr),
    title_officer = if_else(
      title_Mr == 0 && title_Mrs == 0 &&
        title_Master == 0 && title_Miss == 0, 1, 0))

model_tbl %>%
  select(starts_with("title"), Name)


### Particionando em bases de treinamento e teste ----

partition <- model_tbl %>%
  sdf_random_split(training = 0.85, test = 0.15, seed = 131281) # 13 dez 81

#### Criando as conexões de referência
data_training <- sdf_register(partition$train, "trips_train")
data_test <- sdf_register(partition$test, "trips_test")

#### Cache Force the data to be loaded into memory
tbl_cache(sc, "trips_train")
tbl_cache(sc, "trips_test")

#### verificando tabelas no spark
src_tbls(sc)


### Regressão logistica usando o R ----

#### usando o base
formula <- ('Survived ~ Sex + scaled_Age + scaled_Fare + Pclass +
            SibSp + Parch + Embarked + title_Mr + title_Mrs + title_Miss +
            title_Master + title_officer')

lr_model <- glm(formula,
                family=binomial(link='logit'),data=data_training)
summary(lr_model)

#### resultados
pred.test <- predict(lr_model, data_test, type="response")
pred.test <- ifelse(pred.test > 0.5,1,0)

mean(pred.test == data_test %>% pull(Survived)) # média de acertos
table(pred.test, data_test %>% pull(Survived)) # matrix de confusão

#### usando o spark

lr2_model <- data_training %>%
  ml_logistic_regression(formula)

validation_summary <- ml_evaluate(lr2_model, data_test) # Predict()

roc <- validation_summary$roc() %>%
  collect()

validation_summary$area_under_roc()

ggplot(roc, aes(x = FPR, y = TPR)) +
  geom_line() + geom_abline(lty = "dashed")
