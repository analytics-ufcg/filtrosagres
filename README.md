# rsagrespb
Pacote que permite filtragem de licitações e outros documentos públicos da base de dados do SAGRES (TCE-PB)

Para instalar basta efetuar os seguintes comandos em seu R:

```R
# Instala as ferramentas de desenvolvedor
install.packages('devtools')

# Instala o pacote a partir do github
devtools::install_github('analytics-ufcg/rsagrespb')

# Carrega o pacote
library(rsagrespb)
```

É necessário ter uma conexão com a base *sagres_municipal* do Sagres PB disponível e
configurada no arquivo ~/.my.cnf sob o nome de grupo *rsagrespb*, de acordo com o modelo a seguir.

```
[rsagrespb]
database                = sagres_municipal
user                    = ***
password                = ***
host                    = ***
port                    = ***
default-character-set   = utf8
```
