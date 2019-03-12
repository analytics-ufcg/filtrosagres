#' @title Carrega licitações
#' @description Carrega a informações de licitações associadas a CNPJ's participantes da licitação.
#'     O dataframe retornado conta com as seguintes variáveis.
#'     \describe{
#'         \item{cd_UGestora}{Unidade Gestora de origem}
#'         \item{nu_Licitacao}{Número da licitação}
#'         \item{tp_Licitacao}{Tipo da licitação}
#'         \item{dt_Ano}{Ano em que a licitação foi realizada}
#'         \item{dt_Homologacao}{Data em que a licitação foi homologada}
#'         \item{vl_Licitacao}{Valor da licitação}
#'     }
#' @param db_user Usuario utilizado para conexão no BD
#' @param ano_inicial Ano inicial do intervalo de tempo (limite inferior). Default é 2011.
#' @param ano_final Ano final do intervalo de tempo (limite superior). Default é 2016.
#' @return Data frame com informações da licitação. Para cada empresa (CNPJ) informações sobre a quantidade de licitações e o montante de dinheiro envolvido por ano
#' @export
carrega_licitacoes <- function(db_user, ano_inicial = 2011, ano_final = 2016){
    sagres <- dbConnect(RMySQL::MySQL(),
                        dbname = "sagres_municipal",
                        group = "rsagrespb",
                        username = db_user)

    template <- ('
                SELECT cd_UGestora, nu_Licitacao, tp_Licitacao, dt_Ano, dt_Homologacao, vl_Licitacao
                FROM Licitacao
                WHERE dt_Ano BETWEEN %d and %d
                 ')

    query <- template %>%
        sprintf(ano_inicial, ano_final) %>%
        sql()

    licitacoes <- tbl(sagres, query) %>%
        collect(n = Inf)

    DBI::dbDisconnect(sagres)

    return(licitacoes)

}

#' @title Carrega participantes de licitações
#' @description Carrega informações de participação em licitações a partir de uma lista de CNPJ's
#'     O dataframe retornado conta com as seguintes variáveis.
#'     \describe{
#'         \item{cd_UGestora}{Unidade Gestora de origem}
#'         \item{nu_Licitacao}{Número da licitação}
#'         \item{tp_Licitacao}{Tipo da licitação}
#'         \item{dt_Ano}{Ano em que a licitação foi realizada}
#'         \item{dt_MesAno}{Data em que a licitação foi homologada (Mês e ano)}
#'         \item{nu_CPFCNPJ}{Código do credor (CPF/CNPJ)}
#'     }
#' @param db_user Usuario utilizado para conexão no BD
#' @param lista_cnpjs Lista de CNPJ's que se quer pesquisar
#' @return Data frame com informações de participações em licitação para cada CNPJ da lista de CNPJ's.
#' @export
carrega_participantes <- function(db_user, lista_cnpjs) {
    sagres <- dbConnect(RMySQL::MySQL(),
                        dbname = "sagres_municipal",
                        group = "rsagrespb",
                        username = db_user)

    template <- ('
                SELECT *
                FROM Participantes
                WHERE nu_CPFCNPJ IN (%s)
                 ')

    query <- template %>%
        sprintf(paste(lista_cnpjs, collapse = ", ")) %>%
        sql()

    participacoes <- tbl(sagres, query) %>%
        collect(n = Inf)

    DBI::dbDisconnect(sagres)

    return(participacoes)

}

#' @title Carrega vencedores de licitações
#' @description Carrega lista de licitações realizadas em um determinado período de tempo
#'     O dataframe retornado conta com as seguintes variáveis.
#'     \describe{
#'         \item{cd_UGestora}{Unidade Gestora de origem}
#'         \item{nu_Licitacao}{Número da licitação}
#'         \item{tp_Licitacao}{Tipo da licitação}
#'         \item{cd_Credor}{Código do credor (CPF/CNPJ)}
#'         \item{min_dt_Empenho}{Data do primeiro empenho}
#'     }
#' @param db_user Usuario utilizado para conexão no BD
#' @param ano_inicial Ano inicial do período de tempo
#' @param ano_final Ano final do período de tempo
#' @return Data frame com informações da licitação e de seus vencedores
#' @export
carrega_licitacoes_vencedores <- function(db_user, ano_inicial = 2011, ano_final = 2016) {
    sagres <- dbConnect(RMySQL::MySQL(),
                        dbname = "sagres_municipal",
                        group = "rsagrespb",
                        username = db_user)

    template <- ('
                SELECT cd_UGestora, nu_Licitacao, tp_Licitacao, dt_Ano, dt_Homologacao, vl_Licitacao
                FROM Licitacao
                WHERE dt_Ano BETWEEN %d and %d#
                 ')

    query <- template %>%
        sprintf(ano_inicial, ano_final) %>%
        sql()

    licitacoes <- tbl(sagres, query) %>%
        compute(name = "lic") %>%
        collect(n = Inf)

    query <- sql('
                SELECT e.cd_UGestora, e.dt_Ano, e.cd_UnidOrcamentaria, e.nu_Empenho, e.tp_Licitacao, e.nu_Licitacao, e.cd_Credor, e.dt_Empenho
                FROM Empenhos e
                USE INDEX (FK_Empenhos_Licitacao)
                INNER JOIN lic
                USING (cd_UGestora, nu_Licitacao, tp_Licitacao)
    ')

    empenhos <- tbl(sagres, query) %>%
        collect(n = Inf)

    DBI::dbDisconnect(sagres)

    licitacoes_vencedores <- empenhos %>%
        group_by(cd_UGestora, nu_Licitacao, tp_Licitacao, cd_Credor) %>%
        summarise(min_dt_Empenho = min(dt_Empenho)) %>%
        ungroup() %>%
        mutate(venceu = 1)

    return(licitacoes_vencedores)
}

#' @title Carrega propostas de licitações
#' @description Carrega a informações das propostas associadas a licitações que ocorreram dentro de um intervalo de tempo
#'     O dataframe retornado conta com as seguintes variáveis.
#'     \describe{
#'         \item{cd_UGestora}{Unidade Gestora de origem}
#'         \item{nu_Licitacao}{Número da licitação}
#'         \item{tp_Licitacao}{Tipo da licitação}
#'         \item{dt_Homologacao}{Data em que a licitação foi homologada}
#'         \item{dt_Ano}{Ano em que a licitação foi homologada}
#'         \item{cd_Item}{Código do item ao qual se refere uma proposta}
#'         \item{cd_SubGrupoItem}{Código do subgrupo do item ao qual se refere uma proposta}
#'         \item{vl_Ofertado}{Valor ofertado pelo objeto da licitação}
#'     }
#' @param db_user Usuario utilizado para conexão no BD
#' @param ano_inicial Ano inicial do intervalo de tempo (limite inferior). Default é 2011.
#' @param ano_final Ano final do intervalo de tempo (limite superior). Default é 2016.
#' @return Data frame com informações das propostas para licitações em um intervalo de tempo
#' @export
carrega_propostas_licitacao <- function(db_user, ano_inicial = 2011, ano_final = 2016) {
    sagres <- dbConnect(RMySQL::MySQL(),
                        dbname = "sagres_municipal",
                        group = "rsagrespb",
                        username = db_user)

    template <- ('
                SELECT cd_UGestora, nu_Licitacao, tp_Licitacao, dt_Ano, dt_Homologacao, vl_Licitacao
                FROM Licitacao
                WHERE dt_Ano BETWEEN %d and %d
                 ')

    query <- template %>%
        sprintf(ano_inicial, ano_final) %>%
        sql()

    licitacoes <- tbl(sagres, query) %>%
        compute(name = "lic") %>%
        collect(n = Inf)

    query <- sql('
                SELECT p.cd_UGestora, p.nu_Licitacao, p.tp_Licitacao, lic.dt_Homologacao, lic.dt_Ano, p.cd_Item, p.cd_SubGrupoItem, p.nu_CPFCNPJ, p.vl_Ofertado
                FROM Propostas p
                INNER JOIN lic
                USING (cd_UGestora, nu_Licitacao, tp_Licitacao)
                ')

    propostas <- tbl(sagres, query) %>%
        collect(n = Inf)

    DBI::dbDisconnect(sagres)

    return(propostas)
}

