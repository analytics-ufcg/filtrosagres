#' @title Carrega licitações por fornecedor
#' @description Carrega a lista de licitações por fornecedor com informações de pagamentos em um intervalo de tempo.
#' O dataframe retornado conta com as seguintes variáveis:
#'     \describe{
#'         \item{cd_UGestora}{Unidade Gestora de origem}
#'         \item{nu_Licitacao}{Número da licitação}
#'         \item{tp_Licitacao}{Tipo da licitação}
#'         \item{dt_Ano}{Ano em que o empenho foi realizado}
#'         \item{cd_Credor}{Código do credor (CPF, CNPJ)}
#'         \item{no_Credor}{Nome do credor}
#'         \item{qt_Empenhos}{Quantidade de empenhos destinados ao credor gerados por uma licitação}
#'         \item{vl_Pago}{Valor dos pagamentos associados ao empenho}
#'         \item{vl_Estornado}{Valor dos estornos associados ao empenho (quando houver)}
#'         \item{vl_Ganho}{Valor do ganho total associado ao empenho (quando houver)}
#'     }
#' @param db_user Usuario utilizado para conexão no BD
#' @param ano_inicial Ano inicial do intervalo de tempo
#' @param ano_final Ano final do intervalo de tempo
#' @param lista_cnpjs Lista com os CNPJ's dos credores de interesse
#' @return Data frame com informações da licitação, incluindo nome do credor vitorioso, total pago, total estornado e número de empenhos
carrega_licitacoes_por_fornecedor <- function(db_user, ano_inicial = 2014, ano_final = 2014, lista_cnpjs) {
    empenhos <- carrega_empenhos(db_user, ano_inicial, ano_final, lista_cnpjs)

    licitacoes_por_fornecedor <- empenhos %>%
        group_by(cd_UGestora, nu_Licitacao, tp_Licitacao, dt_Ano, cd_Credor) %>%
        summarise(no_Credor = first(no_Credor),
                  qt_Empenhos = n(),
                  vl_Pago = sum(vl_Pago),
                  vl_Estornado = sum(vl_Estornado)) %>%
        mutate(vl_Estornado = replace_na(vl_Estornado, 0),
               vl_Ganho = vl_Pago - vl_Estornado) %>%
        ungroup()

    return(licitacoes_por_fornecedor)
}

#' @title Carrega contratos
#' @description Carrega contratos realizados em um intervalo de tempo
#' O dataframe retornado conta com as seguintes variáveis:
#'     \describe{
#'         \item{cd_UGestora}{Unidade Gestora de origem}
#'         \item{dt_Ano}{Ano em que o empenho foi realizado}
#'         \item{nu_Contrato}{Número do contrato}
#'         \item{dt_Assinatura}{Data da assinatura dos contratos}
#'         \item{pr_Vigencia}{Prazo de vigência do contrato}
#'         \item{nu_CPFCNPJ}{Código do credor (CPF, CNPJ)}
#'         \item{nu_Licitacao}{Número da licitação}
#'         \item{tp_Licitação}{Tipo da licitação}
#'         \item{vl_TotalContrato}{Valor total do contrato}
#'         \item{dt_MesAno}{Mês e ano em que o contrato foi firmado}
#'     }
#' @param db_user Usuario utilizado para conexão no BD
#' @param ano_inicial Ano inicial do intervalo de tempo
#' @param ano_final Ano final do intervalo de tempo
#' @param limite_inferior Limite inferior para o valor do contrato
#' @return Data frame com informações do contrato, como o contratado e o valor do contrato.
#' @export
carrega_contratos <- function(db_user, ano_inicial = 2014, ano_final = 2014, limite_inferior=140e3) {

    sagres <- dbConnect(RMySQL::MySQL(),
                        dbname = "sagres_municipal",
                        group = "rsagrespb",
                        username = db_user)

    template <- ('
        SELECT *
        FROM Contratos
        WHERE dt_Ano BETWEEN %d AND %d AND vl_TotalContrato >= %d
    ')

    query <- template %>%
        sprintf(ano_inicial, ano_final, limite_inferior) %>%
        sql()

    contratos <- tbl(sagres, query) %>%
        select(-c(de_Obs, registroCGE, cd_SIAFI, dt_Recebimento, foto, planilha, ordemServico)) %>%
        collect(n = Inf)

    DBI::dbDisconnect(sagres)

    return(contratos)
}

#' @title Carrega contratos de uma licitação
#' @description Carrega contratos realizados em um intervalo de tempo agrupados por licitação e por fornecedor
#' O dataframe retornado conta com as seguintes variáveis:
#'     \describe{
#'         \item{cd_UGestora}{Unidade Gestora de origem}
#'         \item{nu_Licitacao}{Número da licitação}
#'         \item{tp_Licitação}{Tipo da licitação}
#'         \item{nu_CPFCNPJ}{Código do credor (CPF, CNPJ)}
#'         \item{vl_SomaContratos}{Valor total dos contratos recebidos pelo credor na licitação}
#'     }
#' @param db_user Usuario utilizado para conexão no BD
#' @param ano_inicial Ano inicial do intervalo de tempo
#' @param ano_final Ano final do intervalo de tempo
#' @return Data frame com informações do agrupamento, como o contratado e o soma do valor dos contratos.
carrega_contratos_por_licitacao_e_fornecedor <- function(db_user, ano_inicial = 2014, ano_final = 2014) {
    sagres <- dbConnect(RMySQL::MySQL(),
                        dbname = "sagres_municipal",
                        group = "rsagrespb",
                        username = db_user)

    template <- ('
               SELECT cd_UGestora, nu_Licitacao, tp_Licitacao, nu_CPFCNPJ, SUM(vl_TotalContrato) as vl_SomaContratos
               FROM Contratos
               WHERE dt_Ano BETWEEN %d AND %d
               GROUP BY cd_UGestora, nu_Licitacao, tp_Licitacao, nu_CPFCNPJ
               ')

    query <- template %>%
        sprintf(ano_inicial, ano_final) %>%
        sql()

    contratos_group <- tbl(sagres, query) %>%
        collect(n = Inf)

    DBI::dbDisconnect(sagres)

    return(contratos_group)
}
