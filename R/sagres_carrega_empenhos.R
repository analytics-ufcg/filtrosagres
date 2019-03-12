#' @title Carrega empenhos
#' @description Carrega a lista de empenhos destinado a um grupo de credores em um intervalo de tempo.
#'     O dataframe retornado conta com as seguintes variáveis:
#'     \describe{
#'         \item{cd_UGestora}{Unidade Gestora de origem}
#'         \item{dt_Ano}{Ano em que o empenho foi realizado}
#'         \item{cd_UnidOrcamentaria}{Unidade Orcamentária de origem}
#'         \item{nu_Empenho}{Número do empenho}
#'         \item{nu_Licitacao}{Número da licitação}
#'         \item{tp_Licitacao}{Tipo da licitação}
#'         \item{cd_Credor}{Código do credor (CPF, CNPJ)}
#'         \item{no_Credor}{Nome do credor}
#'         \item{vl_Pago}{Valor dos pagamentos associados ao empenho}
#'         \item{vl_Estornado}{Valor dos estornos associados ao empenho (quando houver)}
#'     }
#' @param db_user Usuario utilizado para conexão no BD
#' @param ano_inicial Ano inicial do intervalo de tempo
#' @param ano_final Ano final do intervalo de tempo
#' @param lista_cnpjs Lista com os CNPJ's dos credores de interesse
#' @return Data frame com informações do empenho, do valor de pagamentos associados a este empenho e o valor estornado associado aos pagamentos
#' @export
carrega_empenhos <- function(db_user, ano_inicial = 2014, ano_final = 2014, lista_cnpjs) {
    sagres <- dbConnect(RMySQL::MySQL(),
                        dbname = "sagres_municipal",
                        group = "rsagrespb",
                        username = db_user)

    template <- ('
            SELECT cd_UGestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho, nu_Licitacao,
                    tp_Licitacao, cd_Credor, no_Credor, vl_Pago, vl_Estornado
            FROM Empenhos e
            INNER JOIN (
	            SELECT p.cd_UGestora, p.dt_Ano, p.cd_UnidOrcamentaria, p.nu_Empenho,
                      SUM(vl_Pagamento) as vl_Pago,
                      SUM(estornado_pagamento) as vl_Estornado
                    FROM Pagamentos p
                    LEFT JOIN (
                        SELECT ep.cd_UGestora, ep.dt_Ano, ep.cd_UnidOrcamentaria, ep.nu_EmpenhoEstorno,
                               ep.nu_ParcelaEstorno, ep.tp_Lancamento, SUM(vl_Estorno) as estornado_pagamento
                        FROM EstornoPagamento ep
                        GROUP BY ep.cd_UGestora, ep.dt_Ano, ep.cd_UnidOrcamentaria, ep.nu_EmpenhoEstorno,
                                 ep.nu_ParcelaEstorno, ep.tp_Lancamento
                    ) as est
                    ON
                        p.cd_UGestora = est.cd_UGestora AND
                        p.dt_Ano = est.dt_Ano AND
                        p.cd_UnidOrcamentaria = est.cd_UnidOrcamentaria AND
                        p.nu_Empenho = est.nu_EmpenhoEstorno AND
                        p.nu_Parcela = est.nu_ParcelaEstorno AND
                        p.tp_Lancamento = est.tp_Lancamento
                    GROUP BY p.cd_UGestora, p.dt_Ano, p.cd_UnidOrcamentaria, p.nu_Empenho
                 ) as Pagamentos_group
            USING (cd_UGestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho)
            WHERE e.dt_Ano BETWEEN %d AND %d AND
                  cd_Credor IN (%s)
                 ')

    query <- template %>%
        sprintf(ano_inicial, ano_final, paste(lista_cnpjs, collapse = ", ")) %>%
        sql()

    empenhos <- tbl(sagres, query) %>%
        collect(n = Inf)

    DBI::dbDisconnect(sagres)

    return(empenhos)
}



#' @title Carrega empenhos em um intervalo de tempo
#' @description Carrega a lista de empenhos realizados em um intervalo de tempo.
#'     O dataframe retornado conta com as seguintes variáveis.
#'     \describe{
#'         \item{dt_Contrato}{Data em que o contrato foi firmado}
#'         \item{cd_Credor}{Código do credor (CPF, CNPJ)}
#'         \item{cd_UGestora}{Unidade Gestora de origem}
#'         \item{dt_Ano}{Ano em que o empenho foi realizado}
#'         \item{cd_UnidOrcamentaria}{Unidade Orcamentária de origem}
#'         \item{nu_Empenho}{Número do empenho}
#'         \item{nu_Licitacao}{Número da licitação}
#'         \item{tp_Licitacao}{Tipo da licitação}
#'         \item{no_Credor}{Nome do credor}
#'         \item{vl_Pago}{Valor dos pagamentos associados ao empenho}
#'         \item{vl_Estornado}{Valor dos estornos associados ao empenho (quando houver)}
#'     }
#' @param db_user Usuario utilizado para conexão no BD
#' @param ano_inicial Ano inicial do intervalo de tempo
#' @param ano_final Ano final do intervalo de tempo
#' @param cnpjs_datas_contratos Dataframe de CNPJ's de interesse e datas de início de contrato para o cálculo das informações de fornecimento (nu_CPFCNPJ, dt_Inicio)
#' @return Data frame com informações do empenho, do valor de pagamentos associados a este empenho e o valor estornado associado aos pagamentos
#' @export
carrega_empenhos_data_limite <- function(db_user, ano_inicial = 2014, ano_final = 2014, cnpjs_datas_contratos) {
    sagres <- dbConnect(RMySQL::MySQL(),
                        dbname = "sagres_municipal",
                        group = "rsagrespb",
                        username = db_user)

    ## Lista de CNPJS distintos
    lista_cnpjs <- cnpjs_datas_contratos %>%
        distinct(nu_CPFCNPJ) %>%
        pull(nu_CPFCNPJ)

    template <- ('
                SELECT cd_UGestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho, nu_Licitacao, tp_Licitacao, cd_Credor, no_Credor
                FROM Empenhos e
                WHERE e.dt_Ano BETWEEN %d AND %d AND
                  cd_Credor IN (%s)
                 ')

    query <- template %>%
        sprintf(ano_inicial, ano_final, paste(lista_cnpjs, collapse = ", ")) %>%
        sql()

    ## Carrega empenhos realizados entre o ano inicial e o ano final
    empenhos <- tbl(sagres, query) %>%
        compute(name = "emp") %>%
        collect(n = Inf)

    template <- ('
                  SELECT p.cd_UGestora, p.dt_Ano, p.cd_UnidOrcamentaria, p.nu_Empenho, p.nu_Parcela, p.tp_Lancamento, dt_Pagamento, vl_Pagamento
                  FROM Pagamentos p
                  INNER JOIN emp
                  USING (cd_UGestora, nu_Empenho, cd_UnidOrcamentaria, dt_Ano)
                 ')

    query <- template %>%
        sql()

    ## Carrega os pagamentos associados aos empenhos
    pagamentos <- tbl(sagres, query) %>%
        compute(name = "pag") %>%
        collect(n = Inf)

    template <- ('
                  SELECT ep.cd_UGestora, ep.cd_UnidOrcamentaria, ep.nu_EmpenhoEstorno, ep.nu_ParcelaEstorno, ep.tp_Lancamento, ep.dt_Ano,
                         ep.dt_Estorno, ep.vl_Estorno
                  FROM EstornoPagamento ep
                  INNER JOIN pag
                  ON
                    ep.cd_UGestora = pag.cd_UGestora AND
                    ep.cd_UnidOrcamentaria = pag.cd_UnidOrcamentaria AND
                    ep.nu_EmpenhoEstorno = pag.nu_Empenho AND
                    ep.nu_ParcelaEstorno = pag.nu_Parcela AND
                    ep.tp_Lancamento = pag.tp_Lancamento AND
                    ep.dt_Ano = pag.dt_Ano
                 ')

    query <- template %>%
        sql()

    ## Carrega os estornos associados aos pagamentos
    estornos_pagamento <- tbl(sagres, query) %>%
        collect(n = Inf)

    DBI::dbDisconnect(sagres)

    ## Lista de datas distintas nas quais existem contratos iniciando
    datas_lista <- cnpjs_datas_contratos %>%
        distinct(dt_Inicio) %>%
        pull(dt_Inicio)

    ## Carrega as informações de empenho
    ## (cd_UGestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho, nu_Licitacao, tp_Licitacao, cd_Credor, no_Credor, vl_Pago, vl_Estornado)
    empenhos_data <- tibble(dt_Inicio = datas_lista) %>%
        mutate(dados = map(
            dt_Inicio,
            empenhos_credores_data,
            empenhos,
            pagamentos,
            estornos_pagamento,
            cnpjs_datas_contratos
        )) %>%
        unnest(dados)

    return(empenhos_data)
}



#' @title Carrega informações dos credores
#' @description Cruza informações sobre empenhos, pagamentos e estornos a partir de uma data de limite.
#' Retorna todos os empenhos até esta data num dataframe com as variáveis:
#'    \describe{
#'         \item{cd_UGestora}{Unidade Gestora de origem}
#'         \item{dt_Ano}{Ano em que o empenho foi realizado}
#'         \item{cd_UnidOrcamentaria}{Unidade Orcamentária de origem}
#'         \item{nu_Empenho}{Número do empenho}
#'         \item{nu_Licitacao}{Número da licitação}
#'         \item{tp_Licitacao}{Tipo da licitação}
#'         \item{cd_Credor}{Código do credor (CPF, CNPJ)}
#'         \item{no_Credor}{Nome do credor}
#'         \item{vl_Pago}{Valor dos pagamentos associados ao empenho}
#'         \item{vl_Estornado}{Valor dos estornos associados ao empenho (quando houver)}
#'     }
#' @param data_limite_superior Data limite para a data do pagamento e do estorno
#' @param empenhos Data frame com todos os empenhos recuperados do SAGRES
#' @param pagamentos Data frame com todos os pagamentos recuperados do SAGRES
#' @param estornos_pagamento Data frame com todos os estornos de pagamentos recuperados do SAGRES
#' @param cnpjs_datas_contratos Lista de CNPJ's e de datas de início de contrato para o cálculo das informações de fornecimento.
#' @return Data frame com informações do empenho, do valor de pagamentos associados a este empenho e o valor estornado associado aos pagamentos
empenhos_credores_data <- function(data_limite_superior, empenhos, pagamentos, estornos_pagamento, cnpjs_datas_contratos) {
    lista_cnpjs <- cnpjs_datas_contratos %>%
        filter(dt_Inicio == data_limite_superior) %>%
        distinct(nu_CPFCNPJ) %>%
        pull(nu_CPFCNPJ)

    empenhos_credor <- empenhos %>%
        filter(cd_Credor %in% lista_cnpjs)

    pagamentos_data <- pagamentos %>%
        filter(dt_Pagamento < data_limite_superior) %>%
        inner_join(empenhos_credor %>% select(cd_UGestora, nu_Empenho, cd_UnidOrcamentaria, dt_Ano),
                   by = c("cd_UGestora", "nu_Empenho", "cd_UnidOrcamentaria", "dt_Ano"))

    estornos_pagamento_data <- estornos_pagamento %>%
        filter(dt_Estorno < data_limite_superior)

    pagamentos_data_merge <- pagamentos_data %>%
        left_join(estornos_pagamento_data,
                  by = c("cd_UGestora" = "cd_UGestora", "cd_UnidOrcamentaria" = "cd_UnidOrcamentaria",
                         "nu_Empenho" = "nu_EmpenhoEstorno", "nu_Parcela" = "nu_ParcelaEstorno",
                         "tp_Lancamento" = "tp_Lancamento", "dt_Ano" = "dt_Ano")) %>%
        mutate(vl_Estorno = replace_na(vl_Estorno, 0)) %>%
        group_by(cd_UGestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho, nu_Parcela, tp_Lancamento) %>%
        summarise(vl_Pagamento = first(vl_Pagamento),
                  vl_Estornado = sum(vl_Estorno)) %>%
        ungroup() %>%
        select(cd_UGestora, nu_Empenho, cd_UnidOrcamentaria, dt_Ano, vl_Pago = vl_Pagamento, vl_Estornado)

    empenhos_credor_merge <- empenhos_credor %>%
        inner_join(pagamentos_data_merge, by = c("cd_UGestora", "nu_Empenho", "cd_UnidOrcamentaria", "dt_Ano")) %>%

        group_by(cd_Credor, cd_UGestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho) %>%
        summarise(nu_Licitacao = first(nu_Licitacao),
                  tp_Licitacao = first(tp_Licitacao),
                  no_Credor = first(no_Credor),
                  vl_Pago = sum(vl_Pago),
                  vl_Estornado = sum(vl_Estornado)) %>%
        ungroup()

    return(empenhos_credor_merge)
}
