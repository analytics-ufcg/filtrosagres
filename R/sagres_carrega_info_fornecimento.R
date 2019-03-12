#' @title Carrega informações de fornecimento
#' @description Calcula informações sobre o fornecimento de uma lista de CNPj's.
#'    \describe{
#'         \item{cd_Credor}{Código do credor (CPF, CNPJ)}
#'         \item{no_Credor}{Nome do credor}
#'         \item{dt_Inicio}{Data em que o contrato foi assinado}
#'         \item{md_Municipios}{Número médio anual de municípios para os quais o credor forneceu serviços até a assinatura do contrato}
#'         \item{qt_Municipios}{Número total de municípios para os quais o credor forneceu serviços até a assinatura do contrato}
#'         \item{md_UGestoras}{Número médio anual de unidades gestoras para os quais o credor forneceu serviços até a assinatura do contrato}
#'         \item{qt_UGestoras}{Número total de unidades gestoras para os quais o credor forneceu serviços até a assinatura do contrato}
#'         \item{md_Ganho}{Valor médio anual ganho pelo credor até a assinatura do contrato}
#'         \item{vl_Ganho}{Valor ganho pelo credor até a assinatura do contrato}
#'     }
#' @param db_user Usuario utilizado para conexão no BD
#' @param ano_inicial Ano inicial do intervalo de tempo
#' @param ano_final Ano final do intervalo de tempo
#' @param cnpjs_datas_contratos Lista de CNPJ's e de datas de início de contrato para o cálculo das informações de fornecimento.
#' @return Data frame com informações sobre as empresas com uma data de limite superior como número de municípios com fornecimento, total de dinheiro recebido.
#' @export
carrega_info_fornecimento <- function(db_user, ano_inicial, ano_final, cnpjs_datas_contratos) {
    datas_lista <- cnpjs_datas_contratos %>%
        distinct(dt_Inicio) %>%
        mutate(dt_Inicio = as.character(dt_Inicio)) %>%
        pull(dt_Inicio)

    ## Ano inicial considerado para o cálculo das informações de empenhos eé o ano inicial do SAGRES.
    ano_inicial_sagres <- 2003
    empenhos_data <- carrega_empenhos_data_limite(db_user, ano_inicial_sagres, ano_final, cnpjs_datas_contratos)

    ## Separa nome dos Credores
    cnpjs_nome <- empenhos_data %>%
        group_by(cd_Credor) %>%
        summarise(no_Credor = first(no_Credor))

    ## Calcula o total ganho por Credor, Data, Unidade Gestora e Ano
    empenhos_group <- empenhos_data %>%
        group_by(cd_Credor, dt_Inicio, cd_UGestora, dt_Ano) %>%
        summarise(vl_Ganho = sum(vl_Pago - replace_na(vl_Estornado, 0))) %>%
        ungroup()

    ## Calcula a Feature do número de municípios agrupando por Credor e Ano
    empenhos_por_fornecedor_ano <- empenhos_group %>%
        mutate(cd_UGestora_copy = cd_UGestora) %>%
        separate(cd_UGestora_copy, c('cd_UGestora_prefix', 'cd_Municipio'), -3) %>%

        group_by(cd_Credor, dt_Inicio, dt_Ano) %>%
        summarise(qt_Municipios = n_distinct(cd_Municipio),
                  qt_UGestoras = n_distinct(cd_UGestora),
                  vl_Ganho = sum(vl_Ganho)) %>%
        ungroup() %>%
        select(cd_Credor, dt_Inicio, dt_Ano, qt_Municipios, qt_UGestoras, vl_Ganho)

    empenhos_features <- empenhos_por_fornecedor_ano %>%
        group_by(cd_Credor, dt_Inicio) %>%
        summarise(md_Municipios = mean(qt_Municipios), # ordem das operações importa. Média calculada apenas para o anos com observações (fornecimento da empresa).
                  qt_Municipios = sum(qt_Municipios),
                  md_UGestoras = mean(qt_UGestoras),
                  qt_UGestoras = sum(qt_UGestoras),
                  md_Ganho = mean(vl_Ganho),
                  vl_Ganho = sum(vl_Ganho)) %>%
        ungroup()

    empenhos_features_nome <- empenhos_features %>%
        left_join(cnpjs_nome, by = "cd_Credor") %>%
        select(cd_Credor, no_Credor, dplyr::everything())

    return(empenhos_features_nome)
}
