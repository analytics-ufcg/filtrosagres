# data_inicio = dt_Inicio, n_propostas = qt_Propostas, media_n_propostas = md_Propostas
#' @title carrega_info_proposta
#' @description Carrega a informações do número de propostas de CNPJS associadas a licitações dentro de um intervalo de tempo.
#'     Retorna um dataframe com as seguintes variáveis:
#'     \describe{
#'         \item{nu_CPFCNPJ}{Código do credor (CPF, CNPJ)}
#'         \item{dt_Inicio}{Data em que o contrato foi iniciado}
#'         \item{qt_Propostas}{Quantidade de propostas feitas pela empresa até a assinatura do contrato}
#'         \item{md_Propostas}{Quantidade média de propostas feitas por ano pela empresa até a assinatura do contrato}
#'     }
#' @param ano_inicial Ano inicial do intervalo de tempo (limite inferior). Default é 2011.
#' @param ano_final Ano final do intervalo de tempo (limite superior). Default é 2016.
#' @param cnpjs_datas_contratos Lista de CNPJ's e de datas de início de contrato para o cálculo das informações das propostas.
#' @return Data frame com informações de quantas propostas foram feitas por uma determinada empresa até uma data limite.
#' @export
carrega_info_proposta <- function(ano_inicial = 2011, ano_final = 2016, cnpjs_datas_contratos) {
    ano_inicial_sagres <- 2003
    propostas <- carrega_propostas_licitacao(ano_inicial_sagres, ano_final)

    propostas_filtradas_fornecedores <- cnpjs_datas_contratos %>%
        left_join(propostas, by = "nu_CPFCNPJ") %>%
        mutate(dt_Homologacao = as.Date(dt_Homologacao, "%Y-%m-%d")) %>%
        filter(dt_Homologacao < dt_Inicio)

    propostas_group <- propostas_filtradas_fornecedores %>%
        group_by(nu_CPFCNPJ, dt_Inicio) %>%
        summarise(qt_Propostas = n_distinct(cd_UGestora, nu_Licitacao, tp_Licitacao, cd_Item, cd_SubGrupoItem, nu_CPFCNPJ)) %>%
        ungroup()

    media_propostas_group <- propostas_filtradas_fornecedores %>%
        group_by(nu_CPFCNPJ, dt_Inicio, dt_Ano) %>%
        summarise(qt_Propostas = n_distinct(cd_UGestora, nu_Licitacao, tp_Licitacao, cd_Item, cd_SubGrupoItem, nu_CPFCNPJ)) %>%
        ungroup() %>%

        group_by(nu_CPFCNPJ, dt_Inicio) %>%
        summarise(md_Propostas = mean(qt_Propostas)) %>%
        ungroup()

    propostas_features <- cnpjs_datas_contratos %>%
        left_join(propostas_group, by = c("nu_CPFCNPJ", "dt_Inicio")) %>%
        left_join(media_propostas_group, by = c("nu_CPFCNPJ", "dt_Inicio")) %>%
        mutate_at(.funs = funs(replace_na(., 0)), .vars = vars(starts_with("qt_Propostas"), starts_with("md_Propostas")))

    return(propostas_features)
}
