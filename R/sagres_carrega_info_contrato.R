#' @title Carrega informações de um contrato
#' @description Carrega a informações de contratos associadas a CNPJ's
#'    \describe{
#'         \item{cd_UGestora}{Unidade Gestora de origem}
#'         \item{nu_Contrato}{Número do contrato}
#'         \item{nu_CPFCNPJ}{Código do credor (CPF, CNPJ)}
#'         \item{dt_Início}{Data em que o empenho foi realizado}
#'         \item{pr_ContratoTotalGanho}{Proporção do valor do contrato sobre o valor total ganho pelo credor em todos contratos até a assinatura}
#'         \item{md_Contratos}{Valor médio obtido pelo credor em todos os contratos realizados}
#'         \item{qt_Contratos}{Quantidade de contratos firmados pelo credor}
#'     }
#' @param dados_contrato Informações dos contratos com informando o valor total do contrato e o montante recebido por ano (cd_UGestora, nu_Contrato, nu_CPFCNPJ, dt_Inicio, vl_Contrato, vl_GanhoAnual)
#' @param ano_inicial Ano inicial do intervalo de tempo (limite inferior). Default é 2011.
#' @param ano_final Ano final do intervalo de tempo (limite superior). Default é 2016.
#' @param limite_inferior Menor valor de contrato a ser considerado ao calcular as tipologias
#' @return Data frame com informações do contrato. Ex: Razão entre o valor do contrato e o montante recebido
#' @export
carrega_info_contrato <- function(dados_contrato, ano_inicial = 2011, ano_final = 2016, limite_inferior = 0) {
    ## Soma o valor recebido pela empresa considerando todos os pagamentos anteriores a data de início do contrato
    contratos <- dados_contrato %>%
        select(cd_UGestora, nu_Contrato, nu_CPFCNPJ, dt_Inicio, vl_TotalContrato, vl_Ganho)

    ## Calcula a razão entre o valor do contrato e o total recebido pela empresa
    contratos_razao <- contratos %>%
        mutate(pr_ContratoTotalGanho = vl_TotalContrato / (vl_TotalContrato + vl_Ganho)) %>%
        mutate(pr_ContratoTotalGanho = ifelse(is.infinite(pr_ContratoTotalGanho), NA, pr_ContratoTotalGanho)) %>%
        select(cd_UGestora, nu_Contrato, nu_CPFCNPJ, dt_Inicio, pr_ContratoTotalGanho)

    cnpjs_contratos <- contratos %>%
        distinct(nu_CPFCNPJ, dt_Inicio)

    ano_inicial_sagres <- 2003
    contratos_all <- carrega_contratos(ano_inicial_sagres, ano_final, limite_inferior)

    # Calcula a quantidade de contratos por CNPJ e data (tomando como limite superior a data de início dos contratos para os CNPJ's)
    contratos_merge <- cnpjs_contratos %>%
        left_join(contratos_all, by = c("nu_CPFCNPJ")) %>%
        filter(dt_Assinatura < dt_Inicio) %>%

        group_by(nu_CPFCNPJ, dt_Inicio, dt_Ano) %>%
        summarise(qt_Contratos = n_distinct(cd_UGestora, nu_Contrato)) %>%
        ungroup()

    contratos_data <- contratos_merge %>%
        group_by(nu_CPFCNPJ, dt_Inicio) %>%
        summarise(md_Contratos = mean(qt_Contratos),
                  qt_Contratos = sum(qt_Contratos)) %>%
        ungroup()

    contratos_features <- contratos_razao %>%
        left_join(contratos_data, by = c("nu_CPFCNPJ", "dt_Inicio")) %>%
        mutate_at(.funs = funs(replace_na(., 0)), .vars = vars(starts_with("qt_Contratos"), "md_Contratos"))

    return(contratos_features)
}
