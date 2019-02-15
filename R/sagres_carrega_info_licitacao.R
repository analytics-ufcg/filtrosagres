# data_inicio = dt_Inicio, n_licitacoes_venceu = qt_LicitacoesVenceu, montante_lic_venceu = vl_TotalLicitacoesVenceu,
# n_licitacoes_part = qt_LicitacoesParticipou,
# n_licitacoes_part_venceu = qt_LicitacoesPartVenceu, perc_vitoria = pr_Vitorias,
# n_licitacoes = qt_LicitacoesParticipou/qt_LicitacoesGanhou, media_n_licitacoes_part = md_LicitacoesParticipou, media_n_licitacoes_venceu = md_LicitacoesVenceu
#' @title carrega_info_licitacao
#' @description Carrega a informações de licitações associadas a CNPJ's participantes da licitação.
#'     O dataframe retornado conta com as seguintes variáveis:
#'    \describe{
#'         \item{nu_CPFCNPJ}{Código do credor (CPF, CNPJ)}
#'         \item{dt_Inicio}{Data em que o contrato foi iniciado}
#'         \item{qt_LicitacoesParticipou}{Quantidade de licitações em que o credor participou até a data de um contrato}
#'         \item{qt_LicitacoesVenceu}{Quantidade de licitações em que o credor venceu até a data de um contrato}
#'         \item{vl_TotalLicitacoesVenceu}{O valor total de todas as licitações vencidas pelo credor até a data de um contrato}
#'         \item{pr_Vitorias}{A razão entre a quantidade de vitórias e a quantidade de participações do credor até a data de um contrato}
#'         \item{md_LicitacoesParticipou}{Quantidade média anual de licitações em que o credor participou até a data de um contrato}
#'         \item{md_LicitacoesVenceu}{Quantidade média anual de licitações em que o credor venceu até a data de um contrato}
#'     }
#' @param ano_inicial Ano inicial do intervalo de tempo (limite inferior). Default é 2011.
#' @param ano_final Ano final do intervalo de tempo (limite superior). Default é 2016.
#' @param cnpjs_datas_contratos Lista de CNPJ's e de datas de início de contrato para o cálculo das informações de licitação
#' @return Data frame com informações da licitação. Para cada empresa (CNPJ) informações sobre a quantidade de licitações e o montante de dinheiro envolvido.
#' @export
carrega_info_licitacao <- function(ano_inicial = 2011, ano_final = 2016, cnpjs_datas_contratos) {
    ano_inicial_sagres <- 2003
    ## Carrega licitações, propostas e vencedores das licitações
    licitacoes <- carrega_licitacoes(ano_inicial_sagres, ano_final)

    lista_cnpjs <- cnpjs_datas_contratos %>%
      distinct(nu_CPFCNPJ) %>%
      pull(nu_CPFCNPJ)

    participacoes <- carrega_participantes(lista_cnpjs)

    licitacoes_vencedores <- carrega_licitacoes_vencedores(ano_inicial_sagres, ano_final)

    ## Cruza as propostas para licitações (a partir da lista de cnpjs) com todas as licitações do sagres usando a chave de licitação
    licitacoes_fornecedor <- participacoes %>%
        select(-dt_Ano) %>%
        inner_join(licitacoes, by = c("cd_UGestora", "nu_Licitacao", "tp_Licitacao")) %>%
        mutate(dt_Homologacao = as.Date(dt_Homologacao, "%Y-%m-%d"))

    ## Filtra licitações deixando apenas aquelas com data de homologação anterior a data de início do contrato
    licitacoes_fornecedor_data <- cnpjs_datas_contratos %>%
        left_join(licitacoes_fornecedor, by = c("nu_CPFCNPJ")) %>%
        rowwise() %>%
        filter(dt_Homologacao < dt_Inicio) %>%
        ungroup()

    ## Calcula informações de vitória em licitações para os cnpjs das empresas
    licitacoes_fornecedor_vencedores <- cnpjs_datas_contratos %>%
        left_join(licitacoes_vencedores, by = c("nu_CPFCNPJ" = "cd_Credor")) %>%
        filter(min_dt_Empenho < dt_Inicio) %>%
        left_join(licitacoes %>% select(cd_UGestora, nu_Licitacao, tp_Licitacao, vl_Licitacao),
                  by = c("cd_UGestora", "nu_Licitacao", "tp_Licitacao")) %>%
        filter(nu_Licitacao != "000000000") %>%

        group_by(nu_CPFCNPJ, dt_Inicio) %>%
        summarise(qt_LicitacoesVenceu = n_distinct(cd_UGestora, nu_Licitacao, tp_Licitacao),
                  vl_TotalLicitacoesVenceu = sum(vl_Licitacao)) %>%
        ungroup()

    ## Cruza as features dos vencedores com todas as licitações (que serão usadas nas features)
    licitacoes_completa <- licitacoes_fornecedor_data %>%
        left_join(licitacoes_fornecedor_vencedores, by = c("nu_CPFCNPJ", "dt_Inicio")) %>%
        mutate(qt_LicitacoesVenceu = replace_na(qt_LicitacoesVenceu, 0),
               vl_TotalLicitacoesVenceu = replace_na(vl_TotalLicitacoesVenceu, 0))

    ## Calcula o número de licitações ganhas pela empresa até uma data limite
    n_licitacoes_empresa_ganhou <- licitacoes_fornecedor_vencedores %>%
        select(nu_CPFCNPJ, dt_Inicio, qt_LicitacoesVenceu)

    ## Calcula o número de licitações que a empresa participou até uma data limite
    n_licitacoes_empresa_participou <- licitacoes_fornecedor_data %>%
        group_by(nu_CPFCNPJ, dt_Inicio) %>%
        summarise(qt_LicitacoesParticipou = n_distinct(cd_UGestora, nu_Licitacao, tp_Licitacao))

    ## Calcula o número de licitações que a empresa participou e venceu parte1
    licitacoes_empresa_ganhou <- cnpjs_datas_contratos %>%
        left_join(licitacoes_vencedores, by = c("nu_CPFCNPJ" = "cd_Credor")) %>%
        filter(min_dt_Empenho < dt_Inicio) %>%
        filter(nu_Licitacao != "000000000")

    ## Calcula o número de licitações que a empresa participou e venceu parte2
    n_licitacoes_empresa_ganhou_participou <- licitacoes_fornecedor_data %>%
        select(nu_CPFCNPJ, dt_Inicio, cd_UGestora, nu_Licitacao, tp_Licitacao, vl_Licitacao) %>%
        inner_join(licitacoes_empresa_ganhou, by = c("nu_CPFCNPJ", "dt_Inicio", "cd_UGestora", "nu_Licitacao", "tp_Licitacao")) %>%
        group_by(nu_CPFCNPJ, dt_Inicio) %>%
        summarise(qt_LicitacoesPartVenceu = n_distinct(cd_UGestora, nu_Licitacao, tp_Licitacao))

    perc_licitacoes_associadas <- cnpjs_datas_contratos %>%
        left_join(n_licitacoes_empresa_participou, by = c("nu_CPFCNPJ", "dt_Inicio")) %>%
        left_join(n_licitacoes_empresa_ganhou, by = c("nu_CPFCNPJ", "dt_Inicio")) %>%
        left_join(n_licitacoes_empresa_ganhou_participou, by = c("nu_CPFCNPJ", "dt_Inicio")) %>%
        mutate_at(.funs = funs(replace_na(., 0)), .vars = vars(starts_with("qt_Licitacoes"))) %>%
        mutate(pr_Vitorias = qt_LicitacoesVenceu / (qt_LicitacoesVenceu + (qt_LicitacoesParticipou - qt_LicitacoesPartVenceu))) %>%
        mutate_at(.funs = funs(replace_na(., 0)), .vars = vars(starts_with("pr_Vitorias")))

    licitacoes_features <- licitacoes_completa %>%
        group_by(nu_CPFCNPJ, dt_Inicio) %>%
        summarise(qt_LicitacoesParticipou = n_distinct(cd_UGestora, nu_Licitacao, tp_Licitacao),
                  qt_LicitacoesVenceu = first(qt_LicitacoesVenceu),
                  vl_TotalLicitacoesVenceu = first(vl_TotalLicitacoesVenceu)) %>%
        ungroup() %>%
        select(nu_CPFCNPJ, dt_Inicio, qt_LicitacoesParticipou, qt_LicitacoesVenceu, vl_TotalLicitacoesVenceu)

    ## Cálculo das médias de licitações que a empresa participou por ano e da média de licitações que a empresa tem empenhos associados
    media_licitacoes_part_empresa <- licitacoes_fornecedor_data %>%
        group_by(nu_CPFCNPJ, dt_Inicio, dt_Ano) %>%
        summarise(qt_LicitacoesParticipou = n_distinct(cd_UGestora, nu_Licitacao, tp_Licitacao)) %>%
        ungroup() %>%

        group_by(nu_CPFCNPJ, dt_Inicio) %>%
        summarise(md_LicitacoesParticipou = mean(qt_LicitacoesParticipou)) %>%
        ungroup()

    media_licitacoes_venceu_empresa <- licitacoes_empresa_ganhou %>%
        mutate(dt_Ano = as.numeric(substr(min_dt_Empenho, 1, 4))) %>%
        group_by(nu_CPFCNPJ, dt_Inicio, dt_Ano) %>%
        summarise(qt_LicitacoesVenceu = n_distinct(cd_UGestora, nu_Licitacao, tp_Licitacao)) %>%
        ungroup() %>%

        group_by(nu_CPFCNPJ, dt_Inicio) %>%
        summarise(md_LicitacoesVenceu = mean(qt_LicitacoesVenceu)) %>%
        ungroup()

    # Merge das features
    licitacoes_features_merge <- licitacoes_features %>%
        left_join(perc_licitacoes_associadas %>% select(nu_CPFCNPJ, dt_Inicio, pr_Vitorias), by = c("nu_CPFCNPJ", "dt_Inicio")) %>%
        left_join(media_licitacoes_part_empresa, by = c("nu_CPFCNPJ", "dt_Inicio")) %>%
        left_join(media_licitacoes_venceu_empresa, by = c("nu_CPFCNPJ", "dt_Inicio"))

    return(licitacoes_features_merge)
}
