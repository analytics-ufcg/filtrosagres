# Funções principais para manipulação dos dados do banco do SAGRES-PB
#Nenhuma das funções restringe o ano. É preciso fazer isso após carregar os dados


#' Carrega a licitacoes de merenda realizadas no estado da Paraíba.
#' Uma licitação é considerada uma licitação de merenda se pelo menos
#' 80% dos seus empenhos é filtrado como um empenho de merenda.
#' O método de filtragem de empenhos pode ser visto na documentação de
#' \code{\link{load_empenhos_merenda}}
load_licitacoes_merenda <- function(){
  sagres <- src_mysql('sagres', group='ministerio-publico', password=NULL)

  query <- sql('
      select l.*
      from licitacao l
      inner join classificacao_licitacao
      using (cd_UGestora, nu_Licitacao, tp_Licitacao)
      where pr_EmpenhosMerenda >= 0.8
    ')

  licitacoes_merenda <- tbl(sagres, query)

  return(licitacoes_merenda)
}




#' Carrega os empenhos de merenda realizadas no estado da Paraíba.
#' Um empenho é considerado um empenho de merenda quando possui função
#' de 'educação' e subelemento 'gênero alimentício' ou quando possui função
#' de 'educação' e subfunção de 'alimentação e nutrição'
load_empenhos_merenda <- function(){
  sagres <- src_mysql('sagres', group='ministerio-publico', password=NULL)

  query <- sql('
      select *
      from empenhos
      where cd_funcao = 12
      and (cd_subelemento = 02 or cd_SubFuncao = 306)
    ')

  empenhos_merenda <- tbl(sagres, query)

  return(empenhos_merenda)
}




#' Carrega os pagamentos de merenda realizados no estado da Paraíba.
#' Os pagamentos de merenda são os pagamentos efetuados em empenhos
#' classificados como empenhos de merenda. Para saber mais sobre o 
#' método de filtragem de empenhos, consulte \code{\link{load_empenhos_merenda}}
load_pagamentos_merenda <- function(){
  sagres <- src_mysql('sagres', group='ministerio-publico', password=NULL)

  query <- sql('
      select *
      from pagamentos
      inner join (
        select cd_Ugestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho
        from empenhos
        where cd_funcao = 12
        and (cd_subelemento = 02 or cd_SubFuncao = 306)
      ) emm
      using (cd_Ugestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho)
    ')

  pagamentos_merenda <- tbl(sagres, query)

  return(pagamentos_merenda)
}




#' Carrega as liquidações de merenda realizadas no estado da Paraíba.
#' As liquidações de merenda são as liquidações efetuadas em empenhos
#' classificados como empenhos de merenda. Para saber mais sobre o 
#' método de filtragem de empenhos, consulte \code{\link{load_empenhos_merenda}}
load_liquidacoes_merenda <- function(){
  sagres <- src_mysql('sagres', group='ministerio-publico', password=NULL)

  query <- sql('
      select *
      from liquidacao
      inner join (
        select cd_Ugestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho
        from empenhos
        where cd_funcao = 12
        and (cd_subelemento = 02 or cd_SubFuncao = 306)
      ) emm
      using (cd_Ugestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho)
    ')

  liquidacoes_merenda <- tbl(sagres, query)
    

  return(liquidacoes_merenda)
}




#' Carrega os contratos de merenda realizados no estado da Paraíba.
#' Os contratos de merenda são os contratos atrelados a licitações
#' classificadas como licitações de merenda. Para saber mais sobre
#' o método de filtragem de licitações, consulte \code{\link{load_licitacoes_merenda}}
load_contratos_merenda <- function(){
  sagres <- src_mysql('sagres', group='ministerio-publico', password=NULL)

  query <- sql('
      select cd_UGestora, nu_Licitacao, tp_Licitacao
      from licitacao l
      inner join classificacao_licitacao cl
      using (cd_UGestora, nu_Licitacao, tp_Licitacao)
      where pr_EmpenhosMerenda >= 0.8
    ')

  licitacoes_merenda_min <- tbl(sagres, query) %>%
    compute('lmm')

  query <- sql('
      select *
      from contratos
      inner join lmm
      using (cd_Ugestora, nu_Licitacao, tp_Licitacao)
    ')

  contratos_merenda <- tbl(sagres, query)

  return(contratos_merenda)
}




#' Carrega os aditivos de merenda realizados no estado da Paraíba.
#' Os aditivos de merenda são os aditivos atrelados a licitações
#' classificadas como licitações de merenda. Para saber mais sobre
#' o método de filtragem de licitações, consulte \code{\link{load_licitacoes_merenda}}
load_aditivos_merenda <- function(){
  sagres <- src_mysql('sagres', group='ministerio-publico', password=NULL)

  query <- sql('
      select cd_UGestora, nu_Licitacao, tp_Licitacao
      from licitacao l
      inner join classificacao_licitacao cl
      using (cd_UGestora, nu_Licitacao, tp_Licitacao)
      where pr_EmpenhosMerenda >= 0.8
    ')

  licitacoes_merenda_min <- tbl(sagres, query) %>%
    compute('lmm')

  query <- sql('
      select cd_UGestora, nu_Contrato
      from contratos
      inner join lmm
      using (cd_Ugestora, nu_Licitacao, tp_Licitacao)
    ')

  contratos_merenda <- tbl(sagres, query)  %>%
    compute('cmm')

  query <- sql('
      select *
      from aditivos
      inner join cmm
      using (cd_Ugestora, nu_Contrato)
    ')

  aditivos_merenda <- tbl(sagres, query)

  return(aditivos_merenda)
}


load_licitantes_merenda <- function() {
  sagres <- src_mysql('sagres', group='ministerio-publico', password=NULL)
  utils <- src_mysql('utils', group='ministerio-publico', password=NULL)
  
  municipios <- tbl(utils, 'municipio') %>%
    collect()
  
  query <- sql('
      select l.*
      from licitacao l
      inner join classificacao_licitacao
      using (cd_UGestora, nu_Licitacao, tp_Licitacao)
      where pr_EmpenhosMerenda >= 0.8
    ') 
  
  licitacoes <- tbl(sagres, query) %>%
    compute(name = 'lm') %>%
    collect()
  
  contratos <- load_contratos_merenda() %>%
    compute(name = 'cm') %>%
    collect()
  
  aditivos <- load_aditivos_merenda() %>%
    distinct() %>%
    collect()
  
  query <- sql('
               SELECT p.*
               FROM participantes p
               INNER JOIN lm
               USING (cd_UGestora, nu_Licitacao, tp_Licitacao)
               ')
  
  participantes <- tbl(sagres, query) %>%
    compute(name = 'pm') %>%
    collect()
  
  query <- sql('
               SELECT DISTINCT f.*
               FROM fornecedores f
               INNER JOIN pm
               USING (cd_UGestora, nu_CPFCNPJ)
               ')
  
  fornecedores <- tbl(sagres, query) %>%
    collect()
  
  get.municipio <- function(cd_UGestora) {
    result <- data.frame(
      cd_Municipio = str_sub(cd_UGestora, -3)) %>%
      left_join(municipios)
    return(result$de_Municipio)
  }
  
  licitacoes <- licitacoes %>%
    mutate(de_Municipio = get.municipio(cd_UGestora))
  
  participacoes <- participantes %>%
    merge(licitacoes, by=c('cd_UGestora', 'tp_Licitacao', 'nu_Licitacao'), all.x=T) %>%
    group_by(nu_CPFCNPJ) %>%
    summarise(
      participou = n(),
      total = sum(vl_Licitacao),
      mediana = median(vl_Licitacao),
      municipios = n_distinct(de_Municipio))
  
  vitorias <- contratos %>%
    group_by(nu_CPFCNPJ) %>%
    summarise(
      ganhou = n(),
      total_ganho = sum(vl_TotalContrato))
  
  aditivacoes <- contratos %>%
    select(cd_UGestora, nu_Contrato, nu_CPFCNPJ) %>%
    merge(aditivos) %>%
    group_by(nu_CPFCNPJ) %>%
    summarise(
      aditivos = n())
  
  licitantes <- fornecedores %>%
    distinct(nu_CPFCNPJ) %>%
    merge(participacoes, all=T) %>%
    merge(vitorias, all=T) %>%
    merge(aditivacoes, all=T)
  
  licitantes[is.na(licitantes)] <- 0
  
  nomefornecedores <- fornecedores %>%
    select(c(nu_CPFCNPJ, no_Fornecedor)) %>%
    distinct(nu_CPFCNPJ, .keep_all = TRUE)
  
  licitantes <- licitantes %>%
    left_join(nomefornecedores, by = c("nu_CPFCNPJ" = "nu_CPFCNPJ"))
  
  licitantes <- licitantes %>%
    mutate(
      aditivos = ifelse(is.nan(aditivos/ganhou), 0, aditivos/ganhou),
      ganhou = ganhou/participou) %>%
    filter(
      is.finite(ganhou),
      nu_CPFCNPJ != '00000000000000')
  
  return(licitantes)
}

