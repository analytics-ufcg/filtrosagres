# Funções principais utilizadas para a manipulação dos dados do banco do SAGRES-PB
# A menos que declarado o contrário na descrição do método, é sempre necessário coletar os dados retornados por esses métodos
# antes de utilizá-los. Isso pode ser feito a partir de uma chamada a dplyr::collect()

#' @title get_empenhos_licitacao_filt
#' @description Retorna os empenhos da licitacao que corresponde aos códigos passados como parâmetro
#' @param dbcon conexão com o banco de dados
#' @param nu_licitacao o número da licitação cujo percentual de aditivos deve ser calculado
#' @param cd_ugestora o código da unidade gestora que lançou a licitação em questão
#' @param tp_licitacao o tipo da licitação em questão
#' @param cd_funcao código de função a ser utilizado na filtragem
#' @param cd_subfuncao código de subfunção a ser utilizado na filtragem
#' @param cd_subelemento código de subelemento a ser utilizado na filtragem. Valor padrão: 99 (Sem subelemento)
#' @export
get_empenhos_licitacao_filt <- function(dbcon, nu_licitacao, cd_ugestora, tp_licitacao,
                                   cd_funcao, cd_subfuncao, cd_subelemento = 99) {
  sql(paste('
              select *
              from Empenhos
              where nu_licitacao =', nu_licitacao,
            ' and cd_ugestora =', cd_ugestora,
            ' and tp_licitacao =', tp_licitacao,
            ' and cd_funcao =', cd_funcao,
            ' and cd_subfuncao =', cd_subfuncao,
            ' and cd_subelemento =', cd_subelemento
            ))

  empenhos <- tbl(dbcon, sql)

  return(empenhos)
}



#' @title get_valor_empenhos_licitacoes
#' @description Retorna o valor total dos empenhos que correspondem aos códigos passados como parâmetro em todas as licitações
#' @param dbcon conexão com o banco de dados
#' @param cd_funcao código de função a ser utilizado na filtragem
#' @param cd_subfuncao código de subfunção a ser utilizado na filtragem
#' @param cd_subelemento código de subelemento a ser utilizado na filtragem. Valor padrão: 99 (Sem subelemento)
get_valor_empenhos_licitacoes <- function(dbcon, cd_funcao, cd_subfuncao, cd_subelemento = 99) {
  query <- sql(paste('
              select l.cd_ugestora, l.nu_licitacao, l.tp_licitacao, sum(e.vl_empenho) as valor_total_codigo
              from Licitacao l, Empenhos e
              where l.nu_licitacao = e.nu_licitacao
              and l.cd_ugestora = e.cd_ugestora
              and l.tp_licitacao = e.tp_licitacao
              and e.cd_funcao =', cd_funcao,
            ' and e.cd_subfuncao =', cd_subfuncao,
            ' and e.cd_subelemento =', cd_subelemento,
            ' group by l.cd_ugestora, l.nu_licitacao, l.tp_licitacao'
  ))

  valor_empenhos <- tbl(dbcon, query)

  return(valor_empenhos)
}



#' @title get_percentual_aditivos
#' @description Calcula o percentual do valor de aditivos que incide sobre uma licitação.
#' Um percentual de exatamente 0 indica que provavelmente o aditivo solicitado para a licitação não foi monetário, mas sim de tempo.
#' Um retorno 'empty' indica que não há aditivos cadastrados para a licitação analisada.
#' ATENÇÃO: Não é necessário realizar o collect do retorno deste método.
#' @param dbcon conexão com o banco de dados
#' @param nu_licitacao o número da licitação cujo percentual de aditivos deve ser calculado
#' @param cd_ugestora o código da unidade gestora que lançou a licitação em questão
#' @param tp_licitacao o tipo da licitação em questão
#' @export
get_percentual_aditivos <- function(dbcon, nu_licitacao, cd_ugestora, tp_licitacao) {

  query <- sql(paste('
                     select a.vl_aditivo/l.vl_licitacao as pr_aditivos
                     from Licitacao l, Contratos c, Aditivos a
                     where l.nu_licitacao =', nu_licitacao,
                    'and l.cd_ugestora =', cd_ugestora,
                    'and l.tp_licitacao =', tp_licitacao,
                    'and l.cd_ugestora = c.cd_ugestora
                     and l.nu_licitacao = c.nu_licitacao
                     and l.tp_licitacao = c.tp_licitacao
                     and c.nu_contrato = a.nu_contrato
                     and c.cd_ugestora = a.cd_ugestora
                     '))

  percentual <- tbl(dbcon, query) %>%
    collect()

  result <- percentual$pr_aditivos

  return(result)
}



#' @title get_empenhos_filtrados
#' @description Filtra os empenhos realizados no estado da Paraíba a partir de seu
#' código de função, código de subfunção e código de subelemento.
#' @param dbcon conexão com o banco de dados
#' @param cd_funcao código de função a ser utilizado na filtragem
#' @param cd_subfuncao código de subfunção a ser utilizado na filtragem
#' @param cd_subelemento código de subelemento a ser utilizado na filtragem. Valor padrão: 99 (Sem subelemento)
#' @export
get_empenhos_filtrados <- function(dbcon, cd_funcao, cd_subfuncao, cd_subelemento = 99){
  query <- sql(paste('
                     select *
                     from Empenhos
                     where cd_funcao =', cd_funcao,
                     'and (cd_subfuncao =', cd_subfuncao,
                     'or cd_subelemento =', cd_subelemento, ')'
  ))

  empenhos <- tbl(dbcon, query)

  return(empenhos)
}




#' @title get_pagamentos_filtrados
#' @description Filtra os pagamentos realizados no estado da Paraíba a partir do
#' código de função, código de subfunção e código de subelementodos empenhos a que eles estão relacionados.
#' @param dbcon conexão com o banco de dados
#' @param cd_funcao código de função a ser utilizado na filtragem
#' @param cd_subfuncao código de subfunção a ser utilizado na filtragem
#' @param cd_subelemento código de subelemento a ser utilizado na filtragem. Valor padrão: 99 (Sem subelemento)
#' @export
get_pagamentos_filtrados <- function(dbcon, cd_funcao, cd_subfuncao, cd_subelemento = 99){
  query <- sql(paste('
                     select *
                     from Pagamentos
                     inner join (
                     select cd_Ugestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho
                     from Empenhos
                     where cd_funcao =', cd_funcao,
                     'and (cd_subelemento =', cd_subelemento ,'or cd_subfuncao =', cd_subfuncao, ')
                     ) emm
                     using (cd_Ugestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho)
                     '))

  pagamentos <- tbl(dbcon, query)

  return(pagamentos)
}




#' @title get_liquidacoes_filtradas
#' @description Filtra as liquidações realizadas no estado da Paraíba a partir do
#' código de função, código de subfunção e código de subelemento dos empenhos a que elas estão relacionadas.
#' @param dbcon conexão com o banco de dados
#' @param cd_funcao código de função a ser utilizado na filtragem
#' @param cd_subfuncao código de subfunção a ser utilizado na filtragem
#' @param cd_subelemento código de subelemento a ser utilizado na filtragem. Valor padrão: 99 (Sem subelemento)
#' @export
get_liquidacoes_filtradas <- function(dbcon, cd_funcao, cd_subfuncao, cd_subelemento = 99){
  query <- sql(paste('
      select *
      from Liquidacao
      inner join (
        select cd_Ugestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho
        from Empenhos
        where cd_funcao =', cd_funcao,
                     'and (cd_subelemento =', cd_subelemento , 'or cd_subfuncao =', cd_subfuncao, ')
      ) emm
      using (cd_Ugestora, dt_Ano, cd_UnidOrcamentaria, nu_Empenho)
    '))

  liquidacoes <- tbl(dbcon, query)


  return(liquidacoes)
}

