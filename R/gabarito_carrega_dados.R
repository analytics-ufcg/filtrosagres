#' @title carrega_cnep
#' @description Carrega do Portal da Transparência os dados de empresas presentes no CNEP cujas sanções foram aplicadas em um intervalo especificado.
#' O dataframe retornado conta com as seguintes variáveis:
#'     \describe{
#'         \item{cd_Sancionado}{CPF ou CNPJ do sancionado}
#'         \item{no_Sancionado}{Nome do sancionado}
#'         \item{dt_Inicio}{Data de início da sanção}
#'         \item{dt_Final}{Data do final da sanção}
#'     }
#' @param ano_inicial Ano em que se inicia o intervalo de busca por sanções
#' @param ano_final Ano em que se finda o intervalo de busca por sanções
#' @return Dataframe com dados do CNEP
carrega_cnep <- function(ano_inicial = 2015, ano_final = 2016) {

  dir_temporario <- tempdir()
  arq_temporario <- tempfile(fileext = ".zip")
  data_versao = format(Sys.time(), "%Y%m%d")

  download.file(paste0("http://www.portaltransparencia.gov.br/download-de-dados/cnep/", data_versao), arq_temporario, "curl")
  arq_descompactado <- unzip(arq_temporario, exdir = dir_temporario)

  cnep <- read_delim(arq_descompactado, ";", escape_double = FALSE,
                        locale = locale(encoding = "ISO-8859-1"), trim_ws = TRUE)

  cnep <- cnep %>%
    filter(between(as.numeric(substr(`DATA INÍCIO SANÇÃO`, 7, 10)), ano_inicial, ano_final)) %>%
    select(cd_Sancionado = `CPF OU CNPJ DO SANCIONADO`,
           no_Sancionado = `NOME INFORMADO PELO ÓRGÃO SANCIONADOR`,
           dt_Inicio = `DATA INÍCIO SANÇÃO`,
           dt_Final = `DATA FINAL SANÇÃO`) %>%
    distinct(cd_Sancionado, .keep_all = TRUE)

  unlink(arq_temporario)
  unlink(arq_descompactado)

  return(cnep)
}


#' @title carrega_ceis
#' @description Carrega do Portal da Transparência os dados de empresas presentes no CEIS cujas sanções foram aplicadas em um intervalo especificado.
#' O dataframe retornado conta com as seguintes variáveis:
#'     \describe{
#'         \item{cd_Sancionado}{CPF ou CNPJ do sancionado}
#'         \item{no_Sancionado}{Nome do sancionado}
#'         \item{dt_Inicio}{Data de início da sanção}
#'         \item{dt_Final}{Data do final da sanção}
#'     }
#' @param ano_inicial Ano em que se inicia o intervalo de busca por sanções
#' @param ano_final Ano em que se finda o intervalo de busca por sanções
#' @return Dataframe com dados do CEIS
carrega_ceis <- function(ano_inicial = 2015, ano_final = 2016) {

  dir_temporario <- tempdir()
  arq_temporario <- tempfile(fileext = ".zip")

  download.file(paste0("http://www.portaltransparencia.gov.br/download-de-dados/ceis/", data_versao), arq_temporario, "curl")
  arq_descompactado <- unzip(arq_temporario, exdir = dir_temporario)

  ceis <- read_delim(arq_descompactado, ";", escape_double = FALSE,
                     locale = locale(encoding = "ISO-8859-1"), trim_ws = TRUE)

  ceis <- ceis %>%
    filter(between(as.numeric(substr(`DATA INÍCIO SANÇÃO`, 7, 10)), ano_inicial, ano_final)) %>%
    select(cd_Sancionado = `CPF OU CNPJ DO SANCIONADO`,
           no_Sancionado = `NOME INFORMADO PELO ÓRGÃO SANCIONADOR`,
           dt_Inicio = `DATA INÍCIO SANÇÃO`,
           dt_Final = `DATA FINAL SANÇÃO`) %>%
    distinct(cd_Sancionado, .keep_all = TRUE)

  unlink(arq_temporario)
  unlink(arq_descompactado)

  return(ceis)
}

#' @title carrega_gabarito_sancoes
#' @description Carrega do Portal da Transparência os dados de empresas presentes no CNEP e no CEIS cujas sanções foram aplicadas em um intervalo especificado.
#' O dataframe retornado conta com as seguintes variáveis:
#'     \describe{
#'         \item{cd_Sancionado}{CPF ou CNPJ do sancionado}
#'         \item{no_Sancionado}{Nome do sancionado}
#'         \item{dt_Inicio}{Data de início da sanção}
#'         \item{dt_Final}{Data do final da sanção}
#'         \item{tp_Origem}{Origem da sanção (CNEP, CEIS)}
#'     }
#' @param ano_inicial Ano em que se inicia o intervalo de busca por sanções
#' @param ano_final Ano em que se finda o intervalo de busca por sanções
#' @return Dataframe com dados do CNEP e do CEIS
carrega_gabarito_sancoes <- function(ano_inicial = 2015, ano_final = 2016) {

  ceis <- carrega_ceis(ano_inicial, ano_final) %>%
    mutate(tp_Origem = "CEIS")

  cnep <- carrega_cnep(ano_inicial, ano_final) %>%
    mutate(tp_Origem = "CNEP")

  gabarito <- ceis %>%
    bind_rows(cnep)

  return(gabarito)
}

