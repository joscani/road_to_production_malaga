#!/usr/bin/env Rscript
# .libPaths("/opt/datos1/utils/srcR/")
args = commandArgs(trailingOnly=TRUE)

zipfile = args[1]
output_path = paste0(args[2],'.html')
fichero_rmd <- 'metricas.Rmd'

# Generar informe automático

tmp_dir <- tempdir()
tmp <- tempfile()


unzip(zipfile = zipfile, exdir = tmp_dir )

fichero_json = paste0(tmp_dir, "/experimental/modelDetails.json")

rmarkdown::render(
  input = fichero_rmd,
  params = list(fichero_json = fichero_json),
  output_file = output_path
)
