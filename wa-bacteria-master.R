#!/usr/bin/env Rscript


#----- LIBRARIES -----#
if(!require(tidyverse)){
    install.packages("tidyverse")
    suppressPackageStartupMessages(library(tidyverse))
}
if(!require(readxl)){
    install.packages("readxl")
    suppressPackageStartupMessages(library(readxl))
}
if(!require(tidyjson)){
    install.packages("tidyjson")
    suppressPackageStartupMessages(library(tidyjson))
}
if(!require(rjson)){
    install.packages("rjson")
    suppressPackageStartupMessages(library(rjson))
}
if(!require(knitr)){
    install.packages("knitr")
    suppressPackageStartupMessages(library(knitr))
}

#----- ARGUMENTS -----#
args <- commandArgs(trailingOnly=TRUE)
epi_data <- args[1]
phx_data <- args[2]
bigb_data <- args[3]

#----- LOAD FILES -----#
## EPI DATA
df.epi <- read_excel(epi_data) %>%
  mutate(ID = case_when(is.na(ALT_ID) ~ WA_ID,
                        TRUE ~ ALT_ID),
        EPI = TRUE
         )

## PHOENIX
load_phoenix <- function(file){
  df <- read_tsv(file, col_types = cols()) %>%
    mutate(ID = str_remove_all(ID, pattern = "-WAPHL-.*")) %>%
    rename(PHOENIX_QC = Auto_QC_Outcome, PHOENIX_SPECIES = Species, PHOENIX_QC_REASON = Auto_QC_Failure_Reason, TAXA_CONFIDENCE = Taxa_Confidence)
}
files.phx <- list.files(phx_data, pattern = ".tsv", full.names = T)
tmp <- lapply(files.phx, FUN=load_phoenix)
df.phx <- do.call(rbind, tmp) %>%
  mutate(PHX = TRUE)

## BIGBACTER
load_bigbacter <- function(file){
  df <- read_tsv(file, col_types = cols()) %>%
    subset(STATUS == "NEW") %>%
    select(ID, QUAL, RUN_ID, CLUSTER) %>%
    rename(BIGBACTER_QC = QUAL, BIGBACTER_RUN = RUN_ID)
}
files.bb <- list.files(bigb_data, pattern = ".tsv", full.names = T)
tmp <- lapply(files.bb, FUN=load_bigbacter)
df.bb <- do.call(rbind, tmp) %>%
  mutate(BB = TRUE)

## NCBI
### pull data using BigQuery
ids <- df.epi %>% 
  drop_na(ALT_ID) %>%
  mutate(ALT_ID = paste0('"',ALT_ID,'"')) %>%
  .$ALT_ID %>%
  paste(collapse = ",")
query <- paste0("'SELECT * FROM `ncbi-pathogen-detect.pdbrowser.isolates` AS isolates, UNNEST(isolates.isolate_identifiers) AS identifier WHERE identifier IN (",ids,")'")
system(command = paste0('bq query --nouse_legacy_sql --format=prettyjson ',query,' > bigquery.json'), intern = T) #%>%
df.ncbi <-  fromJSON(file = 'bigquery.json') %>%
  spread_all() %>%
  data.frame() %>%
  select(identifier, Run, asm_acc, bioproject_acc, collection_date, epi_type, isolation_source, mindiff, minsame, scientific_name, erd_group) %>%
  rename(ID = identifier)


#----- MERGE FILES -----# - without NCBI for now
# join epi & phoenix
df.epi_phx <- df.epi %>%
  merge(df.phx, by = "ID", all = T)

# join bigbacter
df.epi_phx_bb <- merge(df.epi_phx, df.bb, by = "ID", all = T)

# join NCBI
df.epi_phx_bb_ncbi <- merge(df.epi_phx_bb, df.ncbi, by = "ID", all = T)

# check for problems - excludes NCBI because we expect there to be missing samples
## Missing from epi dataset
epi_miss <- df.epi_phx_bb_ncbi %>%
  subset(is.na(EPI)) %>%
  select(ID, EPI, PHX, BB)

if(nrow(epi_miss) > 0){
  cat("\nTHESE SAMPLES ARE MISSING FROM THE EPI DATASET:", sep = "\n")
  epi_miss %>%
    kable() %>%
    cat(sep = "\n")
}
## Missing from PHoeNIx dataset
phx_miss <- df.epi_phx_bb_ncbi %>%
  subset(is.na(PHX)) %>%
  select(ID, EPI, PHX, BB)

if(nrow(phx_miss) > 0){
  cat("\nTHESE SAMPLES ARE MISSING FROM THE PHOENIX DATASET:", sep = "\n")
  phx_miss %>%
    kable() %>%
    cat(sep = "\n")
}
## Missing from BigBacter dataset
bb_miss <- df.epi_phx_bb_ncbi %>%
  subset(is.na(BB)) %>%
  select(ID, EPI, PHX, BB)

if(nrow(bb_miss) > 0){
  cat("\nTHESE SAMPLES ARE MISSING FROM THE BIGBACTER DATASET:", sep = "\n")
  bb_miss %>%
    kable() %>%
    cat(sep = "\n")
}

## duplicated samples
dup <- df.epi_phx_bb_ncbi %>%
  group_by(ID) %>%
  count() %>%
  subset(n > 1) 
if(nrow(dup) > 0){
  cat("\nTHESE SAMPLES ARE DUPLICATED:", sep = "\n")
  dup %>%
    arrange(n) %>%
    kable() %>%
    cat(sep = "\n")
}

# clean up data
master <- df.epi_phx_bb_ncbi %>%
  mutate(STATUS = case_when(is.na(PHOENIX_QC) ~ "PHOENIX_QUEUE",
                            is.na(BIGBACTER_QC) & PHOENIX_QC == "PASS" ~ "BIGBACTER_QUEUE",
                            PHOENIX_QC == "FAIL" ~ "PHOENIX_FAIL",
                            BIGBACTER_QC == "FAIL" ~ "BIGBACTER_FAIL",
                            TRUE ~ "COMPLETE"
                            )
         ) %>%
  select(ID, WA_ID, ALT_ID, STATUS, PHOENIX_QC, BIGBACTER_QC, LAB_SPECIES, PHOENIX_SPECIES, TAXA_CONFIDENCE, CLUSTER, MLST_1, MLST_2, SEQ_LAB, SAMPLE_TYPE, COLLECTION_DATE, SUBMITTER, BIGBACTER_RUN,PHOENIX_QC_REASON, GAMMA_Beta_Lactam_Resistance_Genes, GAMMA_Other_AR_Genes, AMRFinder_Point_Mutations, Hypervirulence_Genes, Plasmid_Incompatibility_Replicons, Run, asm_acc, bioproject_acc, collection_date, epi_type, isolation_source, mindiff, minsame, scientific_name, erd_group)
# replace all commas with semicolons
master <- apply(master, 2, FUN = str_replace_all, pattern = ",", replacement = ";")

# save master
write.csv(x = master, file = "wa-bacteria-master.csv", quote = F, row.names = F)
