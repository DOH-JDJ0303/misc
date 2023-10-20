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
phx_std_data <- args[2]
phx_terra_data <- args[3]
bigb_data <- args[4]

#----- LOAD FILES -----#
## EPI DATA
df.epi <- read_excel(epi_data) %>%
  mutate(ID = case_when(is.na(ALT_ID) ~ WA_ID,
                        TRUE ~ ALT_ID),
        EPI = TRUE
         )

## PHOENIX
# standard output
load_phoenix <- function(file){
  df <- read_tsv(file) %>%
    mutate(ID = str_remove_all(ID, pattern = "-WAPHL-.*")) %>%
    rename(PHOENIX_QC = Auto_QC_Outcome, 
           PHOENIX_SPECIES = Species, 
           PHOENIX_QC_REASON = Auto_QC_Failure_Reason, 
           TAXA_CONFIDENCE = Taxa_Confidence)
}
files.phx <- list.files(phx_std_data, pattern = ".tsv", full.names = T)
tmp <- lapply(files.phx, FUN=load_phoenix)
df.phx_std <- do.call(rbind, tmp)

# Terra output
load_terra_phoenix <- function(file){
  df <- read_tsv(file) %>%
    rename(ID = 1) %>%
    mutate(ID = str_remove_all(ID, pattern = "-WA.*")) %>%
    mutate(Taxa_Coverage = NA) %>%
    select(ID, 
           qc_outcome, 
           warning_count, 
           estimated_coverage, 
           genome_length, 
           assembly_ratio, 
           scaffold_count, 
           gc_percent, 
           species, 
           taxa_confidence,
           Taxa_Coverage,
           taxa_source, 
           kraken2_trimmed, 
           kraken2_weighted,
           mlst_scheme_1,
           mlst_1,
           mlst_scheme_2,
           mlst_2,
           beta_lactam_resistance_genes,
           other_ar_genes,
           amrfinder_point_mutations,
           hypervirulence_genes,
           plasmid_incompatibility_replicons,
           qc_reason,
           assembly,
           trimmed_read1,
           trimmed_read2)
}
files.terra_phx <- list.files(phx_terra_data, pattern = ".tsv", full.names = T)
tmp <- lapply(files.terra_phx, FUN=load_terra_phoenix)
df.phx_terra <- do.call(rbind, tmp)
colnames(df.phx_terra) <- c(colnames(df.phx_std),"assembly","fastq_1","fastq_2")

# Combined
df.phx <- df.phx_terra %>% 
  select(-assembly, -fastq_1, -fastq_2) %>% 
  rbind(df.phx_std) %>%
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
system("mkdir ncbi")
bigquery <- function(id){
  id <- paste0('"',id,'"')
  query <- paste0("'SELECT * FROM \`ncbi-pathogen-detect.pdbrowser.isolates\` AS isolates, UNNEST(isolates.isolate_identifiers) AS identifier WHERE identifier = ",id,"'")
  cmd <- paste0('bq query --nouse_legacy_sql --format=prettyjson ',query,' > ncbi/',id,'.json')
  cat(cmd, sep = "\n")
  system(command = cmd, intern = T)
}
past_ids <- list.files("ncbi/") %>% str_remove_all(pattern = ".json")
ids <- df.epi %>% 
  drop_na(ALT_ID) %>%
  .$ALT_ID %>%
  unique()

ids <- ids[!(ids %in% past_ids)]
if(length(ids) > 0){
  lapply(ids, FUN = bigquery)
}

load_ncbi <- function(file){
  # check if json file is empty
  if(read_lines(file) != "[]"){
    df <- fromJSON(file = file) %>%
      spread_all() %>%
      data.frame() %>%
      select(identifier, Run, asm_acc, bioproject_acc, collection_date, epi_type, isolation_source, mindiff, minsame, scientific_name, erd_group) %>%
      rename(ID = identifier)
  }else{
    df <- data.frame(ID = NA,
                     Run = NA, 
                     asm_acc = NA,
                     bioproject_acc = NA,
                     collection_date = NA,
                     epi_type = NA,
                     isolation_source = NA,
                     mindiff = NA,
                     minsame = NA,
                     scientific_name = NA, 
                     erd_group = NA)
  }

  return(df)
}
files.ncbi <- list.files('ncbi/', pattern = ".json", full.names = T)
tmp <- lapply(files.ncbi, FUN=load_ncbi)
df.ncbi <- do.call(rbind, tmp) %>%
  drop_na(ID)
df.ncbi <- apply(df.ncbi, 2, FUN = str_replace_all, pattern = ",", replacement = ";") %>%
  data.frame()
write.csv(x = df.ncbi, file = "bigquery.csv", quote = F, row.names = F)

#----- MERGE FILES -----# - without NCBI for now
# join epi & phoenix
df.epi_phx <- df.epi %>%
  merge(df.phx, by = "ID", all = T)

# join bigbacter
df.epi_phx_bb <- merge(df.epi_phx, df.bb, by = "ID", all = T)

# join NCBI
df.epi_phx_bb_ncbi <- merge(df.epi_phx_bb, df.ncbi, by = "ID", all = T)

#----- SANITY CHECK -----#
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

#----- WRITE TO MASTER -----#
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

#----- TERRA SAMPLES FOR BIGBACTER -----#
bb_queue <- master %>%
  data.frame() %>%
  subset(PHOENIX_QC == "PASS" & STATUS == "BIGBACTER_QUEUE")

df.phx_terra[df.phx_terra$ID %in% bb_queue$ID,] %>%
  select(ID, PHOENIX_SPECIES, assembly, fastq_1, fastq_2) %>%
  rename(taxa = PHOENIX_SPECIES) %>%
  mutate(taxa = str_replace_all(taxa, pattern = " ", replacement = "_")) %>%
  write.csv(file = "terra-samples-for-bigbacter.csv", quote = F, row.names = F)