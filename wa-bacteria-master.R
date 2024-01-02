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
hai_tracker <- args[2]
phx_aws <- args[3]
phx_gs <- args[4]
bb_aws <- args[5]
bb_aws_db <- args[6]

#----- FUNCTIONS -----#
# function for syncing files from AWS
aws_sync <- function(s3_path, pattern, outdir){
    cmd <- paste0('aws s3 sync ',s3_path,' ',outdir,' --exclude "*" --include "',pattern,'"')
    cat(paste0("CMD: ",cmd,"\n"))
    system(cmd)
}
# function for merging synced TSV files from AWS
aws_sync_merge <- function(s3_paths, outdir, pattern){
  s3_paths <- str_split(s3_paths, pattern = ",") %>% 
    unlist()
  lapply(s3_paths, FUN = aws_sync, outdir = outdir, pattern = pattern)

  load_files <- function(file){
    df <- read_tsv(file, show_col_types = F)
  }
  files <- list.files(outdir, pattern = pattern, recursive = T, full.names = T)
  result <- do.call(rbind, lapply(files, FUN = load_files)) %>%
    unique()

  return(result)
}

# function for syncing files from Google Cloud
gs_sync_merge <- function(gs_paths, outdir, pattern, n_cols){
  gs_sync <- function(gs_path){
    cmd <- paste0('gsutil rsync -r -x "',gs_pattern,'" ',gs_path,' ',outdir)
    cat(paste0("CMD: ",cmd,"\n"))
    system(cmd)
  }
  gs_paths <- str_split(gs_paths, pattern = ",") %>% 
    unlist()
  gs_pattern = paste0('^(?!.*',pattern,'$).*')
  lapply(gs_paths, FUN = gs_sync)

  load_files <- function(file){
    df <- read_tsv(file, show_col_types = F)
    if(ncol(df) == n_cols){
        return(df)
    }
  }
  files <- list.files(outdir, pattern = pattern, recursive = T, full.names = T)
  result <- do.call(rbind, lapply(files, FUN = load_files)) %>%
    unique()

  return(result)
}

#----- LOAD FILES -----#
# Create temp directory to store intermediate files
dir.create("tmp")
# EPI DATA
## Manually entered
df.epi_man <- read_excel(epi_data) %>%
  mutate(ID = case_when(is.na(ALT_ID) ~ WA_ID,
                        TRUE ~ ALT_ID),
        EPI = "COMPLETE"
         )

## HAI Tracker
df.epi_hai <-  read_excel(hai_tracker, sheet = "Tracker", skip = 8, guess_max = 10000) %>%
  rename(WA_ID = 2,
         ALT_ID = 3,
         LAB_SPECIES = 4,
         STATE = 7) %>%
  select(WA_ID, ALT_ID, LAB_SPECIES, STATE) %>%
  mutate(SUBMITTER = NA,
         SEQ_LAB = "ARLN",
         SAMPLE_TYPE = "CLINICAL",
         COLLECTION_DATE = NA,
         ID = case_when(is.na(ALT_ID) ~ WA_ID,
                        TRUE ~ ALT_ID),
         EPI = "COMPLETE") %>%
  filter(STATE == "WA")

## Combined
df.epi <- rbind(df.epi_man, df.epi_hai) %>%
  unique() %>%
  mutate(EPI = "COMPLETE")

write.csv(df.epi, file = "epi.csv", row.names = F, quote = F)

# PHOENIX
df.phx_aws <- aws_sync_merge(s3_paths = phx_aws, outdir = "phx_aws", pattern = "*_summaryline.tsv") %>%
    mutate(ID = str_remove_all(ID, pattern = "-WA.*")) %>%
    rename(PHOENIX_QC = Auto_QC_Outcome, 
           PHOENIX_SPECIES = Species, 
           PHOENIX_QC_REASON = Auto_QC_Failure_Reason, 
           TAXA_CONFIDENCE = Taxa_Confidence) %>%
    unique() %>%
    mutate(PHX_RUN_LOC = "AWS")

## Terra output
df.phx_gs <- gs_sync_merge(gs_paths = phx_gs, outdir = "tmp/", pattern = '_summaryline.tsv', n_col = 24) %>%
    mutate(ID = str_remove_all(ID, pattern = "-WA.*")) %>%
    rename(PHOENIX_QC = Auto_QC_Outcome, 
           PHOENIX_SPECIES = Species, 
           PHOENIX_QC_REASON = Auto_QC_Failure_Reason, 
           TAXA_CONFIDENCE = Taxa_Confidence) %>%
    unique() %>%
    mutate(PHX_RUN_LOC = "TERRA")

## Combined
df.phx <- rbind(df.phx_aws, df.phx_gs) %>%
  unique() %>%
  mutate(PHX = "COMPLETE")
write.csv(df.phx, file = "phoenix.csv", row.names = F, quote = F)

# BIGBACTER
## load BigBacter results
df.bb <- aws_sync_merge(s3_paths = bb_aws, outdir = "bb_aws", pattern = "*-summary.tsv") %>%
    mutate(ID = str_remove_all(ID, pattern = "-WA.*"),
           BB = "COMPLETE") %>%
    subset(STATUS == "NEW") %>%
    select(ID, QUAL, RUN_ID, CLUSTER,BB) %>%
    rename(BIGBACTER_QC = QUAL, BIGBACTER_RUN = RUN_ID) %>%
    group_by(ID) %>%
    top_n(1, as.numeric(BIGBACTER_RUN))

write.csv(df.bb, file = "bigbacter.csv", row.names = F, quote = F)

## get list of species with BigBacter DBs
bb_species <- system(paste0("aws s3 ls ",bb_aws_db," | grep 'PRE' | sed 's/.*PRE//g' | tr -d '/'"), intern= T) %>%
  split(" ") %>%
  .$` ` %>%
  str_remove_all(pattern = " ") %>%
  str_replace_all(pattern = "_", replacement = " ")

# NCBI
## pull data using BigQuery
dir.create("ncbi")
bigquery <- function(id){
  id_quote <- paste0('"',id,'"')
  file <- paste0('ncbi/',id,".json")
  query <- paste0("'SELECT * FROM \`ncbi-pathogen-detect.pdbrowser.isolates\` AS isolates, UNNEST(isolates.isolate_identifiers) AS identifier WHERE identifier = ",id_quote,"'")
  cmd <- paste0('bq query --nouse_legacy_sql --format=prettyjson ',query,' > ',file)
  cat(cmd, sep = "\n")
  system(command = cmd, intern = T)

  # remove file if empty
  if(read_lines(file) == "[]"){
    file.remove(file)
  }

}
past_ids <- list.files("ncbi/") %>% str_remove_all(pattern = ".json")
ids <- df.epi %>% 
  drop_na(ALT_ID) %>%
  .$ALT_ID %>%
  unique()

ids <- ids[!(ids %in% past_ids)]
if(length(ids) > 0){
  #dev_null <- lapply(ids, FUN = bigquery)
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
#tmp <- lapply(files.ncbi, FUN=load_ncbi)
#df.ncbi <- do.call(rbind, tmp) %>%
#  drop_na(ID)
#df.ncbi <- apply(df.ncbi, 2, FUN = str_replace_all, pattern = ",", replacement = ";") %>%
#  data.frame()
#write.csv(x = df.ncbi, file = "bigquery.csv", quote = F, row.names = F)

#----- MERGE FILES -----# - without NCBI for now
# join epi & phoenix
df.epi_phx <- df.epi %>%
  merge(df.phx, by = "ID", all.x = T)

# join bigbacter
df.epi_phx_bb <- merge(df.epi_phx, df.bb, by = "ID", all.x = T)

# join NCBI
#df.epi_phx_bb_ncbi <- merge(df.epi_phx_bb, df.ncbi, by = "ID", all.x = T)
df.epi_phx_bb_ncbi <- df.epi_phx_bb

# replace all commas with semicolons
df.epi_phx_bb_ncbi <- apply(df.epi_phx_bb_ncbi, 2, FUN = str_replace_all, pattern = ",", replacement = ";") %>%
  data.frame()

# clean up data for master
master <- df.epi_phx_bb_ncbi %>%
  mutate(STATUS = case_when(is.na(PHOENIX_QC) ~ "PHOENIX_QUEUE",
                            is.na(BIGBACTER_QC) & PHOENIX_QC == "PASS" ~ "BIGBACTER_QUEUE",
                            PHOENIX_QC == "FAIL" ~ "PHOENIX_FAIL",
                            BIGBACTER_QC == "FAIL" ~ "BIGBACTER_FAIL",
                            TRUE ~ "COMPLETE"
                            )
         ) %>%
  select(ID, 
         WA_ID, 
         ALT_ID, 
         STATUS, 
         PHOENIX_QC, 
         BIGBACTER_QC, 
         LAB_SPECIES, 
         PHOENIX_SPECIES, 
         TAXA_CONFIDENCE, 
         CLUSTER, 
         MLST_1, 
         MLST_2, 
         SEQ_LAB, 
         SAMPLE_TYPE, 
         COLLECTION_DATE, 
         SUBMITTER, 
         BIGBACTER_RUN,PHOENIX_QC_REASON, 
         GAMMA_Beta_Lactam_Resistance_Genes, 
         GAMMA_Other_AR_Genes, 
         AMRFinder_Point_Mutations, 
         Hypervirulence_Genes, 
         Plasmid_Incompatibility_Replicons, 
         #Run, 
         #asm_acc, 
         #bioproject_acc, 
         #collection_date, 
         #epi_type, 
         #isolation_source, 
         #mindiff, 
         #minsame, 
         #scientific_name, 
         #erd_group,
         EPI,
         PHX,
         BB) %>%
         unique()

#----- FETCH MOST RECENT BIGBACTER FILES -----#
# create list of most recent files for each cluster within each species
latest.bb <- master %>%
  drop_na(CLUSTER) %>%
  group_by(PHOENIX_SPECIES, CLUSTER) %>%
  summarize(last_run = max(BIGBACTER_RUN)) %>%
  mutate(species = str_replace_all(PHOENIX_SPECIES, pattern = " ", replacement = "_"),
         species_cluster = paste(species,CLUSTER,sep = "-"),
         s3_path_species = file.path(gsub("/$", "", bb_aws), last_run, species),
         s3_path_cluster = file.path(gsub("/$", "", bb_aws), last_run, species, CLUSTER),
         local_path_species = file.path("bb_files", species),
         local_path_cluster = file.path("bb_files", species, CLUSTER))

get_latest_bb_files <- function(sc){
  # function for downloading the relevant files
  sync_files <- function(){
    ## image files
    aws_sync(s3_path = df$s3_path_cluster, pattern = "*.jpg", outdir = df$local_path_cluster)
    ## newick files
    aws_sync(s3_path = file.path(df$s3_path_cluster,"variants","core"), pattern = "*.nwk", outdir = df$local_path_cluster)
    ## alignment files
    aws_sync(s3_path = file.path(df$s3_path_cluster,"variants","core"), pattern = "*.aln", outdir = df$local_path_cluster)
    ## distance matrix
    aws_sync(s3_path = file.path(df$s3_path_cluster,"variants","core"), pattern = "*.dist", outdir = df$local_path_cluster)
    ## microreact files
    aws_sync(s3_path = file.path(df$s3_path_species, "poppunk"), pattern = "*.microreact", outdir = df$local_path_species)
  }

  # filter based on species and cluster
  df <- latest.bb %>%
    filter(species_cluster == sc)

  # check if the species/cluster directory exist and list files, otherwise make the directory
  if(file.exists(df$local_path_cluster)){
    # extract run ID from file names
    current_id <- list.files(df$local_path_cluster, pattern = "*") %>%
      substr(start = 1, stop = 10) %>%
      as.numeric() %>%
      na.omit() %>%
      unique()
    
    # check if the current file is up to date
    if(df$last_run == current_id){
        cat(paste0(df$local_path_cluster, " is up to date. No further action taken.\n"))
    }else{
        cat(paste0(df$local_path_cluster, " is behind. Updating with new files:\n"))
        unlink(df$local_path_cluster, recursive = T)
        dir.create(df$local_path_cluster, recursive = T)
        sync_files()
    }
  }else{
    dir.create(df$local_path_cluster, recursive = T)
    sync_files()
  }
  
}

dev_null <- lapply(latest.bb$species_cluster, FUN = get_latest_bb_files)

#----- SAMPLE CHECK -----#
## Missing from PHoeNIx dataset
phx_miss <- master %>%
  subset(is.na(PHX))

write.csv(phx_miss, file = "phoenix-miss.csv", row.names = F, quote = F)

if(nrow(phx_miss) > 0){
  cat("\nTHESE SAMPLES ARE MISSING FROM THE PHOENIX DATASET:", sep = "\n")
  phx_miss %>%
    select(ID, EPI, PHX, BB) %>%
    kable() %>%
    cat(sep = "\n")
}
## Missing from BigBacter dataset
### Missing but has database
bb_miss <- master %>%
  subset(PHOENIX_SPECIES %in% bb_species) %>%
  subset(is.na(BB) & PHOENIX_QC == "PASS")

if(nrow(bb_miss) > 0){
  cat("\nTHESE SAMPLES ARE MISSING FROM THE BIGBACTER DATASET:", sep = "\n")
  bb_miss %>%
    select(ID, EPI, PHX, BB) %>%
    kable() %>%
    cat(sep = "\n")
}

write.csv(bb_miss, file = "bigbacter-miss.csv", row.names = F, quote = F)

### Missing but does not have database
bb_miss_db <- master %>%
  subset(!(PHOENIX_SPECIES %in% bb_species)) %>%
  subset(is.na(BB) & PHOENIX_QC == "PASS") %>%
  group_by(PHOENIX_SPECIES) %>%
  count()

write.csv(bb_miss_db, file = "bigbacter-miss-db.csv", row.names = F, quote = F)

if(nrow(bb_miss_db) > 0){
  cat("\nTHESE SPECIES DO NO HAVE A BIGBACTER DATABASE:", sep = "\n")
  bb_miss_db %>%
    arrange(desc(n)) %>%
    kable() %>%
    cat(sep = "\n")
}

## duplicated samples
dup <- master %>%
  group_by(ID) %>%
  count() %>%
  subset(n > 1) 

write.csv(dup, file = "dup-samples.csv", row.names = F, quote = F)

if(nrow(dup) > 0){
  cat("\nTHESE SAMPLES ARE DUPLICATED:", sep = "\n")
  dup %>%
    arrange(n) %>%
    kable() %>%
    cat(sep = "\n")
}

#----- WRITE TO MASTER -----#
write.csv(x = master, file = "wa-bacteria-master.csv", quote = F, row.names = F)

#----- TERRA SAMPLES FOR BIGBACTER -----#
#bb_queue <- master %>%
#  data.frame() %>%
#  subset(PHOENIX_QC == "PASS" & STATUS == "BIGBACTER_QUEUE")

#df.phx_gs[df.phx_gs$ID %in% bb_queue$ID,] %>%
#  select(ID, PHOENIX_SPECIES, assembly, fastq_1, fastq_2) %>%
#  rename(taxa = PHOENIX_SPECIES) %>%
#  mutate(taxa = str_replace_all(taxa, pattern = " ", replacement = "_")) %>%
#  write.csv(file = "terra-samples-for-bigbacter.csv", quote = F, row.names = F)
