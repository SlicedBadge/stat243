library(foreach)
library(doParallel)
library(stringr)
library(readr)

#setup parallel
parread <- function(){
  nCores <- 24
  registerDoParallel(nCores) 
  
  #find files to read in
  files = list.files(path = "/global/scratch/paciorek/wikistats_full/dated_for_R/", pattern = "part-",
                     full.names=TRUE)
  
  result <- foreach(i = files) %dopar% {
    #read in file to dataframe
    df = read_delim(i, delim = " ", quote = "")
    #get only the entries with Barrack
    output = df[grep("Barrack_Obama", df$V4),] # this will become part of the out object
    
  }
  return(result)
}
system.time(result <- parread())
#combine the results into single dataframe
df = do.call("rbind", result)
