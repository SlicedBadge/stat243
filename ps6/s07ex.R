library(foreach)
library(doParallel)
library(stringr)
library(readr)

#setup parallel
nCores <- 20
registerDoParallel(nCores) 

#find files to read in
files = list.files(path = "/global/scratch/paciorek/wikistats_small/dated/", pattern = "part-",
                   full.names=TRUE)

result <- foreach(i = files) %dopar% {
  #read in file to dataframe
  df = read.delim(i, header = FALSE, sep = " ", quote = "")
  #get only the entries with Barrack
  output = df[grep("Barrack_Obama", df$V4),] # this will become part of the out object
  
}
#combine the results into single dataframe
df = do.call("rbind", result)
