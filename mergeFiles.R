
setwd("C:/Users/John/Desktop/Desktop/Proj4Data")

# library(data.table)

# Get the path to all data files
files <- list.files(list.dirs(), full.names = T)

# Ignore the empty "SUCCESS" files
files <- files[grepl("part", files)]

# Initialize an empty data frame
dt <- data.frame()

for (f in files) { # For each file,

      contents <- readLines(f) # Read the lines,
      contents <- strsplit(contents, "\t") # Split on the tab delimiter
      
      contents <- data.frame(Reddit = sapply(contents, function(x) x[1]), # Get the first element of the list (subreddit name)
                             Title = sapply(contents, function(x) x[2])) # Get the second element of the list (post title)
      
      dt <- rbind(dt, contents) # Append to dataframe
}

write.table(dt, file = "C:/Users/John/Google Drive/KU Leuven/4th Semester/Big Data/Project 4/proj4data_merged.txt", 
            sep = "\t", row.names = F, col.names = T)

# Difficulty to be mentioned in the report:
# We had challenges merging all the files outputted from Spark
#     .coalesce only works on an rdd, our stream is class: DStream
#     merging all the files via Python results in strange characters being added to the files
#     trouble getting CURL to download/work on the virtual machine
#     most R functions struggled to read the raw data files as well, (such as read.table, read.txt and fread from the data.table package)
#     Resorted to using the most abstract reading function, readLines, then explicitly splitting the string on the tab character
#           , then iterating over the lists and accessing the first and second element respectively (reddit and post title)