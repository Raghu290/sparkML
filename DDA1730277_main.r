#data in kagel - https://www.kaggle.com/new-york-city/nyc-parking-tickets/data
#Transfer data from kagel to EC2 mster - https://stackoverflow.com/questions/45261190/how-to-get-kaggle-competition-data-via-command-line-on-virtual-machine
#then transfer data from ec2 to s3

#loading SparkR into RStudio
Sys.getenv()
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))

### load SparkR
library(SparkR)

### initiating the spark session
sparkR.session(master='local')

### reading all the three data frames

# 2015 dataframe
parking_violations_issued_2015 <- read.df("s3://pk-nyc-parking-tickets/dataset/Parking_Violations_Issued_-_Fiscal_Year_2015.csv",
                                          source = "csv", inferSchema = "true", header = "true")

# 2016 dataframe
parking_violations_issued_2016 <- read.df("s3://pk-nyc-parking-tickets/dataset/Parking_Violations_Issued_-_Fiscal_Year_2016.csv",
                                          source = "csv", inferSchema = "true", header = "true")

# 2017 dataframe
parking_violations_issued_2017 <- read.df("s3://pk-nyc-parking-tickets/dataset/Parking_Violations_Issued_-_Fiscal_Year_2017.csv",
                                          source = "csv", inferSchema = "true", header = "true")

#### Understanding the basics of data 

### 2015 dataframe

## checking head 
head(parking_violations_issued_2015)

## checking number of rows
nrow(parking_violations_issued_2015)
# 11809233 rows observed

## checking number of columns
ncol(parking_violations_issued_2015)
# 51 columns observed

## checking the structure
str(parking_violations_issued_2015)

## Removing duplicated rows if any
parking_violations_issued_2015 <- dropDuplicates(parking_violations_issued_2015)

## Checking if the issue date is within the fiscal period or not
parking_violations_issued_2015$`Issue Month` <- cast(substr(parking_violations_issued_2015$`Issue Date`, 2,3),"integer")
parking_violations_issued_2015$`Issue Year` <- cast(substr(parking_violations_issued_2015$`Issue Date`, 8,11),"integer")

# Grouping each and verify the data
frequency_of_issue_year_month_2015 <- summarize(groupBy(parking_violations_issued_2015, parking_violations_issued_2015$`Issue Year`, parking_violations_issued_2015$`Issue Month`),
                                                count = n(parking_violations_issued_2015$`Issue Month`))

collect(arrange(frequency_of_issue_year_month_2015, desc(frequency_of_issue_year_month_2015$count)))

# The issue dates are present outside the fiscal year also which needs to be removed
parking_violations_issued_2015 <- filter(parking_violations_issued_2015, (parking_violations_issued_2015$`Issue Year` == 2014 & 
                                                                            parking_violations_issued_2015$`Issue Month`>= 07 &
                                                                            parking_violations_issued_2015$`Issue Month`<= 12) |
                                           (parking_violations_issued_2015$`Issue Year` == 2015 & 
                                              parking_violations_issued_2015$`Issue Month`>= 01 &
                                              parking_violations_issued_2015$`Issue Month`<= 06))

# rechecking number of rows
nrow(parking_violations_issued_2015)
# 10598036 rows observed after removing duplicate rows and filtering 

## 2016 dataframe

# checking head 
head(parking_violations_issued_2016)

# checking number of rows
nrow(parking_violations_issued_2016)
# 10626899 rows observed

# checking number of columns
ncol(parking_violations_issued_2016)
# 51 columns observed

# checking the structure
str(parking_violations_issued_2016)

## Removing duplicated rows if any
parking_violations_issued_2016 <- dropDuplicates(parking_violations_issued_2016)

## Checking if the issue date is within the fiscal period or not
parking_violations_issued_2016$`Issue Month` <- cast(substr(parking_violations_issued_2016$`Issue Date`, 2,3),"integer")
parking_violations_issued_2016$`Issue Year` <- cast(substr(parking_violations_issued_2016$`Issue Date`, 8,11),"integer")

# Grouping each and verify the data
frequency_of_issue_year_month_2016 <- summarize(groupBy(parking_violations_issued_2016, parking_violations_issued_2016$`Issue Year`, parking_violations_issued_2016$`Issue Month`),
                                                count = n(parking_violations_issued_2016$`Issue Month`))

collect(arrange(frequency_of_issue_year_month_2016, desc(frequency_of_issue_year_month_2016$count)))

# The issue dates are present outside the fiscal year also which needs to be removed
parking_violations_issued_2016 <- filter(parking_violations_issued_2016, (parking_violations_issued_2016$`Issue Year` == 2015 & 
                                                                            parking_violations_issued_2016$`Issue Month`>= 07 &
                                                                            parking_violations_issued_2016$`Issue Month`<= 12) |
                                           (parking_violations_issued_2016$`Issue Year` == 2016 & 
                                              parking_violations_issued_2016$`Issue Month`>= 01 &
                                              parking_violations_issued_2016$`Issue Month`<= 06))

# rechecking number of rows
nrow(parking_violations_issued_2016)
# 10396984 rows observed after removing duplicate rows and filtering

## 2017 dataframe

# checking head 
head(parking_violations_issued_2017)

# checking number of rows
nrow(parking_violations_issued_2017)
# 10803028 rows observed

# checking number of columns
ncol(parking_violations_issued_2017)
# 43 columns observed

# checking the structure
str(parking_violations_issued_2017)

## Removing duplicated rows if any
parking_violations_issued_2017 <- dropDuplicates(parking_violations_issued_2017)

## Checking if the issue date is within the fiscal period or not
parking_violations_issued_2017$`Issue Month` <- cast(substr(parking_violations_issued_2017$`Issue Date`, 2,3),"integer")
parking_violations_issued_2017$`Issue Year` <- cast(substr(parking_violations_issued_2017$`Issue Date`, 8,11),"integer")

# Grouping each and verify the data
frequency_of_issue_year_month_2017 <- summarize(groupBy(parking_violations_issued_2017, parking_violations_issued_2017$`Issue Year`, parking_violations_issued_2017$`Issue Month`),
                                                count = n(parking_violations_issued_2017$`Issue Month`))

collect(arrange(frequency_of_issue_year_month_2017, desc(frequency_of_issue_year_month_2017$count)))

# The issue dates are present outside the fiscal year also which needs to be removed
parking_violations_issued_2017 <- filter(parking_violations_issued_2017, (parking_violations_issued_2017$`Issue Year` == 2016 & 
                                                                            parking_violations_issued_2017$`Issue Month`>= 07 &
                                                                            parking_violations_issued_2017$`Issue Month`<= 12) |
                                           (parking_violations_issued_2017$`Issue Year` == 2017 & 
                                              parking_violations_issued_2017$`Issue Month`>= 01 &
                                              parking_violations_issued_2017$`Issue Month`<= 06))

# rechecking number of rows
nrow(parking_violations_issued_2017)
# 10539563 rows observed after removing duplicate rows

#############################Examine the data#######################################
# load ggplot2 for ploting the data
library(ggplot2)

### Q1. Find total number of tickets for each year ###

# 2015 Year
total_tickets_2015 <- collect(select(parking_violations_issued_2015,countDistinct(parking_violations_issued_2015$`Summons Number`)))
total_tickets_2015
# 10598035 tickets observed

# 2016 Year
total_tickets_2016 <- collect(select(parking_violations_issued_2016,countDistinct(parking_violations_issued_2016$`Summons Number`)))
total_tickets_2016
# 10396984 tickets observed

# 2017 Year
total_tickets_2017 <- collect(select(parking_violations_issued_2017,countDistinct(parking_violations_issued_2017$`Summons Number`)))
total_tickets_2017
# 10539563 tickets observed

total_tickets <- data.frame(year = c("2015", "2016", "2017"), 
                            total = c(total_tickets_2015$`count(DISTINCT Summons Number)`, total_tickets_2016$`count(DISTINCT Summons Number)`, total_tickets_2017$`count(DISTINCT Summons Number)`))
ggplot(total_tickets, aes(x=year, y = total, fill = year)) + geom_bar(stat="identity")

############################################################################################################

### Q2. Find out how many unique states the cars which got parking tickets came from ###

# 2015 Year
unique_states_2015 <- collect(select(parking_violations_issued_2015,countDistinct(parking_violations_issued_2015$`Registration State`)))
unique_states_2015
# 69 unique states observed

# 2016 Year
unique_states_2016 <- collect(select(parking_violations_issued_2016,countDistinct(parking_violations_issued_2016$`Registration State`)))
unique_states_2016
# 68 unique states observed

# 2017 Year
unique_states_2017 <- collect(select(parking_violations_issued_2017,countDistinct(parking_violations_issued_2017$`Registration State`)))
unique_states_2017
# 67 unique states observed

unique_states <- data.frame(year = c("2015", "2016", "2017"), 
                            count = c(unique_states_2015$`count(DISTINCT Registration State)`,unique_states_2016$`count(DISTINCT Registration State)`,unique_states_2017$`count(DISTINCT Registration State)`) )
ggplot(unique_states, aes(x=year, y = count, fill = year)) + geom_bar(stat="identity")

### Q3. Some parking tickets don’t have addresses on them, which is cause for concern. Find out how many such tickets there are ###

tickets_without_address <- data.frame(year = c("2015", "2016", "2017"), count = 0)

# 2015 Year
tickets_without_address[tickets_without_address$year=="2015",]$count <- nrow(filter(parking_violations_issued_2015, isNull(parking_violations_issued_2015$`Street Name`) | isNull(parking_violations_issued_2015$`House Number`)))
tickets_without_address[tickets_without_address$year=="2015",]$count
# 1622076 tickets don't have addresses

# 2016 Year
tickets_without_address[tickets_without_address$year=="2016",]$count <- nrow(filter(parking_violations_issued_2016, isNull(parking_violations_issued_2016$`Street Name`) | isNull(parking_violations_issued_2016$`House Number`)))
tickets_without_address[tickets_without_address$year=="2016",]$count
# 1963921 tickets don't have addresses

# 2017 Year
tickets_without_address[tickets_without_address$year=="2017",]$count <- nrow(filter(parking_violations_issued_2017, isNull(parking_violations_issued_2017$`Street Name`) | isNull(parking_violations_issued_2017$`House Number`)))
tickets_without_address[tickets_without_address$year=="2017",]$count
# 2160639 tickets don't have addresses

ggplot(tickets_without_address, aes(x=year, y = count, fill = year)) + geom_bar(stat="identity")


#############################Aggregation tasks#######################################

### Q1. How often does each violation code occur? (frequency of violation codes - find the top 5) ###

# 2015 Year
frequency_of_violation_2015 <- summarize(groupBy(parking_violations_issued_2015, parking_violations_issued_2015$`Violation Code`),
                                         count = n(parking_violations_issued_2015$`Violation Code`))

df_violation_freq_2015 <- head(arrange(frequency_of_violation_2015, desc(frequency_of_violation_2015$count)),5)
df_violation_freq_2015
# Violation Code   count
#            21   1469228
#            38   1305007
#            14    908418
#            36    747098
#            37    735600

# 2016 Year
frequency_of_violation_2016 <- summarize(groupBy(parking_violations_issued_2016, parking_violations_issued_2016$`Violation Code`),
                                         count = n(parking_violations_issued_2016$`Violation Code`))

df_violation_freq_2016 <- head(arrange(frequency_of_violation_2016, desc(frequency_of_violation_2016$count)),5)
df_violation_freq_2016
# Violation Code   count
#            21   1497269
#            36   1232952
#            38   1126835
#            14    860045
#            37    677805

# 2017 Year
frequency_of_violation_2017 <- summarize(groupBy(parking_violations_issued_2017, parking_violations_issued_2017$`Violation Code`),
                                         count = n(parking_violations_issued_2017$`Violation Code`))

df_violation_freq_2017 <- head(arrange(frequency_of_violation_2017, desc(frequency_of_violation_2017$count)),5)
df_violation_freq_2017
# Violation Code   count
#            21   1500396
#            36   1345237
#            38   1050418
#            14    880152
#            20    609231

# Plot the output of frequency of violation code each year
df_violation_freq_2015$year = "2015"
df_violation_freq_2016$year = "2016"
df_violation_freq_2017$year = "2017"

df_violation_freq <- rbind.data.frame(df_violation_freq_2015,df_violation_freq_2016,df_violation_freq_2017)
ggplot(df_violation_freq, aes(x=factor(`Violation Code`), y = count, fill = factor(`Violation Code`))) + geom_bar(stat="identity") + facet_wrap(~year)

############################################################################################################

### Q2. How often does each vehicle body type get a parking ticket? How about the vehicle make? (find the top 5 for both) ###
## Q2.1 How often does each vehicle body type get a parking ticket?

#2015 Year
frequency_of_vehicle_body_type_2015 <- summarize(groupBy(parking_violations_issued_2015, parking_violations_issued_2015$`Vehicle Body Type`),
                                                 count = n(parking_violations_issued_2015$`Vehicle Body Type`))

df_vehicle_body_freq_2015 <- head(arrange(frequency_of_vehicle_body_type_2015, desc(frequency_of_vehicle_body_type_2015$count)),5)
df_vehicle_body_freq_2015
# Vehicle Body Type   count
#             SUBN  3341110
#             4DSD  3001810
#              VAN  1570227
#             DELV   822041
#              SDN   428571

# 2016 Year
frequency_of_vehicle_body_type_2016 <- summarize(groupBy(parking_violations_issued_2016, parking_violations_issued_2016$`Vehicle Body Type`),
                                                 count = n(parking_violations_issued_2016$`Vehicle Body Type`))

df_vehicle_body_freq_2016 <- head(arrange(frequency_of_vehicle_body_type_2016, desc(frequency_of_vehicle_body_type_2016$count)),5)
df_vehicle_body_freq_2016
# Vehicle Body Type   count
#             SUBN  3393838
#             4DSD  2936729
#              VAN  1489924
#             DELV   738747
#              SDN   401750

# 2017 Year
frequency_of_vehicle_body_type_2017 <- summarize(groupBy(parking_violations_issued_2017, parking_violations_issued_2017$`Vehicle Body Type`),
                                                 count = n(parking_violations_issued_2017$`Vehicle Body Type`))

df_vehicle_body_freq_2017 <- head(arrange(frequency_of_vehicle_body_type_2017, desc(frequency_of_vehicle_body_type_2017$count)),5)
df_vehicle_body_freq_2017
# Vehicle Body Type   count
#             SUBN  3632003
#             4DSD  3017372
#              VAN  1384121
#             DELV   672123
#              SDN   414984


# Plot the output of vehicle body frequency by year
df_vehicle_body_freq_2015$year = "2015"
df_vehicle_body_freq_2016$year = "2016"
df_vehicle_body_freq_2017$year = "2017"

df_vehicle_body_freq <- rbind.data.frame(df_vehicle_body_freq_2015,df_vehicle_body_freq_2016,df_vehicle_body_freq_2017)
ggplot(df_vehicle_body_freq, aes(x=`Vehicle Body Type`, y = count, fill = `Vehicle Body Type`)) + geom_bar(stat="identity") + facet_wrap(~year)


##  Q2.2 How about the vehicle make?

#2015 Year
frequency_of_vehicle_make_2015 <- summarize(groupBy(parking_violations_issued_2015, parking_violations_issued_2015$`Vehicle Make`),
                                            count = n(parking_violations_issued_2015$`Vehicle Make`))

df_vehicle_make_freq_2015 <- head(arrange(frequency_of_vehicle_make_2015, desc(frequency_of_vehicle_make_2015$count)),5)
df_vehicle_make_freq_2015
# Vehicle Make   count
#        FORD   1373157
#       TOYOT   1082206
#       HONDA    982130
#       CHEVR    811659
#       NISSA    805572

# 2016 Year
frequency_of_vehicle_make_2016 <- summarize(groupBy(parking_violations_issued_2016, parking_violations_issued_2016$`Vehicle Make`),
                                            count = n(parking_violations_issued_2016$`Vehicle Make`))

df_vehicle_make_freq_2016 <- head(arrange(frequency_of_vehicle_make_2016, desc(frequency_of_vehicle_make_2016$count)),5)
df_vehicle_make_freq_2016
# Vehicle Make   count
#        FORD   1297363
#       TOYOT   1128909
#       HONDA    991735
#       NISSA    815963
#       CHEVR    743416

# 2017 Year
frequency_of_vehicle_make_2017 <- summarize(groupBy(parking_violations_issued_2017, parking_violations_issued_2017$`Vehicle Make`),
                                            count = n(parking_violations_issued_2017$`Vehicle Make`))

df_vehicle_make_freq_2017 <- head(arrange(frequency_of_vehicle_make_2017, desc(frequency_of_vehicle_make_2017$count)),5)
df_vehicle_make_freq_2017
# Vehicle Make   count
#        FORD   1250777
#       TOYOT   1179265
#       HONDA   1052006
#       NISSA    895225
#       CHEVR    698024

# Plot the output of vehicle make frequency by year
df_vehicle_make_freq_2015$year = "2015"
df_vehicle_make_freq_2016$year = "2016"
df_vehicle_make_freq_2017$year = "2017"

df_vehicle_make_freq <- rbind.data.frame(df_vehicle_make_freq_2015,df_vehicle_make_freq_2016,df_vehicle_make_freq_2017)
ggplot(df_vehicle_make_freq, aes(x=`Vehicle Make`, y = count, fill = `Vehicle Make`)) + geom_bar(stat="identity") + facet_wrap(~year)

############################################################################################################

### Q3. A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequencies of: ###
## Q3.1 Violating Precincts (this is the precinct of the zone where the violation occurred)

# 2015 Year
frequency_of_violating_precincts_2015 <- summarize(groupBy(parking_violations_issued_2015, parking_violations_issued_2015$`Violation Precinct`),
                                                   count = n(parking_violations_issued_2015$`Violation Precinct`))

df_violating_precints_freq_2015 <- head(arrange(frequency_of_violating_precincts_2015, desc(frequency_of_violating_precincts_2015$count)),5)
df_violating_precints_freq_2015
# Violation Precinct   count
#                 0   1455166
#                19    550797
#                18    393802
#                14    377750
#                 1    302737

# 2016 Year
frequency_of_violating_precincts_2016 <- summarize(groupBy(parking_violations_issued_2016, parking_violations_issued_2016$`Violation Precinct`),
                                                   count = n(parking_violations_issued_2016$`Violation Precinct`))

df_violating_precints_freq_2016 <- head(arrange(frequency_of_violating_precincts_2016, desc(frequency_of_violating_precincts_2016$count)),5)
df_violating_precints_freq_2016
# Violation Precinct   count
#                 0   1807139
#                19    545669
#                18    325559
#                14    318193
#                 1    299074

# 2017 Year
frequency_of_violating_precincts_2017 <- summarize(groupBy(parking_violations_issued_2017, parking_violations_issued_2017$`Violation Precinct`),
                                                   count = n(parking_violations_issued_2017$`Violation Precinct`))

df_violating_precints_freq_2017 <- head(arrange(frequency_of_violating_precincts_2017, desc(frequency_of_violating_precincts_2017$count)),5)
df_violating_precints_freq_2017
# Violation Precinct   count
#                 0   1950083
#                19    528317
#                14    347736
#                 1    326961
#                18    302008

df_violating_precints_freq_2015$year = "2015"
df_violating_precints_freq_2016$year = "2016"
df_violating_precints_freq_2017$year = "2017"

df_violating_precints_freq <- rbind.data.frame(df_violating_precints_freq_2015,df_violating_precints_freq_2016,df_violating_precints_freq_2017)
ggplot(df_violating_precints_freq, aes(x=factor(`Violation Precinct`), y = count, fill = factor(`Violation Precinct`))) + geom_bar(stat="identity") + facet_wrap(~year)


## Q3.2 Issuing Precincts (this is the precinct that issued the ticket)

# 2015 Year
frequency_of_issuing_precincts_2015 <- summarize(groupBy(parking_violations_issued_2015, parking_violations_issued_2015$`Issuer Precinct`),
                                                 count = n(parking_violations_issued_2015$`Issuer Precinct`))

df_issuing_precincts_freq_2015 <- head(arrange(frequency_of_issuing_precincts_2015, desc(frequency_of_issuing_precincts_2015$count)),5)
df_issuing_precincts_freq_2015
# Issuer Precinct   count
#              0  1648671
#             19   536627
#             18   384863
#             14   363734
#              1   293942

# 2016 Year
frequency_of_issuing_precincts_2016 <- summarize(groupBy(parking_violations_issued_2016, parking_violations_issued_2016$`Issuer Precinct`),
                                                 count = n(parking_violations_issued_2016$`Issuer Precinct`))

df_issuing_precincts_freq_2016 <- head(arrange(frequency_of_issuing_precincts_2016, desc(frequency_of_issuing_precincts_2016$count)),5)
df_issuing_precincts_freq_2016
# Issuer Precinct   count
#              0  2067219
#             19   532298
#             18   317451
#             14   309727
#              1   290472

# 2017 Year
frequency_of_issuing_precincts_2017 <- summarize(groupBy(parking_violations_issued_2017, parking_violations_issued_2017$`Issuer Precinct`),
                                                 count = n(parking_violations_issued_2017$`Issuer Precinct`))

df_issuing_precincts_freq_2017 <- head(arrange(frequency_of_issuing_precincts_2017, desc(frequency_of_issuing_precincts_2017$count)),5)
df_issuing_precincts_freq_2017
# Issuer Precinct   count
#              0  2255086
#             19   514786
#             14   340862
#              1   316776
#             18   292237

# Plot the output of frequency of issuing precincts
df_issuing_precincts_freq_2015$year = "2015"
df_issuing_precincts_freq_2016$year = "2016"
df_issuing_precincts_freq_2017$year = "2017"

df_issuing_precincts_freq <- rbind.data.frame(df_issuing_precincts_freq_2015,df_issuing_precincts_freq_2016,df_issuing_precincts_freq_2017)
ggplot(df_issuing_precincts_freq, aes(x=factor(`Issuer Precinct`), y = count, fill = factor(`Issuer Precinct`))) + geom_bar(stat="identity") + facet_wrap(~year)

############################################################################################################

### Q4. Find the violation code frequency across 3 precincts which have issued the most number of tickets. do these precinct zones have an exceptionally high frequency of certain violation codes? ### 
### Are these codes common across precincts? ###

## 2015 Year
# Q4.1 Find the violation code frequency across 3 precincts which have issued the most number of tickets
frequency_of_violation_code_across_precincts_2015 <- summarize(groupBy(filter(parking_violations_issued_2015, (parking_violations_issued_2015$`Issuer Precinct` == 0 |  parking_violations_issued_2015$`Issuer Precinct` == 19 | parking_violations_issued_2015$`Issuer Precinct` == 18)), parking_violations_issued_2015$`Violation Code`),
                                                               count = n(parking_violations_issued_2015$`Violation Code`))

head(arrange(frequency_of_violation_code_across_precincts_2015, desc(frequency_of_violation_code_across_precincts_2015$count)),5)

# Violation Code  count
#             36 747098
#              7 567953
#             21 232101
#             14 183000
#              5 127154

# Q4.2 Are these codes common across precincts? 
frequency_of_violation_code_each_precincts_2015 <- summarize(groupBy(filter(parking_violations_issued_2015, (parking_violations_issued_2015$`Issuer Precinct` == 0 |  parking_violations_issued_2015$`Issuer Precinct` == 19 | parking_violations_issued_2015$`Issuer Precinct` == 18)), parking_violations_issued_2015$`Issuer Precinct`, parking_violations_issued_2015$`Violation Code`),
                                                             count = n(parking_violations_issued_2015$`Violation Code`))

createOrReplaceTempView(frequency_of_violation_code_each_precincts_2015, "frequency_of_violation_code_each_precincts_2015_tbl")
frequency_of_violation_code_issue_precincts_2015 <- SparkR::sql("SELECT `Issuer Precinct`,`Violation Code`,count FROM 
                                                                (SELECT `Issuer Precinct`,`Violation Code`,count,dense_rank() 
                                                                OVER (PARTITION BY `Issuer Precinct` ORDER BY count DESC) as rank 
                                                                FROM frequency_of_violation_code_each_precincts_2015_tbl) tmp 
                                                                WHERE rank <= 5")

df_violation_code_precincts_freq_2015 <- collect(frequency_of_violation_code_issue_precincts_2015)
df_violation_code_precincts_freq_2015
# Issuer Precinct Violation Code  count
#              19             38  89102
#              19             37  78716
#              19             14  59915
#              19             16  55762
#              19             21  55296
#               0             36 747098
#               0              7 567951
#               0             21 173191
#               0              5 127153
#               0             66   4703
#              18             14 119078
#              18             69  56436
#              18             31  30030
#              18             47  28724
#              18             42  19522

## 2016 Year
# Q4.1 Find the violation code frequency across 3 precincts which have issued the most number of tickets
frequency_of_violation_code_across_precincts_2016 <- summarize(groupBy(filter(parking_violations_issued_2016, (parking_violations_issued_2016$`Issuer Precinct` == 0 |  parking_violations_issued_2016$`Issuer Precinct` == 19 | parking_violations_issued_2016$`Issuer Precinct` == 18)), parking_violations_issued_2016$`Violation Code`),
                                                               count = n(parking_violations_issued_2016$`Violation Code`))

head(arrange(frequency_of_violation_code_across_precincts_2016, desc(frequency_of_violation_code_across_precincts_2016$count)),5)
# Violation Code   count
#             36 1232951
#              7  457871
#             21  287729
#             14  164816
#              5  106617

# Q4.2 Are these codes common across precincts? 
frequency_of_violation_code_each_precincts_2016 <- summarize(groupBy(filter(parking_violations_issued_2016, (parking_violations_issued_2016$`Issuer Precinct` == 0 |  parking_violations_issued_2016$`Issuer Precinct` == 19 | parking_violations_issued_2016$`Issuer Precinct` == 18)), parking_violations_issued_2016$`Issuer Precinct`, parking_violations_issued_2016$`Violation Code`),
                                                             count = n(parking_violations_issued_2016$`Violation Code`))

createOrReplaceTempView(frequency_of_violation_code_each_precincts_2016, "frequency_of_violation_code_each_precincts_2016_tbl")
frequency_of_violation_code_issue_precincts_2016 <- SparkR::sql("SELECT `Issuer Precinct`,`Violation Code`,count FROM 
                                                                (SELECT `Issuer Precinct`,`Violation Code`,count,dense_rank() 
                                                                OVER (PARTITION BY `Issuer Precinct` ORDER BY count DESC) as rank 
                                                                FROM frequency_of_violation_code_each_precincts_2016_tbl) tmp 
                                                                WHERE rank <= 5")

df_violation_code_precincts_freq_2016 <- collect(frequency_of_violation_code_issue_precincts_2016)
df_violation_code_precincts_freq_2016
# Issuer Precinct Violation Code   count
#              19             38   76178
#              19             37   74758
#              19             46   71509
#              19             14   60856
#              19             21   57601
#               0             36 1232951
#               0              7  457871
#               0             21  226687
#               0              5  106617
#               0             66    7275
#              18             14   98160
#              18             69   47129
#              18             47   23618
#              18             31   22413
#              18             42   17416

# 2017 Year
# Q4.1 Find the violation code frequency across 3 precincts which have issued the most number of tickets
frequency_of_violation_code_across_precincts_2017 <- summarize(groupBy(filter(parking_violations_issued_2017, (parking_violations_issued_2017$`Issuer Precinct` == 0 |  parking_violations_issued_2017$`Issuer Precinct` == 19 | parking_violations_issued_2017$`Issuer Precinct` == 14)), parking_violations_issued_2017$`Violation Code`),
                                                               count = n(parking_violations_issued_2017$`Violation Code`))

head(arrange(frequency_of_violation_code_across_precincts_2017, desc(frequency_of_violation_code_across_precincts_2017$count)),5)
# Violation Code   count
#             36 1345237
#              7  464691
#             21  314929
#             14  136728
#              5  130964

# Q4.2 Are these codes common across precincts? 
frequency_of_violation_code_each_precincts_2017 <- summarize(groupBy(filter(parking_violations_issued_2017, (parking_violations_issued_2017$`Issuer Precinct` == 0 |  parking_violations_issued_2017$`Issuer Precinct` == 19 | parking_violations_issued_2017$`Issuer Precinct` == 14)), parking_violations_issued_2017$`Issuer Precinct`, parking_violations_issued_2017$`Violation Code`),
                                                             count = n(parking_violations_issued_2017$`Violation Code`))

createOrReplaceTempView(frequency_of_violation_code_each_precincts_2017, "frequency_of_violation_code_each_precincts_2017_tbl")
frequency_of_violation_code_issue_precincts_2017 <- SparkR::sql("SELECT `Issuer Precinct`,`Violation Code`,count FROM 
                                                                (SELECT `Issuer Precinct`,`Violation Code`,count,dense_rank() 
                                                                OVER (PARTITION BY `Issuer Precinct` ORDER BY count DESC) as rank 
                                                                FROM frequency_of_violation_code_each_precincts_2017_tbl) tmp 
                                                                WHERE rank <= 5")

df_violation_code_precincts_freq_2017 <- collect(frequency_of_violation_code_issue_precincts_2017)
df_violation_code_precincts_freq_2017
# Issuer Precinct Violation Code   count
#              19             46   84789
#              19             38   71631
#              19             37   71592
#              19             14   56873
#              19             21   54033
#              14             14   73007
#              14             69   57316
#              14             31   39430
#              14             47   30200
#              14             42   20402
#               0             36 1345237
#               0              7  464690
#               0             21  258771
#               0              5  130963
#               0             66    9281

# Plot the output of violation code frequency across 3 precincts which have issued the most number of tickets
df_violation_code_precincts_freq_2015$year = "2015"
df_violation_code_precincts_freq_2016$year = "2016"
df_violation_code_precincts_freq_2017$year = "2017"

df_violation_code_precincts_freq <- rbind.data.frame(df_violation_code_precincts_freq_2015,df_violation_code_precincts_freq_2016,df_violation_code_precincts_freq_2017)
ggplot(df_violation_code_precincts_freq, aes(x=factor(`Issuer Precinct`), y = count, fill = factor(`Violation Code`))) + geom_bar(stat="identity") + facet_wrap(~year)

############################################################################################################

### Q5. You’d want to find out the properties of parking violations across different times of the day: ###
## The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.

## 2015 Year
parking_violations_issued_2015$`Violation Hour` <-  cast(substr(parking_violations_issued_2015$`Violation Time`, 2,3),"integer")
parking_violations_issued_2015$`Violation Minutes` <-  cast(substr(parking_violations_issued_2015$`Violation Time`, 4,5), "integer")
parking_violations_issued_2015$`Violation Convention` <-  substr(parking_violations_issued_2015$`Violation Time`, 6,6)

# Grouping each and verify the data
frequency_of_violation_hour_2015 <- summarize(groupBy(parking_violations_issued_2015, parking_violations_issued_2015$`Violation Hour`),
                                              count = n(parking_violations_issued_2015$`Violation Hour`))

collect(arrange(frequency_of_violation_hour_2015, desc(frequency_of_violation_hour_2015$count)))
# Hour more than 12 are observed which is a data quality issue as the time are provided in AM/PM format

frequency_of_violation_minutes_2015 <- summarize(groupBy(parking_violations_issued_2015, parking_violations_issued_2015$`Violation Minutes`),
                                                 count = n(parking_violations_issued_2015$`Violation Minutes`))

collect(arrange(frequency_of_violation_minutes_2015, desc(frequency_of_violation_minutes_2015$count)))
# Minutes seems okay

# filtering the dataset to have hours between 1 and 12 and minutes <= 60 and has A/M in Convention
parking_violations_issued_2015_filtered <- filter(parking_violations_issued_2015, parking_violations_issued_2015$`Violation Hour`>= 1 & parking_violations_issued_2015$`Violation Hour`<= 12 &
                                                    parking_violations_issued_2015$`Violation Minutes` <=59 & (parking_violations_issued_2015$`Violation Convention` == "A" |parking_violations_issued_2015$`Violation Convention` == "P"))

# checking number of rows after filtering
nrow(parking_violations_issued_2015_filtered)
# 10536469 rows observed

# Concating hour into 24 hours format
parking_violations_issued_2015_filtered$`Violation Hour` <- ifelse(parking_violations_issued_2015_filtered$`Violation Convention` == "A" & parking_violations_issued_2015_filtered$`Violation Hour` == 12, cast(parking_violations_issued_2015_filtered$`Violation Hour`-12,"integer"),
                                                                   ifelse(parking_violations_issued_2015_filtered$`Violation Convention` == "A" & parking_violations_issued_2015_filtered$`Violation Hour` < 12, parking_violations_issued_2015_filtered$`Violation Hour`,
                                                                          ifelse(parking_violations_issued_2015_filtered$`Violation Convention` == "P" & parking_violations_issued_2015_filtered$`Violation Hour` == 12, parking_violations_issued_2015_filtered$`Violation Hour`, 
                                                                                 ifelse(parking_violations_issued_2015_filtered$`Violation Convention` == "P" & parking_violations_issued_2015_filtered$`Violation Hour` < 12, cast(parking_violations_issued_2015_filtered$`Violation Hour`+12,"integer"),""))))

# Combining hour and minute together
parking_violations_issued_2015_filtered$`Time Violation` <- concat_ws(":", parking_violations_issued_2015_filtered$`Violation Hour`, parking_violations_issued_2015_filtered$`Violation Minutes`)

# Divide 24 hours into 6 equal discrete bins of time.
parking_violations_issued_2015_filtered$`Time Bins` <- ifelse(parking_violations_issued_2015_filtered$`Violation Hour` >= 5 & parking_violations_issued_2015_filtered$`Violation Hour` <= 7, "Early Morning",
                                                              ifelse(parking_violations_issued_2015_filtered$`Violation Hour` >= 8 & parking_violations_issued_2015_filtered$`Violation Hour` <= 11, "Morning",
                                                                     ifelse(parking_violations_issued_2015_filtered$`Violation Hour` >= 12 & parking_violations_issued_2015_filtered$`Violation Hour` <= 16, "Afternoon",
                                                                            ifelse(parking_violations_issued_2015_filtered$`Violation Hour` >= 17 & parking_violations_issued_2015_filtered$`Violation Hour` <= 20, "Evening",
                                                                                   ifelse(parking_violations_issued_2015_filtered$`Violation Hour` >= 21 & parking_violations_issued_2015_filtered$`Violation Hour` <= 23, "Night",
                                                                                          ifelse(parking_violations_issued_2015_filtered$`Violation Hour` >= 0 & parking_violations_issued_2015_filtered$`Violation Hour` <= 4, "Late Night",""))))))

# For each of these groups, find the 3 most commonly occurring violations
frequency_of_violation_binswise_2015 <- summarize(groupBy(parking_violations_issued_2015_filtered, parking_violations_issued_2015_filtered$`Time Bins`, parking_violations_issued_2015_filtered$`Violation Code`),
                                                  count = n(parking_violations_issued_2015_filtered$`Violation Code`))

createOrReplaceTempView(frequency_of_violation_binswise_2015, "frequency_of_violation_binswise_2015_tbl")
frequency_of_3_most_occuring_violations_2015 <- SparkR::sql("SELECT `Time Bins`,`Violation Code`,count FROM 
                                                            (SELECT `Time Bins`,`Violation Code`,count,dense_rank() 
                                                            OVER (PARTITION BY `Time Bins` ORDER BY count DESC) as rank 
                                                            FROM frequency_of_violation_binswise_2015_tbl) tmp 
                                                            WHERE rank <= 3")

violation_fre_bin_2015<-collect(frequency_of_3_most_occuring_violations_2015)
violation_fre_bin_2015
#        Time Bins Violation Code   count
# 1        Evening             38  155790
# 2        Evening              7  122958
# 3        Evening             37   98028
# 4        Morning             21 1167094
# 5        Morning             38  442655
# 6        Morning             36  353555
# 7  Early Morning             14  130419
# 8  Early Morning             21  102689
# 9  Early Morning             40   84539
# 10     Afternoon             38  668865
# 11     Afternoon             37  496972
# 12     Afternoon             14  343298
# 13         Night              7   48129
# 14         Night             38   34338
# 15         Night             14   33672
# 16    Late Night             40   41587
# 17    Late Night             21   41140
# 18    Late Night             78   36298

# Now, try another direction. For the 3 most commonly occurring violation codes, find the most common times of day (in terms of the bins from the previous part)
# From Question no. 1 we know what are most common violation codes. 21,38,14
frequency_of_times_of_day_2015 <- summarize(groupBy(filter(parking_violations_issued_2015_filtered, parking_violations_issued_2015_filtered$`Violation Code` == 21 |parking_violations_issued_2015_filtered$`Violation Code` == 38| parking_violations_issued_2015_filtered$`Violation Code` == 14), parking_violations_issued_2015_filtered$`Violation Code`, parking_violations_issued_2015_filtered$`Time Bins`),
                                            count = n(parking_violations_issued_2015_filtered$`Time Bins`))

df_times_of_day_freq_2015 <- collect(arrange(frequency_of_times_of_day_2015, frequency_of_times_of_day_2015$`Violation Code`, desc(frequency_of_times_of_day_2015$count)))
df_times_of_day_freq_2015
#    Violation Code     Time Bins   count
# 1              14     Afternoon  343298
# 2              14       Morning  292903
# 3              14 Early Morning  130419
# 4              14       Evening   76747
# 5              14         Night   33672
# 6              14    Late Night   23738
# 7              21       Morning 1167094
# 8              21     Afternoon  131476
# 9              21 Early Morning  102689
# 10             21    Late Night   41140
# 11             21       Evening     573
# 12             21         Night     414
# 13             38     Afternoon  668865
# 14             38       Morning  442655
# 15             38       Evening  155790
# 16             38         Night   34338
# 17             38 Early Morning    2640
# 18             38    Late Night     703

## 2016 Year
parking_violations_issued_2016$`Violation Hour` <-  cast(substr(parking_violations_issued_2016$`Violation Time`, 2,3),"integer")
parking_violations_issued_2016$`Violation Minutes` <-  cast(substr(parking_violations_issued_2016$`Violation Time`, 4,5), "integer")
parking_violations_issued_2016$`Violation Convention` <-  substr(parking_violations_issued_2016$`Violation Time`, 6,6)

# Grouping each and verify the data
frequency_of_violation_hour_2016 <- summarize(groupBy(parking_violations_issued_2016, parking_violations_issued_2016$`Violation Hour`),
                                              count = n(parking_violations_issued_2016$`Violation Hour`))

collect(arrange(frequency_of_violation_hour_2016, desc(frequency_of_violation_hour_2016$count)))
# Hour more than 12 are observed which is a data quality issue as the time are provided in AM/PM format

frequency_of_violation_minutes_2016 <- summarize(groupBy(parking_violations_issued_2016, parking_violations_issued_2016$`Violation Minutes`),
                                                 count = n(parking_violations_issued_2016$`Violation Minutes`))

collect(arrange(frequency_of_violation_minutes_2016, desc(frequency_of_violation_minutes_2016$count)))
# Minutes seems okay

# filtering the dataset to have hours between 1 and 12 and minutes <= 60 and has A/M in Convention
parking_violations_issued_2016_filtered <- filter(parking_violations_issued_2016, parking_violations_issued_2016$`Violation Hour`>= 1 & parking_violations_issued_2016$`Violation Hour`<= 12 &
                                                    parking_violations_issued_2016$`Violation Minutes` <=59 & (parking_violations_issued_2016$`Violation Convention` == "A" |parking_violations_issued_2016$`Violation Convention` == "P"))

# checking number of rows after filtering
nrow(parking_violations_issued_2016_filtered)
# 10333354 rows observed

# Concating hour into 24 hours format
parking_violations_issued_2016_filtered$`Violation Hour` <- ifelse(parking_violations_issued_2016_filtered$`Violation Convention` == "A" & parking_violations_issued_2016_filtered$`Violation Hour` == 12, cast(parking_violations_issued_2016_filtered$`Violation Hour`-12,"integer"),
                                                                   ifelse(parking_violations_issued_2016_filtered$`Violation Convention` == "A" & parking_violations_issued_2016_filtered$`Violation Hour` < 12, parking_violations_issued_2016_filtered$`Violation Hour`,
                                                                          ifelse(parking_violations_issued_2016_filtered$`Violation Convention` == "P" & parking_violations_issued_2016_filtered$`Violation Hour` == 12, parking_violations_issued_2016_filtered$`Violation Hour`, 
                                                                                 ifelse(parking_violations_issued_2016_filtered$`Violation Convention` == "P" & parking_violations_issued_2016_filtered$`Violation Hour` < 12, cast(parking_violations_issued_2016_filtered$`Violation Hour`+12,"integer"),""))))
# Combining hour and minute together
parking_violations_issued_2016_filtered$`Time Violation` <- concat_ws(":", parking_violations_issued_2016_filtered$`Violation Hour`, parking_violations_issued_2016_filtered$`Violation Minutes`)

# Divide 24 hours into 6 equal discrete bins of time.
parking_violations_issued_2016_filtered$`Time Bins` <- ifelse(parking_violations_issued_2016_filtered$`Violation Hour` >= 5 & parking_violations_issued_2016_filtered$`Violation Hour` <= 7, "Early Morning",
                                                              ifelse(parking_violations_issued_2016_filtered$`Violation Hour` >= 8 & parking_violations_issued_2016_filtered$`Violation Hour` <= 11, "Morning",
                                                                     ifelse(parking_violations_issued_2016_filtered$`Violation Hour` >= 12 & parking_violations_issued_2016_filtered$`Violation Hour` <= 16, "Afternoon",
                                                                            ifelse(parking_violations_issued_2016_filtered$`Violation Hour` >= 17 & parking_violations_issued_2016_filtered$`Violation Hour` <= 20, "Evening",
                                                                                   ifelse(parking_violations_issued_2016_filtered$`Violation Hour` >= 21 & parking_violations_issued_2016_filtered$`Violation Hour` <= 23, "Night",
                                                                                          ifelse(parking_violations_issued_2016_filtered$`Violation Hour` >= 0 & parking_violations_issued_2016_filtered$`Violation Hour` <= 4, "Late Night",""))))))

# For each of these groups, find the 3 most commonly occurring violations
frequency_of_violation_binswise_2016 <- summarize(groupBy(parking_violations_issued_2016_filtered, parking_violations_issued_2016_filtered$`Time Bins`, parking_violations_issued_2016_filtered$`Violation Code`),
                                                  count = n(parking_violations_issued_2016_filtered$`Violation Code`))

createOrReplaceTempView(frequency_of_violation_binswise_2016, "frequency_of_violation_binswise_2016_tbl")
frequency_of_3_most_occuring_violations_2016 <- SparkR::sql("SELECT `Time Bins`,`Violation Code`,count FROM 
                                                            (SELECT `Time Bins`,`Violation Code`,count,dense_rank() 
                                                            OVER (PARTITION BY `Time Bins` ORDER BY count DESC) as rank 
                                                            FROM frequency_of_violation_binswise_2016_tbl) tmp 
                                                            WHERE rank <= 3")

violation_fre_bin_2016<-collect(frequency_of_3_most_occuring_violations_2016)

violation_fre_bin_2016
#        Time Bins Violation Code   count
# 1        Evening             38  135471
# 2        Evening              7   98834
# 3        Evening             37   88334
# 4        Morning             21 1183374
# 5        Morning             36  578035
# 6        Morning             38  382100
# 7  Early Morning             14  136222
# 8  Early Morning             21  109753
# 9  Early Morning             40   85250
# 10     Afternoon             38  575504
# 11     Afternoon             36  564438
# 12     Afternoon             37  457571
# 13         Night              7   38990
# 14         Night             40   33679
# 15         Night             14   32289
# 16    Late Night             21   44878
# 17    Late Night             40   39806
# 18    Late Night             78   29639

# Now, try another direction. For the 3 most commonly occurring violation codes, find the most common times of day (in terms of the bins from the previous part)
# From Question no. 1 we know what are most common violation codes. 21,38,14
frequency_of_times_of_day_2016 <- summarize(groupBy(filter(parking_violations_issued_2016_filtered, parking_violations_issued_2016_filtered$`Violation Code` == 21 |parking_violations_issued_2016_filtered$`Violation Code` == 36| parking_violations_issued_2016_filtered$`Violation Code` == 38), parking_violations_issued_2016_filtered$`Violation Code`, parking_violations_issued_2016_filtered$`Time Bins`),
                                            count = n(parking_violations_issued_2016_filtered$`Time Bins`))

df_times_of_day_freq_2016 <- collect(arrange(frequency_of_times_of_day_2016, frequency_of_times_of_day_2016$`Violation Code`, desc(frequency_of_times_of_day_2016$count)))
df_times_of_day_freq_2016
#    Violation Code     Time Bins   count
# 1              21       Morning 1183374
# 2              21     Afternoon  131754
# 3              21 Early Morning  109753
# 4              21    Late Night   44878
# 5              21       Evening     414
# 6              21         Night     332
# 7              36       Morning  578035
# 8              36     Afternoon  564438
# 9              36 Early Morning   77656
# 10             36       Evening   12820
# 11             36    Late Night       2
# 12             38     Afternoon  575504
# 13             38       Morning  382100
# 14             38       Evening  135471
# 15             38         Night   31207
# 16             38 Early Morning    2054
# 17             38    Late Night     493

## 2017 Year
parking_violations_issued_2017$`Violation Hour` <-  cast(substr(parking_violations_issued_2017$`Violation Time`, 2,3),"integer")
parking_violations_issued_2017$`Violation Minutes` <-  cast(substr(parking_violations_issued_2017$`Violation Time`, 4,5), "integer")
parking_violations_issued_2017$`Violation Convention` <-  substr(parking_violations_issued_2017$`Violation Time`, 6,6)

# Grouping each and verify the data
frequency_of_violation_hour_2017 <- summarize(groupBy(parking_violations_issued_2017, parking_violations_issued_2017$`Violation Hour`),
                                              count = n(parking_violations_issued_2017$`Violation Hour`))

collect(arrange(frequency_of_violation_hour_2017, desc(frequency_of_violation_hour_2017$count)))
# Hour more than 12 are observed which is a data quality issue as the time are provided in AM/PM format

frequency_of_violation_minutes_2017 <- summarize(groupBy(parking_violations_issued_2017, parking_violations_issued_2017$`Violation Minutes`),
                                                 count = n(parking_violations_issued_2017$`Violation Minutes`))

collect(arrange(frequency_of_violation_minutes_2017, desc(frequency_of_violation_minutes_2017$count)))
# Minutes seems okay

# filtering the dataset to have hours between 1 and 12 and minutes <= 60 and has A/M in Convention
parking_violations_issued_2017_filtered <- filter(parking_violations_issued_2017, parking_violations_issued_2017$`Violation Hour`>= 1 & parking_violations_issued_2017$`Violation Hour`<= 12 &
                                                    parking_violations_issued_2017$`Violation Minutes` <=59 & (parking_violations_issued_2017$`Violation Convention` == "A" |parking_violations_issued_2017$`Violation Convention` == "P"))

# checking number of rows after filtering
nrow(parking_violations_issued_2017_filtered)
# 10481583 rows observed

# Concating hour into 24 hours format
parking_violations_issued_2017_filtered$`Violation Hour` <- ifelse(parking_violations_issued_2017_filtered$`Violation Convention` == "A" & parking_violations_issued_2017_filtered$`Violation Hour` == 12, cast(parking_violations_issued_2017_filtered$`Violation Hour`-12,"integer"),
                                                                   ifelse(parking_violations_issued_2017_filtered$`Violation Convention` == "A" & parking_violations_issued_2017_filtered$`Violation Hour` < 12, parking_violations_issued_2017_filtered$`Violation Hour`,
                                                                          ifelse(parking_violations_issued_2017_filtered$`Violation Convention` == "P" & parking_violations_issued_2017_filtered$`Violation Hour` == 12, parking_violations_issued_2017_filtered$`Violation Hour`, 
                                                                                 ifelse(parking_violations_issued_2017_filtered$`Violation Convention` == "P" & parking_violations_issued_2017_filtered$`Violation Hour` < 12, cast(parking_violations_issued_2017_filtered$`Violation Hour`+12,"integer"),""))))
# Combining hour and minute together
parking_violations_issued_2017_filtered$`Time Violation` <- concat_ws(":", parking_violations_issued_2017_filtered$`Violation Hour`, parking_violations_issued_2017_filtered$`Violation Minutes`)

# Divide 24 hours into 6 equal discrete bins of time.
parking_violations_issued_2017_filtered$`Time Bins` <- ifelse(parking_violations_issued_2017_filtered$`Violation Hour` >= 5 & parking_violations_issued_2017_filtered$`Violation Hour` <= 7, "Early Morning",
                                                              ifelse(parking_violations_issued_2017_filtered$`Violation Hour` >= 8 & parking_violations_issued_2017_filtered$`Violation Hour` <= 11, "Morning",
                                                                     ifelse(parking_violations_issued_2017_filtered$`Violation Hour` >= 12 & parking_violations_issued_2017_filtered$`Violation Hour` <= 16, "Afternoon",
                                                                            ifelse(parking_violations_issued_2017_filtered$`Violation Hour` >= 17 & parking_violations_issued_2017_filtered$`Violation Hour` <= 20, "Evening",
                                                                                   ifelse(parking_violations_issued_2017_filtered$`Violation Hour` >= 21 & parking_violations_issued_2017_filtered$`Violation Hour` <= 23, "Night",
                                                                                          ifelse(parking_violations_issued_2017_filtered$`Violation Hour` >= 0 & parking_violations_issued_2017_filtered$`Violation Hour` <= 4, "Late Night",""))))))

# For each of these groups, find the 3 most commonly occurring violations
frequency_of_violation_binswise_2017 <- summarize(groupBy(parking_violations_issued_2017_filtered, parking_violations_issued_2017_filtered$`Time Bins`, parking_violations_issued_2017_filtered$`Violation Code`),
                                                  count = n(parking_violations_issued_2017_filtered$`Violation Code`))

createOrReplaceTempView(frequency_of_violation_binswise_2017, "frequency_of_violation_binswise_2017_tbl")
frequency_of_3_most_occuring_violations_2017 <- SparkR::sql("SELECT `Time Bins`,`Violation Code`,count FROM 
                                                            (SELECT `Time Bins`,`Violation Code`,count,dense_rank() 
                                                            OVER (PARTITION BY `Time Bins` ORDER BY count DESC) as rank 
                                                            FROM frequency_of_violation_binswise_2017_tbl) tmp 
                                                            WHERE rank <= 3")

violation_fre_bin_2017<-collect(frequency_of_3_most_occuring_violations_2017)
violation_fre_bin_2017


#     Time Bins Violation Code   count
#       Evening             38  122642
#       Evening              7  100221
#       Evening             14   76871
#       Morning             21 1161225
#       Morning             36  726513
#       Morning             38  343204
# Early Morning             14  137262
# Early Morning             21  115073
# Early Morning             40  104859
#     Afternoon             36  579804
#     Afternoon             38  553366
#     Afternoon             37  405221
#         Night              7   40953
#         Night             40   33935
#         Night             14   32517
#    Late Night             21   54067
#    Late Night             40   49109
#    Late Night             78   30463

# Now, try another direction. For the 3 most commonly occurring violation codes, find the most common times of day (in terms of the bins from the previous part)
# From Question no. 1 we know what are most common violation codes. 21,38,14
frequency_of_times_of_day_2017 <- summarize(groupBy(filter(parking_violations_issued_2017_filtered, parking_violations_issued_2017_filtered$`Violation Code` == 21 |parking_violations_issued_2017_filtered$`Violation Code` == 36| parking_violations_issued_2017_filtered$`Violation Code` == 38), parking_violations_issued_2017_filtered$`Violation Code`, parking_violations_issued_2017_filtered$`Time Bins`),
                                            count = n(parking_violations_issued_2017_filtered$`Time Bins`))

df_times_of_day_freq_2017 <- collect(arrange(frequency_of_times_of_day_2017, frequency_of_times_of_day_2017$`Violation Code`, desc(frequency_of_times_of_day_2017$count)))
df_times_of_day_freq_2017
# Violation Code     Time Bins   count
#             21       Morning 1161225
#             21     Afternoon  145791
#             21 Early Morning  115073
#             21    Late Night   54067
#             21       Evening     391
#             21         Night     275
#             36       Morning  726513
#             36     Afternoon  579804
#             36 Early Morning   30072
#             36       Evening    8848
#             38     Afternoon  553366
#             38       Morning  343204
#             38       Evening  122642
#             38         Night   28465
#             38 Early Morning    2157
#             38    Late Night     568

# Plot the output of violation code based on Time bins for all years
violation_fre_bin_2015$year = "2015"
violation_fre_bin_2016$year = "2016"
violation_fre_bin_2017$year = "2017"

df_vehicle_make_freq <- rbind.data.frame(violation_fre_bin_2015,violation_fre_bin_2016,violation_fre_bin_2017)
ggplot(df_vehicle_make_freq, aes(x=as.factor(`Violation Code`), y = count, fill = `Time Bins`)) + geom_bar(stat="identity") + facet_wrap(~year)

# Plot the output for another direction.
df_times_of_day_freq_2015$year = "2015"
df_times_of_day_freq_2016$year = "2016"
df_times_of_day_freq_2017$year = "2017"

df_times_of_day_freq <- rbind.data.frame(df_times_of_day_freq_2015,df_times_of_day_freq_2016,df_times_of_day_freq_2017)
ggplot(df_times_of_day_freq, aes(x=as.factor(`Violation Code`), y = count, fill = `Time Bins`)) + geom_bar(stat="identity") + facet_wrap(~year)


############################################################################################################

### Q6. Let’s try and find some seasonality in this data ###
### First, divide the year into some number of seasons, and find frequencies of tickets for each season. ###

## We will consider following seasons and their respective months for analysis
## Summer - June to August
## Fall - September to November
## Winter - December to Feburary
## Spring - March to May

## 2015 Year
# First, divide the year into some number of seasons
parking_violations_issued_2015$season <- ifelse(parking_violations_issued_2015$`Issue Month` == 6 | parking_violations_issued_2015$`Issue Month` == 7 | parking_violations_issued_2015$`Issue Month` == 8, "Summer",
                                                ifelse(parking_violations_issued_2015$`Issue Month` == 9 | parking_violations_issued_2015$`Issue Month` == 10 | parking_violations_issued_2015$`Issue Month` == 11, "Fall",
                                                       ifelse(parking_violations_issued_2015$`Issue Month` == 12 | parking_violations_issued_2015$`Issue Month` == 1 | parking_violations_issued_2015$`Issue Month` == 2, "Winter",
                                                              ifelse(parking_violations_issued_2015$`Issue Month` == 3 | parking_violations_issued_2015$`Issue Month` == 4 | parking_violations_issued_2015$`Issue Month` == 5, "Spring",""))))

# find frequencies of tickets for each season
frequencies_of_tickets_seasonwise_2015 <- summarize(groupBy(parking_violations_issued_2015, parking_violations_issued_2015$season),
                                                    count = n(parking_violations_issued_2015$season))

collect(arrange(frequencies_of_tickets_seasonwise_2015, desc(frequencies_of_tickets_seasonwise_2015$count)))
# season   count
# Spring 2860987
# Summer 2838306
#   Fall 2718502
# Winter 2180241

# Then, find the 3 most common violations for each of these season
frequency_of_violation_code_seasonwise_2015 <- summarize(groupBy(parking_violations_issued_2015, parking_violations_issued_2015$season, parking_violations_issued_2015$`Violation Code`),
                                                         count = n(parking_violations_issued_2015$`Violation Code`))

createOrReplaceTempView(frequency_of_violation_code_seasonwise_2015, "frequency_of_violation_code_seasonwise_2015_tbl")
top3_common_violations_seasonwise_2015 <- SparkR::sql("SELECT season,`Violation Code`,count FROM 
                                                      (SELECT season,`Violation Code`,count,dense_rank() 
                                                      OVER (PARTITION BY season ORDER BY count DESC) as rank 
                                                      FROM frequency_of_violation_code_seasonwise_2015_tbl) tmp 
                                                      WHERE rank <= 3")

df_top3_common_violations_seasonwise_2015 <- collect(top3_common_violations_seasonwise_2015)
df_top3_common_violations_seasonwise_2015
# season Violation Code  count
# Spring             21 425163
# Spring             38 327048
# Spring             14 243622
# Summer             21 439632
# Summer             38 344262
# Summer             14 239339
#   Fall             21 351390
#   Fall             38 326700
#   Fall             14 232300
# Winter             38 306997
# Winter             21 253043
# Winter             14 193157

## 2016 Year
# First, divide the year into some number of seasons
parking_violations_issued_2016$season <- ifelse(parking_violations_issued_2016$`Issue Month` == 6 | parking_violations_issued_2016$`Issue Month` == 7 | parking_violations_issued_2016$`Issue Month` == 8, "Summer",
                                                ifelse(parking_violations_issued_2016$`Issue Month` == 9 | parking_violations_issued_2016$`Issue Month` == 10 | parking_violations_issued_2016$`Issue Month` == 11, "Fall",
                                                       ifelse(parking_violations_issued_2016$`Issue Month` == 12 | parking_violations_issued_2016$`Issue Month` == 1 | parking_violations_issued_2016$`Issue Month` == 2, "Winter",
                                                              ifelse(parking_violations_issued_2016$`Issue Month` == 3 | parking_violations_issued_2016$`Issue Month` == 4 | parking_violations_issued_2016$`Issue Month` == 5, "Spring",""))))

# find frequencies of tickets for each season
frequencies_of_tickets_seasonwise_2016 <- summarize(groupBy(parking_violations_issued_2016, parking_violations_issued_2016$season),
                                                    count = n(parking_violations_issued_2016$season))

collect(arrange(frequencies_of_tickets_seasonwise_2016, desc(frequencies_of_tickets_seasonwise_2016$count)))
# season   count
#   Fall 2971672
# Spring 2789066
# Winter 2421620
# Summer 2214536

# Then, find the 3 most common violations for each of these season
frequency_of_violation_code_seasonwise_2016 <- summarize(groupBy(parking_violations_issued_2016, parking_violations_issued_2016$season, parking_violations_issued_2016$`Violation Code`),
                                                         count = n(parking_violations_issued_2016$`Violation Code`))

createOrReplaceTempView(frequency_of_violation_code_seasonwise_2016, "frequency_of_violation_code_seasonwise_2016_tbl")
top3_common_violations_seasonwise_2016 <- SparkR::sql("SELECT season,`Violation Code`,count FROM 
                                                      (SELECT season,`Violation Code`,count,dense_rank() 
                                                      OVER (PARTITION BY season ORDER BY count DESC) as rank 
                                                      FROM frequency_of_violation_code_seasonwise_2016_tbl) tmp 
                                                      WHERE rank <= 3")

df_top3_common_violations_seasonwise_2016 <- collect(top3_common_violations_seasonwise_2016)
df_top3_common_violations_seasonwise_2016
# season Violation Code  count
# Spring             21 383448
# Spring             36 374362
# Spring             38 299439
# Summer             21 358896
# Summer             38 255600
# Summer             14 200608
#   Fall             36 438320
#   Fall             21 395020
#   Fall             38 303387
# Winter             21 359905
# Winter             36 314765
# Winter             38 268409

## 2017 Year

# First, divide the year into some number of seasons
parking_violations_issued_2017$season <- ifelse(parking_violations_issued_2017$`Issue Month` == 6 | parking_violations_issued_2017$`Issue Month` == 7 | parking_violations_issued_2017$`Issue Month` == 8, "Summer",
                                                ifelse(parking_violations_issued_2017$`Issue Month` == 9 | parking_violations_issued_2017$`Issue Month` == 10 | parking_violations_issued_2017$`Issue Month` == 11, "Fall",
                                                       ifelse(parking_violations_issued_2017$`Issue Month` == 12 | parking_violations_issued_2017$`Issue Month` == 1 | parking_violations_issued_2017$`Issue Month` == 2, "Winter",
                                                              ifelse(parking_violations_issued_2017$`Issue Month` == 3 | parking_violations_issued_2017$`Issue Month` == 4 | parking_violations_issued_2017$`Issue Month` == 5, "Spring",""))))

# find frequencies of tickets for each season
frequencies_of_tickets_seasonwise_2017 <- summarize(groupBy(parking_violations_issued_2017, parking_violations_issued_2017$season),
                                                    count = n(parking_violations_issued_2017$season))

collect(arrange(frequencies_of_tickets_seasonwise_2017, desc(frequencies_of_tickets_seasonwise_2017$count)))
# season   count
# Spring 2873383
#   Fall 2829224
# Winter 2483036
# Summer 2353920

# Then, find the 3 most common violations for each of these season
frequency_of_violation_code_seasonwise_2017 <- summarize(groupBy(parking_violations_issued_2017, parking_violations_issued_2017$season, parking_violations_issued_2017$`Violation Code`),
                                                         count = n(parking_violations_issued_2017$`Violation Code`))


createOrReplaceTempView(frequency_of_violation_code_seasonwise_2017, "frequency_of_violation_code_seasonwise_2017_tbl")
top3_common_violations_seasonwise_2017 <- SparkR::sql("SELECT season,`Violation Code`,count FROM 
                                                      (SELECT season,`Violation Code`,count,dense_rank() 
                                                      OVER (PARTITION BY season ORDER BY count DESC) as rank 
                                                      FROM frequency_of_violation_code_seasonwise_2017_tbl) tmp 
                                                      WHERE rank <= 3")

df_top3_common_violations_seasonwise_2017 <- collect(top3_common_violations_seasonwise_2017)
df_top3_common_violations_seasonwise_2017
# season Violation Code  count
# Spring             21 402424
# Spring             36 344834
# Spring             38 271167
# Summer             21 378699
# Summer             38 235725
# Summer             14 207495
#   Fall             36 456046
#   Fall             21 357257
#   Fall             38 283816
# Winter             21 362016
# Winter             36 359338
# Winter             38 259710

# Plot the output of top 3 common violations season wise in each year
df_top3_common_violations_seasonwise_2015$year = "2015"
df_top3_common_violations_seasonwise_2016$year = "2016"
df_top3_common_violations_seasonwise_2017$year = "2017"

df_top3_common_violations_seasonwise <- rbind.data.frame(df_top3_common_violations_seasonwise_2015,df_top3_common_violations_seasonwise_2016,df_top3_common_violations_seasonwise_2017)
ggplot(df_top3_common_violations_seasonwise, aes(x=factor(`season`), y = count, fill = factor(`Violation Code`))) + geom_bar(stat="identity") + facet_wrap(~year)

############################################################################################################

### Q7.The fines collected from all the parking violation constitute a revenue source for the NYC police department. Let’s take an example of estimating that for the 3 most commonly occurring codes. ### 
## Q7.1 Find most common Violation codes and their total occurences and top 3 violations in each year
# 2015 Year
frequency_violation_2015 <- summarize(group_by(parking_violations_issued_2015, parking_violations_issued_2015$`Violation Code`), tickets=n(parking_violations_issued_2015$`Summons Number`))
df_violation_freq_2015 <- head(arrange(frequency_violation_2015, desc(frequency_violation_2015$tickets)),3)
df_violation_freq_2015
# Violation Code  tickets
#            21   1469228
#            38   1305007
#            14   908418

# 2016 Year
frequency_violation_2016 <- summarize(group_by(parking_violations_issued_2016, parking_violations_issued_2016$`Violation Code`), tickets=n(parking_violations_issued_2016$`Summons Number`))
df_violation_freq_2016 <- head(arrange(frequency_violation_2016, desc(frequency_violation_2016$tickets)),3)
df_violation_freq_2016
# Violation Code  tickets
#            21   1497269
#            36   1232952
#            38   1126835

# 2017 Year
frequency_violation_2017 <- summarize(group_by(parking_violations_issued_2017, parking_violations_issued_2017$`Violation Code`), tickets=n(parking_violations_issued_2017$`Summons Number`))
df_violation_freq_2017 <- head(arrange(frequency_violation_2017, desc(frequency_violation_2017$tickets)),3)
df_violation_freq_2017
# Violation Code  tickets
#            21   1500396
#            36   1345237
#            38   1050418

# Plot the output of top 3 violations in each year
df_violation_freq_2015$year = "2015"
df_violation_freq_2016$year = "2016"
df_violation_freq_2017$year = "2017"

df_violation_freq <- rbind.data.frame(df_violation_freq_2015,df_violation_freq_2016,df_violation_freq_2017)
ggplot(df_violation_freq, aes(x=factor(`Violation Code`), y = tickets, fill = factor(`Violation Code`))) + geom_bar(stat="identity") + facet_wrap(~year)


# Q7.2 get the parking fines data from NY.gov website. a csv file is created and the same is uploaded to the S3 bucket.
nyc_fines <- read.df("s3://pk-nyc-parking-tickets/nyc-parking-fines/nyc_parking_fines.csv", source = "csv", inferSchema = "true", header = "true")
printSchema(nyc_fines)

# Remove unwanted columns
nyc_fines$Manhattan <- NULL
nyc_fines$Outside <- NULL

#View(nyc_fines)

# Create Merge dataframe to get the fines for violation codes from nyc_fines data frame.
# 2015 Year
frequency_violation_2015_merged <- merge(frequency_violation_2015, nyc_fines, by.x="`Violation Code`", by.y="Violation_Code", all.x=T)

# 2016 Year
frequency_violation_2016_merged <- merge(frequency_violation_2016, nyc_fines, by.x="`Violation Code`", by.y="Violation_Code", all.x=T)

# 2017 Year
frequency_violation_2017_merged <- merge(frequency_violation_2017, nyc_fines, by.x="`Violation Code`", by.y="Violation_Code", all.x=T)

# Calculate the Total Fine for each violation 
# 2015 Year
frequency_violation_2015_merged$total_fine <- frequency_violation_2015_merged$tickets * frequency_violation_2015_merged$AvgFine

# 2016 Year
frequency_violation_2016_merged$total_fine <- frequency_violation_2016_merged$tickets * frequency_violation_2016_merged$AvgFine

# 2017 Year
frequency_violation_2017_merged$total_fine <- frequency_violation_2017_merged$tickets * frequency_violation_2017_merged$AvgFine

# Remove unwanted columns
# 2015 Year
frequency_violation_2015_merged$Violation_Code <- NULL

# 2016 Year
frequency_violation_2016_merged$Violation_Code <- NULL

# 2017 Year
frequency_violation_2017_merged$Violation_Code <- NULL

# Find the top violation each year by Amount of Fine collected
# 2015 Year
df_top_violation_2015 <- head(arrange(frequency_violation_2015_merged, desc(frequency_violation_2015_merged$total_fine)),1)
df_top_violation_2015
# Violation Code tickets AvgFine total_fine
#             14  908418     115  104468070

# 2016 Year
df_top_violation_2016 <- head(arrange(frequency_violation_2016_merged, desc(frequency_violation_2016_merged$total_fine)),1)
df_top_violation_2016
# Violation Code tickets AvgFine total_fine
#             14  860045     115   98905175

# 2017 Year
df_top_violation_2017 <- head(arrange(frequency_violation_2017_merged, desc(frequency_violation_2017_merged$total_fine)),1)
df_top_violation_2017
# Violation Code tickets AvgFine total_fine
#             14  880152     115  101217480

# Plot the output of top violation by total amount collected in each year
df_top_violation_2015$year = "2015"
df_top_violation_2016$year = "2016"
df_top_violation_2017$year = "2017"

df_top_violation <- rbind.data.frame(df_top_violation_2015,df_top_violation_2016,df_top_violation_2017)
ggplot(df_top_violation, aes(x = year, y = total_fine, fill = factor(`Violation Code`))) + geom_bar(stat="identity")
