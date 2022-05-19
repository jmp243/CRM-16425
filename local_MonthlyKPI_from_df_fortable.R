# Jung Mee Park
# attempt to reproduce Francis's tables
# 2022-02-02
# for local use
# last updated 2022-03-02

#### SET UP Environment ####
library('tidyverse')
library(dbplyr)
library('readxl')
library('salesforcer')
# library('RForcecom') # not available in this version of R on 08 Dec 2021
library('lubridate')
library(reshape2)
library(seplyr)
library(pointblank)
library(kableExtra) # added 2022-02-25
library(knitr)
library(janitor)
#### check working directory ####
getwd() #figure out the working directory
setwd("~/Documents/Trellis/CRM-16425/CRM-16425-exploration/Apr2022 data")

rm(list=ls(all=TRUE)) 
options(digits=3)

today <- Sys.Date()

####capture current month####
this_month <- month(today)
last_month<-month(today() - months(1))
this_year <- year(today)
last_year <-year(today() - years(1))

format(today, format="%B %d %Y")

####function to write csv####
write_named_csv <- function(x) 
  write_csv(x, file = paste0(deparse(substitute(x)), last_month, this_year,".csv"))

####read in csv####
# df_foruse <- read.csv("df_foruse12022.csv") # foruse limited variables and took out admin accts
# df_fortable <- read.csv("df_fortable12022.csv") 
# df_month2 <- read.csv("df_month212022.csv")
# df_month2 <- read.csv("df_month222022.csv")
df_foruse <- read.csv("df_foruse242022.csv")
df_fortable <- read.csv("df_fortable42022.csv")
# df_foruse_non_na <- read.csv("df_foruse_non_na42022.csv")
# MC_s_c_u_foruse <- read.csv("MC_s_c_u_foruse42022.csv")
MC_s_c_u <- read.csv("MC_s_c_u242022.csv")

#### testing out the tables ####
# df_foruse %>% 
#   distinct(Email,MCProfile) %>%
#   group_by(MCProfile) %>% 
#   count()%>% 
#   ungroup() # n=150 
# df_foruse %>% 
#   distinct(Email, SocialProfile) %>%
#   group_by(SocialProfile) %>% 
#   count()%>% 
#   ungroup() # n=82
df_foruse %>% 
  distinct(Name, MC.Social.SFProduct) %>%
  group_by(MC.Social.SFProduct) %>% 
  count()%>% 
  ungroup() 
#### removing Student & Acad Technologies ####
# df_foruse <- df_month2[df_month2$hed__Account__r.Name != "Student & Acad Technologies", ] 

#### testing out the tables ####
df_fortable %>% 
  distinct(Email,MCProfile) %>%
  group_by(MCProfile) %>% 
  count()%>% 
  ungroup() # n=150 
df_fortable %>% 
  distinct(Email, SocialProfile) %>%
  group_by(SocialProfile) %>% 
  count()%>% 
  ungroup() # n=82

# df_subset_date <- df_fortable %>% 
#   dplyr::filter(SF_LastLoginDate >= "2022-02-01" & SF_LastLoginDate <="2022-02-28") %>% 
#   distinct()
df_fortable %>% 
  dplyr::filter(SF_LastLoginDate >= "2022-02-01" & SF_LastLoginDate <="2022-02-28") %>% 
  distinct(Email, SFProduct) %>% 
  group_by(SFProduct) %>% 
  count() %>% 
  ungroup() 


df_fortable2 <- df_fortable %>% 
  dplyr::filter(SF_LastLoginDate >= "2022-02-01" & SF_LastLoginDate <="2022-02-28") 


# df_fortable %>% 
#   group_by(Email) %>%
#   summarise_each(funs(sum), - MCProfile, - SocialProfile, - SFProduct) %>% 
#   gather(Var, Val, - Date_Range) %>%
#   group_by(Date_Range) %>% 
#   mutate(ind = row_number()) %>% 
#   spread(Date_Range, Val)

# df_fortable %>%
#   group_by(Email, SFProduct, MCProfile, SocialProfile) %>% 
#   summarize(Freq = count())

# pivot longer to produce a table
# make one long table with MCProfile, SocialProfile, and SFProduct
head(df_fortable)
# pivoting to make a new dataframe
product_longer <- df_fortable %>% 
  pivot_longer(cols = c(MCProfile, SocialProfile, SFProduct),
               names_to = "col_name", 
               values_to = "MC-Social-SFProduct") %>% 
  drop_na("MC-Social-SFProduct") %>% 
  distinct()

write_named_csv(product_longer)

# check tables
product_longer %>% 
  # dplyr::filter(SF_LastLoginDate >= "2022-02-01" & SF_LastLoginDate <="2022-02-28") %>% 
  distinct(Email,`MC-Social-SFProduct`) %>%
  group_by(`MC-Social-SFProduct`) %>% 
  count()%>% 
  ungroup() # n=150 

df_fortable %>% 
  # dplyr::filter(SF_LastLoginDate >= "2022-02-01" & SF_LastLoginDate <="2022-02-28") %>% 
  distinct(NetID__c, MC.Social.SFProduct) %>%
  group_by(MC.Social.SFProduct) %>% 
  count()%>% 
  ungroup() # n=150 

### add netid value if name present, add name if netid present
df_fortable2 <- df_fortable %>%
  dplyr::mutate(NetID = case_when(!is.na(NetID__c) ~ NetID__c,
                              TRUE ~ Name))

write_named_csv(df_fortable2)

# count number of distinct NetID from df_fortable2
library(sqldf)
sqldf("select count(distinct NetID) from df_fortable2") #1233


  # mutate(NetID = ifelse(`Q7.2_1 Have used Release notes` == "I have used/attended.",
  #                       "I am aware.", 
  #                       `Q7.1_1 Aware of Release notes`))
# old way to merge
SFuserscount_t<- df_fortable %>%
  dplyr::filter(SF_LastLoginDate >= "2022-02-01" & SF_LastLoginDate <="2022-02-28") %>% 
  group_by(Email, SFProduct) %>% 
  summarise(Total = n())

MCuserscount_t<- df_fortable %>%
  group_by(Email, MCProfile) %>% 
  summarise(Total = n())
Socuserscount_t<- df_fortable %>%
  group_by(Email, SocialProfile) %>% 
  summarise(Total = n())
colnames(MCuserscount_t)[2]<-"SFProduct" #change from MC 
colnames(Socuserscount_t)[2]<-"SFProduct" #change from Social
products_table <- rbind(SFuserscount_t, MCuserscount_t, Socuserscount_t)

write_named_csv(products_table)
# merge dataset with different variables 
products_table_df <- left_join(products_table, df_fortable, by = "Email")

# drop columns
products_table_df2 <- products_table_df[, !colnames(products_table_df) %in% c("Total", "Username")]

# rename the column
colnames(products_table_df2)[2]<-"SFProduct/MC/Social" #SFProduct.x

write_named_csv(products_table_df2)

#### FKM total users by product####
SFuserscount<-as.data.frame(table(df_fortable$SFProduct))

# View(MCuserscount_t)
MCuserscount<-as.data.frame(table(df_fortable$MCuserscount_t))
# View(MCuserscount)
MCuserscount_t<-subset(MCuserscount_t, !is.na(MCuserscount_t$MCProfile))
MCuserscount<-as.data.frame(table(MCuserscount_t$Email))
MCuserscount<-as.data.frame(table(MCuserscount_t$MCProfile))

Socuserscount_t<-subset(Socuserscount_t, !is.na(Socuserscount_t$SocialProfile))
Socuserscount<-as.data.frame(table(Socuserscount_t$SocialProfile))
user_counts_by_product<-rbind(SFuserscount, MCuserscount, Socuserscount)

#### JMP attempt total users by products ####
# MCProfile_df <- df_fortable_non_na[!(is.na(df_fortable_non_na$MCProfile) | df_fortable_non_na$MCProfile==""), ]
MCProfile_df1 <- df_foruse_non_na[!(is.na(df_foruse_non_na$MCProfile) | df_foruse_non_na$MCProfile==""), ]
MC_count1 <- unique(as.data.frame(table(MCProfile_df1$Email))) #69 

MC_count1 <- MCProfile_df %>% 
  group_by(Email) %>%
  # summarise(Total = n())
  summarise(MC_count = n_distinct(MCProfile)) #170 instances of MC but there are 69 distinct emails

# data %>%                    # take the data.frame "data"
#   filter(!is.na(aa)) %>%    # Using "data", filter out all rows with NAs in aa 
#   group_by(bb) %>%          # Then, with the filtered data, group it by "bb"
#   summarise(Unique_Elements = n_distinct(aa))   # Now summarise with unique elements per group

df_foruse_non_na  %>%
  tabyl(MCProfile, SocialProfile, show_missing_levels = FALSE) %>%
  adorn_totals("row") %>%
  adorn_percentages("all") %>%
  adorn_pct_formatting(digits = 1) %>%
  adorn_ns %>%
  adorn_title

SocialProfile_df <- df_month2[!(is.na(df_month2$SocialProfile) | df_month2$SocialProfile==""), ]
Socialcount<-as.data.frame(table(SocialProfile_df$Email)) #15

df_foruse_non_na %>%                    # take the data.frame "data"
  filter(!is.na(SocialProfile)) %>%    # Using "data", filter out all rows with NAs in aa
  group_by(Email) %>%          # Then, with the filtered data, group it by "bb"
  summarise(Unique_Elements = n_distinct(Email))   # Now summarise with unique elements per group
# only be 15

# Select only SF Product == SAFER
df_foruse_non_na %>% 
  filter(SFProduct=="SAFER") %>% 
  # summarise(Email_count = n_distinct(Email)) %>% 
  count(Primary_Department__r.Name, Profile.Name)
  # summarise(n = length(uniqe(Email))) #83 emails are associated with it

df_foruse_non_na %>%                    # take the data.frame "data"
  filter(!is.na(SFProduct)) %>%    # Using "data", filter out all rows with NAs in aa
  group_by(SFProduct, Email) %>%          # Then, with the filtered data, group it by "bb"
  summarise(Unique_Elements = n_distinct(SFProduct)) %>% # Now summarise with unique elements per group
  mutate(Freq = Unique_Elements/sum(Unique_Elements))

#### Usable User Count Table ####
#SFuserscount<-as.data.frame(table(df_fortable$SFProduct))
SFProduct_count <- df_foruse_non_na%>% 
  filter(!is.na(SFProduct)) %>%
  dplyr::distinct(Email, SFProduct) %>%
  group_by(SFProduct) %>% 
  summarize(Freq = n()) %>% 
  ungroup()

MCProfile_count <- df_foruse_non_na%>% 
  filter(!is.na(MCProfile)) %>%
  dplyr::distinct(Email, MCProfile) %>%
  group_by(MCProfile) %>% 
  summarize(Freq = n())%>% 
  ungroup()

SocialProfile_count <- df_foruse_non_na%>% 
  filter(!is.na(SocialProfile)) %>%
  dplyr::distinct(Email, SocialProfile) %>%
  group_by(SocialProfile) %>% 
  summarize(Freq = n())%>% 
  ungroup()

# bind the tables
colnames(MCProfile_count)[1]<-"Product/Profile"
colnames(SocialProfile_count)[1]<-"Product/Profile"
colnames(SFProduct_count)[1]<-"Product/Profile"
user_counts_by_product<-rbind(SFProduct_count, MCProfile_count, SocialProfile_count)

## alternatively
df_foruse_non_na%>% 
  filter(!is.na(SFProduct)) %>%
  dplyr::distinct(Email, SFProduct) %>%
  group_by(SFProduct) %>% 
  tabyl(SFProduct)

df_foruse_non_na %>%                    # take the data.frame "data"
  filter(!is.na(SocialProfile)) %>%    # Using "data", filter out all rows with NAs in aa
  group_by(Email) %>%          # Then, with the filtered data, group it by "bb"
  summarise(Unique_Elements = n_distinct(Email)) %>% 
  tabyl(Unique_Elements)# Now summarise with unique elements per group
# only be 15

##total users by product by unit
sftest<-df_fortable %>% group_by(SFProduct, Parent_Organization__c) %>% tally()

SFUC_unit<-as.data.frame(table(df_fortable$SFProduct, by=df_fortable$Parent_Organization__c))
SFUC_unit<-subset(SFUC_unit, SFUC_unit$Freq!=0)

MCUC_unit<-as.data.frame(table(df_fortable$MCProfile, by=df_fortable$Parent_Organization__c))
MCUC_unit<-subset(MCUC_unit, MCUC_unit$Freq!=0)

SocUC_unit<-as.data.frame(table(df_fortable$SocialProfile, by=df_fortable$Parent_Organization__c))
SocUC_unit<-subset(SocUC_unit, SocUC_unit$Freq!=0)

user_cts_by_prd_unit<-rbind(SFUC_unit, MCUC_unit, SocUC_unit)

##total users by unit
users_by_unit<- user_cts_by_prd_unit %>%
  group_by(by) %>% 
  summarise(sum(Freq))

##total products by unit
prds_by_unit<- user_cts_by_prd_unit %>%
  group_by(by) %>% 
  summarise(Total = n())

##reshaped products by unit
for_reshape<-user_cts_by_prd_unit[, -c(3)]
for_reshape$counter<-1
for_reshape<-unique(for_reshape)
prds_by_unit_reshaped<-reshape(for_reshape, v.names="counter", timevar="Var1", idvar="by",
                               direction="wide")

###make total activity var
names(prds_by_unit_reshaped)
prds_by_unit_reshaped<-prds_by_unit_reshaped %>% 
  #rowwise will make sure the sum operation will occur on each row
  rowwise() %>% 
  #then a simple sum(..., na.rm=TRUE) is enough to result in what you need
  mutate(Total = sum(`counter.Service Desk`, `counter.Marketing - SF`, `counter.Scheduling/Notes`, counter.MC, counter.Social, counter.Events, na.rm=TRUE))

### rename vars and write tables to files and folder
prds_by_unit_reshaped<-rename(prds_by_unit_reshaped, c("Unit/Division" = "by", "MC" = "counter.MC", "Social" = "counter.Social",
                                                       "Scheduling/Notes" = "counter.Scheduling/Notes", "Service Desk" = "counter.Service Desk", "Marketing - SF" = "counter.Marketing - SF", "Events" = "counter.Events"))
user_counts_by_product<-rename(user_counts_by_product,c("Product/Profile" = "Var1", "Total Users" = "Freq"))
user_cts_by_prd_unit <- rename(user_cts_by_prd_unit,c("Unit/Division" = "by", "Product/Profile" = "Var1", "Total Users" = "Freq"))
users_by_unit<-rename(users_by_unit, c("Unit/Division" = "by", "Total Users"="sum(Freq)"))
prds_by_unit<-rename(prds_by_unit, c("Unit/Division" = "by", "Total Products/Profiles" = "Total"))

write_named_csv(user_counts_by_product)
write_named_csv(user_cts_by_prd_unit) #, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/user_cts_by_prd_unit", last_month, last_year, ".csv"), row.names = FALSE)
write_named_csv(users_by_unit) #, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/users_by_unit", last_month, last_year, ".csv"), row.names = FALSE)
write_named_csv(prds_by_unit) #, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/prds_by_unit", last_month, last_year, ".csv"), row.names = FALSE)
write_named_csv(prds_by_unit_reshaped) #, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/prds_by_unit_reshaped", last_month, last_year, ".csv"), row.names = FALSE)

