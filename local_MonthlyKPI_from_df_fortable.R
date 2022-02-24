# Jung Mee Park
# attempt to reproduce Francis's tables
# 2022-02-02
# for local use

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

#### check working directory ####
getwd() #figure out the working directory
setwd("~/Documents/Trellis/CRM-16425/CRM-16425-exploration/Jan2022 data")

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
df_foruse <- read.csv("df_foruse12022.csv")
df_fortable <- read.csv("df_fortable12022.csv")
df_month2 <- read.csv("df_month212022.csv")

#### removing Student & Acad Technologies ####
df_foruse <- df_month2[df_month2$hed__Account__r.Name != "Student & Acad Technologies", ] 

####check df_foruse####
names(df_foruse)
df_foruse <- df_foruse %>% 
  # select(-NetID__c.x, -NetID__c.y, -SFUserId, -Academic_Department__c) %>% 
  # select(-SFUserId, -Academic_Department__c) %>% 
  #remove redundant columns
  distinct() 


write_named_csv(df_foruse)

####floor the dates to all Jan 2022####
df_month <- df_use %>%
  mutate(MCLoginMonth = floor_date(as_date(MC_LastLoginDate), "month")) %>% 
  mutate(SFLoginMonth = floor_date(as_date(SF_LastLoginDate), "month")) %>% 
  mutate(SocialLoginMonth = floor_date(as_date(Social_LastLoginDate), "month")) %>% 
  mutate(LastLoginTime = floor_date(as_date(LoginTime), "month"))

df_month <- df_month %>% 
  select(-MC_LastLoginDate, -SF_LastLoginDate, -Social_LastLoginDate, -LoginTime)

df_month2 <- df_month %>%
  # filter(FL_DATE >= as.Date("2014-01-05"))
  filter(SFLoginMonth >= as.Date("2022-01-01"))

# remove redundant columns 
df_month2 <- unique(df_month2)


#### write csv
write_named_csv(df_use)
write_named_csv(df_month2)

names(df_fortable)
df_fortable <- df_foruse %>% select(Email, Primary_Department__r.Name,	MCProfile,
                                    SocialProfile,SFProduct,Parent_Organization__c,hed__Account__r.Name)
df_fortable<-unique(df_fortable) # 1440

#check outcome
df_foruse %>% count(df_foruse$MCProfile)
length(unique(df_foruse$Email[df_foruse$MCProfile=="MC"]))

## drop none logins
df_foruse_non_na <- df_foruse %>% 
  mutate(num_na = is.na(MC_LastLoginDate) + is.na(Social_LastLoginDate) + is.na(SF_LastLoginDate)) %>% 
  filter(num_na < 3)


# remove duplicates 
# df_fortable2 <- df_fortable[!duplicated(df_fortable$Last.Login), ]
# df_fortable2<-unique(df_fortable2)
# df_fortable2 <- df_fortable %>% 
#   group_by(SFProduct) %>% 
#   dplyr::mutate(count=sum(!(is.na(n))), sum = sum(n, na.rm = T))

# write to csv
# write.csv(df_fortable, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/df_fortable", last_month, this_year, ".csv"), row.names = FALSE)

names(df_fortable)
####total users by product####
SFuserscount<-as.data.frame(table(df_fortable$SFProduct))
MCuserscount_t<- df_fortable %>%
  group_by(Email.x, MCProfile) %>% 
  summarise(Total = n())
View(MCuserscount_t)
MCuserscount<-as.data.frame(table(df_fortable$MCuserscount_t))
View(MCuserscount)
MCuserscount_t<-subset(MCuserscount_t, !is.na(MCuserscount_t$MCProfile))
MCuserscount<-as.data.frame(table(MCuserscount_t$Email.x))
MCuserscount<-as.data.frame(table(MCuserscount_t$MCProfile))
Socuserscount_t<- df_fortable %>%
  group_by(Email.x, SocialProfile) %>% 
  summarise(Total = n())
Socuserscount_t<-subset(Socuserscount_t, !is.na(Socuserscount_t$SocialProfile))
Socuserscount<-as.data.frame(table(Socuserscount_t$SocialProfile))
View(Socuserscount)
user_counts_by_product<-rbind(SFuserscount, MCuserscount, Socuserscount)


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

