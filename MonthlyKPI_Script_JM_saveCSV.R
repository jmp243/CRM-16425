###Script to merge and output formatted Trellis USer Data Monthly
##Follows these steps:
#1. Import the data from Brett
#2. Import the data from SF Contacts
#3. merge files together using netid
#4. Collapse into table with counts by department/unit and product used
#5. Export data to csv file

#LAST RUN AND UPDATED ON 03 December 2021

#SET UP Environment
library('tidyverse')
library('dplyr')
library('readxl')
library('salesforcer')
# library('RForcecom') # not available in this version of R on 08 Dec 2021
library('lubridate')
library(reshape2)

### check working directory
getwd() #figure out the working directory
#setwd("~/Trellis Users KPIs") 
setwd("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/")

rm(list=ls(all=TRUE)) 
options(digits=3)

today <- Sys.Date()

#capture current month
this_month <- month(today)
last_month<-month(today() - months(1))
this_year <- year(today)


format(today, format="%B %d %Y")

############################################
##STEP 1: Read in the data from Brett

# setwd("U:/WorkSpaces/fkm_data/Program Team KPIs/") 
Social_Logins <- read_csv("November Splunk Reports/Users,_last_login,_social_studio-2021-12-01.csv")
MC_Logins <- read_csv("November Splunk Reports/Users,_last_login,_marketing_cloud-2021-12-01.csv")

##look at data to see format and variables
glimpse(MC_Logins)
glimpse(Social_Logins)

# perms2prods<-read_xlsx("Raw/Permissionsets_mapped_to_Products.xlsx")
# perms2prods<-read_csv("user_perms_prods.csv")

############################################
##STEP 2: Import data from SF ------ Paused for now while Brett fixes something

#Oauth
sf_auth()
# 
# sf_auth(login_url="https://login.salesforce.com")

###bring in Contact Records
my_soql_contact <- sprintf("SELECT Id,	Emplid__c, Email, User__c, Primary_Department__r.Name, 
                           NetId__c, hed__Primary_Organization__c, MDM_Primary_Type__c
                           FROM Contact")

contact_records <- sf_query(my_soql_contact, object_name="Contact", api_type="Bulk 1.0")

###bring in User object fields
soql_users<-sprintf("select Id, Email, UserType, Name, NetID__c, Profile.Name, 
                    ContactId, CreatedDate,
                    Department, ProfileId, UserRoleId, 
                    UserRole.Name, Title, Username, LastLoginDate 
                    from User
                    WHERE IsActive=TRUE ")
users_SF <- sf_query(soql_users, object_name="User", api_type="Bulk 1.0")

#####Bring In User Login History
solq_logins<-sprintf("Select UserId, Browser, LoginTime, LoginType, Platform, Status from LoginHistory
                     WHERE LoginTime > 2019-10-01T00:00:00.000Z")
user_logins <- sf_query(solq_logins, object_name="LoginHistory", api_type="Bulk 1.0")

###bring in affiliation Records
my_soql_aff <- sprintf("SELECT Academic_Department__c, hed__Affiliation_Type__c, hed__Contact__c, 
                        hed__Primary__c, hed__Account__r.Name, Parent_Organization__c
                           FROM hed__Affiliation__c")

affiliations <- sf_query(my_soql_aff, object_name="hed__Affiliation__c", api_type="Bulk 1.0")


###bring in permissionsets Records
solq_perms<-sprintf("select AssigneeId, PermissionSet.Name, PermissionSet.Type, PermissionSet.ProfileId, 
                    PermissionSetGroupId from PermissionSetAssignment")
permissionsets <- sf_query(solq_perms, object_name="PermissionSetAssignment", api_type="Bulk 1.0")

library(readxl)
perms2prods <- read.csv("user_perms_prods.csv")
# View(perms2prods)

users<-read.csv("MtD Licensed Users Logged In.csv") #not sure if this needed to stay commented out

############################################
##STEP 3: Merge files

##restrict to users with last logins this month

str(MC_Logins)

#MC_Logins<-subset(MC_Logins, month(as.Date(MC_Logins$"max(timestamp)", "%m/%d/%Y")) == this_month & year(as.Date(MC_Logins$"max(timestamp)", "%m/%d/%Y"))==this_year)
#Social_Logins<-subset(Social_Logins, month(as.Date(Social_Logins$"max(timestamp)", "%m/%d/%Y")) == this_month & year(as.Date(Social_Logins$"max(timestamp)", "%m/%d/%Y")) == this_year)
MC_Logins <- subset(MC_Logins, month(as.Date(MC_Logins$"max(timestamp)", "%m/%d/%Y")) 
                    == last_month & year(as.Date(MC_Logins$"max(timestamp)", "%m/%d/%Y"))==this_year)
Social_Logins <- subset(Social_Logins, month(as.Date(Social_Logins$"max(timestamp)", "%m/%d/%Y")) 
                        == last_month & year(as.Date(Social_Logins$"max(timestamp)", "%m/%d/%Y")) == this_year)

#users<-subset(user_logins, month(user_logins$LoginTime)== this_month & year(user_logins$LoginTime)==this_year)
# this yields
# users<-subset(user_logins, month(user_logins$LoginTime) == last_month & year(user_logins$LoginTime)==this_year) # this yields 0
# users<-merge(users_SF,users, by.x = "Id", by.y = "UserId")

# try to merge on email from Jung Mee
users<-merge(users_SF,users, by.x = "Email", by.y = "Username") # this produced something with 23 variables
# users<-users[,-c(7,10,13,16:20)] # from FKM not necessarily because LastLogin can be important
users <- users[, -c(7,16:19)]
users<-unique(users)

#rename var indicating that login was for a particular tool
MC_Logins$Profile.Name<-"MC"
Social_Logins$Profile.Name<-"Social"
#Social_Logins<-subset(Social_Logins, !is.na(Social_Logins$netid))

users %>% group_by(Profile.Name) %>% tally
##restrict User file to those we want
users<-subset(users, users$Profile.Name %in% c("Salesforce Base", "Advising Base", "Service Desk Base"))
users$NetID__c[users$NetID__c=="Mlfink3"]<-"mlfink3"
# save to csv
write.csv(users, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/users", last_month, this_year, ".csv"), row.names = FALSE)

##reshape the permissionsets file

# permset_tally<-permissionsets %>% group_by(PermissionSet.Name) %>% tally
# write.csv(permset_tally,"permissionsets.csv")
# permissionsets %>% group_by(PermissionSet.Type) %>% tally

names(users)
names(permissionsets)
names(perms2prods)

users_perms<-merge(users, permissionsets, by.x = "Id", by.y = "AssigneeId")
users_perms_prods<-merge(users_perms, perms2prods, by.x = "PermissionSet.Name", 
                         by.y = "PermissionSet.Name", all.x = TRUE)
#save to csv
write.csv(users_perms, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/users_perms", last_month, this_year, ".csv"), row.names = FALSE)
write.csv(users_perms_prods, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/users_perms_prods", last_month, this_year, ".csv"), row.names = FALSE)

names(users_perms_prods)
# users_perms_prods<-users_perms_prods[, -c(10,14,15,16)] # no real need to delete columns

#write.csv(users_perms_prods, file = "user_perms_prods.csv")
#
#merge MC into contacts
#
names(MC_Logins) 
contact_records<-subset(contact_records, !is.na(contact_records$NetID__c))

#save to csv
write.csv(contact_records, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/contact_records", last_month, this_year, ".csv"), row.names = FALSE)

##need to check to make sure the email addresses are @email.arizona.edu, not @arizona.edu before merge - ok
# MC_Logins_Feb_2021$pre<-sapply(strsplit(MC_Logins_Feb_2021$`Email Address`, "@"), "[", 1)
# MC_Logins_Feb_2021$emailright<-sapply(strsplit(MC_Logins_Feb_2021$`Email Address`, "@"), "[", 2)
# MC_Logins_Feb_2021$dummyemail<-paste(MC_Logins_Feb_2021$pre,"email.arizona.edu", sep = "@", collapse = NULL)
# MC_Logins_Feb_2021$`Email Address`[MC_Logins_Feb_2021$emailright=="arizona.edu"]<-MC_Logins_Feb_2021$dummyemail


colnames(MC_Logins)[1]<-"NetID__c"
colnames(MC_Logins)[2]<-"LastLoginDate"
colnames(Social_Logins)[1]<-"NetID__c"
colnames(Social_Logins)[2]<-"LastLoginDate"

# MC_Logins$Email[MC_Logins$Email=="letsonn@arizona.edu"]<-"letsonn@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="abarela1@arizona.edu"]<-"abarela1@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="dumonta@arizona.edu"]<-"dumonta@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="ksouth@arizona.edu"]<-"ksouth@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="lisas@arizona.edu"]<-"lisas@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="mveres@arizona.edu"]<-"mveres@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="mdenham@arizona.edu"]<-"mdenham@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="nprevenas@arizona.edu"]<-"nprevenas@email.arizona.edu"

MC_contacts<-merge(contact_records, MC_Logins, by.x = "NetID__c", by.y = "NetID__c", all = TRUE)
#MC_contacts<-subset(MC_contacts, is.na(MC_contacts$MDM_Primary_Type__c) | MC_contacts$MDM_Primary_Type__c %in% c("former_student","former-member","student","staff", "studentworker", "gradasst", "staff", "dcc", "admit", "affiliate", "faculty"))

##see which email addresses in MC have no contact match (n=11) - go back and change these email addresses in the MC file
pop_density_no_match <- anti_join(MC_Logins, contact_records, 
                                  by = c("NetID__c" = "NetID__c")) # zero in this case is good

#
#Merge social into contacts plus MC
#
MC_contacts_social<-merge(MC_contacts, Social_Logins, by.x = c("NetID__c"), by.y = c("NetID__c"), all = TRUE)

MC_contacts_social %>% group_by(MDM_Primary_Type__c) %>% tally

#
#Merge users into contacts plus MC plus social
#
length(unique(MC_contacts_social$NetID__c))
names(MC_contacts_social)
#MC_contacts_social <- MC_contacts_social[, c(1:8,10,13,18:21)]

colnames(MC_contacts_social)[9]<-"MC_LastLoginDate"
colnames(MC_contacts_social)[10]<-"MCProfile"
colnames(MC_contacts_social)[11]<-"Social_LastLoginDate"
colnames(MC_contacts_social)[12]<-"SocialProfile"

# write csv
write.csv(MC_contacts_social, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/MC_contacts_social", last_month, this_year, ".csv"), row.names = FALSE)

names(users_perms_prods)
# upp_clean <- users_perms_prods[, c(1,2,4,7,8,11:13)] # product not kept in this code
# upp_clean <- users_perms_prods[, c(1,2,4,7,8,11:14, 35)] # had to add these
upp_clean <- users_perms_prods[, -c(3)]
upp_clean<-unique(upp_clean)
upp_clean<-subset(upp_clean, upp_clean$Profile.Name!="System Administrator")
upp_clean %>% group_by(Profile.Name) %>% tally 
# upp_clean<-subset(upp_clean, !is.na(upp_clean$Product)) #turns into 0 

# upp_clean <- rename(upp_clean,c("SFUserId" = "Id", "SFCreatedDate" = "CreatedDate", "SFProduct" = "Product"))
# I don't see these names in my variables list

MC_s_c_u<-merge(MC_contacts_social, upp_clean, by.x = c("NetID__c"), by.y = c("NetID__c.x"), all=TRUE)
# tried with NetID__c.y instead of NetID__c.x. 
names(MC_s_c_u)
colnames(MC_s_c_u)[16]<-"SF_LastLoginDate" # check the column numbers

# MC_s_c_u <- MC_s_c_u[, -c(4,6,7,15,18)] #skip this process on 12/13/2021

MC_s_c_u_foruse<-subset(MC_s_c_u, (!is.na(MC_s_c_u$SF_LastLoginDate) | !is.na(MC_s_c_u$Social_LastLoginDate) | !is.na(MC_s_c_u$MC_LastLoginDate)))

names(MC_s_c_u_foruse)
length(unique(MC_s_c_u_foruse$Id[MC_s_c_u_foruse$MCProfile=="MC"]))
test<-subset(MC_s_c_u_foruse, MC_s_c_u_foruse$SocialProfile=="Social")

# write to csv
write.csv(MC_s_c_u_foruse, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/MC_s_c_u_foruse", last_month, this_year, ".csv"), row.names = FALSE)

#
#Merge in Affiliations
#
names(affiliations)
affiliation1s <- subset(affiliations, affiliations$hed__Primary__c==TRUE)
affiliation1s <- affiliation1s[, -c(4)]
affiliation1s<-distinct(affiliation1s)

# write to csv
write.csv(affiliation1s, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/affiliation1s", last_month, this_year, ".csv"), row.names = FALSE)

df_foruse<-merge(MC_s_c_u_foruse, affiliation1s, by.x = "Id", by.y = "hed__Contact__c", all.x = TRUE)
##take out all Trellis team members from counts
df_foruse<-subset(df_foruse, !(df_foruse$NetID__c %in% c("sananava", "fkmiller", "mariovasquez","karamcintyre", "rsrobrahn", "sarceneaux", "krusie", "gestautus", "cynthiavaldez", "puffvu", "cyrusmadrone")))

#check outcome
df_foruse %>%group_by(df_foruse$MCProfile)%>% tally
length(unique(df_foruse$Email[df_foruse$MCProfile=="MC"])) # removed the .x from email

# write to csv
write.csv(df_foruse, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/df_foruse", last_month, this_year, ".csv"), row.names = FALSE)

#restrict columns to those we need
names(df_foruse)
# df_fortable <- df_foruse[, -c(1,2,4,6,8,10,11,12,13)] # removed the 15 - createddate and 16 to keep product
df_fortable <- df_foruse[, -c(1,7)] # what jung mee wants to do. 
df_fortable<-unique(df_fortable)

# write to csv
write.csv(df_fortable, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/df_fortable", last_month, this_year, ".csv"), row.names = FALSE)

#########################################################
#########################################################
#######################################################
##table construction
names(df_fortable)
# ##total users by product
# SFuserscount<-as.data.frame(table(df_fortable$SFProduct))
SFuserscount<-as.data.frame(table(df_fortable$Product))
# MCuserscount<-as.data.frame(table(df_fortable$MCProfile))
MCuserscount<-as.data.frame(table(df_fortable$Profile))
Socuserscount<-as.data.frame(table(df_fortable$SocialProfile))

user_counts_by_product<-rbind(SFuserscount, MCuserscount, Socuserscount) 
colnames(user_counts_by_product)[1]<-"Product/Profile"

##total users by product by unit
sftest<-df_fortable %>% group_by(Product, Parent_Organization__c) %>% tally() #not SFProduct but Product

SFUC_unit<-as.data.frame(table(df_fortable$Product, by=df_fortable$Parent_Organization__c))
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
# for_reshape<-user_cts_by_prd_unit[, -c(3)]
# for_reshape$counter<-1
# for_reshape<-unique(for_reshape)
# prds_by_unit_reshaped<-reshape(for_reshape, v.names="counter", timevar="Var1", idvar="by",
#                                direction="wide")
# ###make total activity var
# names(prds_by_unit_reshaped)
# prds_by_unit_reshaped<-prds_by_unit_reshaped %>% 
#   #rowwise will make sure the sum operation will occur on each row
#   rowwise() %>% 
#   #then a simple sum(..., na.rm=TRUE) is enough to result in what you need
#   mutate(Total = sum(`counter.Service Desk`, `counter.Marketing - SF`, `counter.Scheduling/Notes`, counter.MC, counter.Social, counter.Events, na.rm=TRUE))

# pivot longer Jung Mee 12/13/2021
prds_by_unit_reshaped <- user_cts_by_prd_unit %>%
  group_by(Var1) %>%
  summarise(nProduct = n_distinct(by))

### rename vars and write tables to files and folder
prds_by_unit_reshaped<-rename(prds_by_unit_reshaped, c("Unit/Division" = "by", "MC" = "counter.MC", "Social" = "counter.Social",
                                                       "Scheduling/Notes" = "counter.Scheduling/Notes", "Service Desk" = "counter.Service Desk", "Marketing - SF" = "counter.Marketing - SF", "Events" = "counter.Events"))
# user_counts_by_product<-rename(user_counts_by_product,  c("Product/Profile" = "Var1"))
# user_cts_by_prd_unit <- rename(user_cts_by_prd_unit, c("Unit/Division" = "by", "Product/Profile" = "Var1"))
# users_by_unit<-rename(users_by_unit, c("Unit/Division" = "by", "Total Users"="sum(Freq)"))
# prds_by_unit<-rename(prds_by_unit, c("Unit/Division" = "by", "Total Products/Profiles" = "Total"))

# setwd<-"U:/WorkSpaces/fkm_data/Program Team KPIs/"

write.csv(user_counts_by_product, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/user_counts_by_product", last_month, this_year, ".csv"), row.names = FALSE)
write.csv(user_cts_by_prd_unit, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/user_cts_by_prd_unit", last_month, this_year, ".csv"), row.names = FALSE)
write.csv(users_by_unit, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/users_by_unit", last_month, this_year, ".csv"), row.names = FALSE)
write.csv(prds_by_unit, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/prds_by_unit", last_month, this_year, ".csv"), row.names = FALSE)
write.csv(prds_by_unit_reshaped, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/prds_by_unit_reshaped", last_month, this_year, ".csv"), row.names = FALSE)


###################################################################################
###################################################################################
# ##                      Generate counts of units initially onboarded by month
# #First time a user from a particular department/unit logs in SF/MC/Social = first month unit 'onboarded'

# df_fortable <- df_fortable %>% 
#   mutate(SFCreatedDate = lubridate::dmy_hms(CreatedDate.y)) #JM updated to CreatedDate.y

names(df_fortable)

units_by_month<-df_fortable
names(units_by_month)

# change dates from characters to Date-Time
units_by_month$SFCreatedDate <- lubridate::mdy_hms(units_by_month$Created.Date) 

units_max_min <- units_by_month %>%
  group_by(Parent_Organization__c) %>%
  summarize(
    MaxcreatebyUnit = max(SFCreatedDate, na.rm = T),
    MinCreatebyUnit = min(SFCreatedDate, na.rm = T)
  ) %>%
  arrange(Parent_Organization__c)  #it is all the same time, that's a problem
###MC and Social are not included because we don't have the data
# fixing the date format
# switched to summarize not summarise

units_max_min$firstmonth<-month(units_max_min$MinCreatebyUnit)
units_max_min$firstyear<-year(units_max_min$MinCreatebyUnit)

units_max_min<-subset(units_max_min, !is.na(units_max_min$firstmonth))

unit_FirstTrellisMonth<-units_max_min[,-c(2,3)]

write.csv(unit_FirstTrellisMonth, "D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/Usage and Adoption/unit_FirstTrellisMonth_062021.csv")


###################################################################################
###################################################################################
##                      average time between user creation and first login for users created in last 6 months
# user_logins <- sf_query(solq_logins, object_name="LoginHistory", api_type="Bulk 1.0")

# user_logins <- user_logins %>% 
#   mutate(LoginTime = lubridate::ymd_hms(LoginTime))
# essentially a left join
logins<-merge(users, user_logins, by.x = "Id", by.y = "UserId", all = TRUE) #changed from all.x to all

##keep only users created in last 6 months
logins<-subset(logins, logins$CreatedDate>=as.Date("2021-01-01"))
names(logins)
# logins<-logins[, c(3,5,11,12,14)]

logins_firstlogin <- logins %>%
  group_by(Email, CreatedDate, Profile.Name, UserRole.Name) %>%
  summarise(
    MinLoginDate = min(lubridate::ymd_hms(LoginTime), na.rm = T)
  ) %>%
  arrange(Email, CreatedDate, Profile.Name, UserRole.Name) #Min_login is not showing or one with NA

save.image("MonthlyKPI_JM_Dec15.RData")
###stopped here 19 April 2021
# jung mee updates the code 10 December 2021
logins_firstlogin$T2firstlogin<-difftime(as.Date(logins_firstlogin$MinLoginDate), 
                                         as.Date(logins_firstlogin$CreatedDate), units = c("days"))
logins_firstlogin$createmonth<-month(logins_firstlogin$CreatedDate)
logins_firstlogin$createyear<-year(logins_firstlogin$CreatedDate)

logins_firstlogin$CollegeUnit<-sapply(strsplit(logins_firstlogin$UserRole.Name, " "), "[", 1)

avg_t2l_bymonth <- logins_firstlogin %>%
  group_by(createmonth, createyear) %>%
  summarise(
    avg_t2l = mean(T2firstlogin, na.rm = T)
  ) %>%
  arrange(createmonth, createyear)


avg_t2l_bymonth_byprof <- logins_firstlogin %>%
  group_by(Profile.Name,createmonth, createyear) %>%
  summarise(
    avg_t2l = mean(T2firstlogin, na.rm = T)
  ) %>%
  arrange(Profile.Name,createmonth, createyear)

avg_t2l_bymonth_bycollegeunit <- logins_firstlogin %>%
  group_by(CollegeUnit,createmonth, createyear) %>%
  summarise(
    avg_t2l = mean(T2firstlogin, na.rm = T)
  ) %>%
  arrange(CollegeUnit,createmonth, createyear)

write.csv(logins, "U:/WorkSpaces/fkm_data/Program Team KPIs/Raw/logins_raw_jan2021-june2021.csv")

write.csv(avg_t2l_bymonth, "U:/WorkSpaces/fkm_data/Program Team KPIs/Usage and Adoption/avg_t2l_bymonth_jan2021-june2021.csv")
write.csv(avg_t2l_bymonth_byprof, "U:/WorkSpaces/fkm_data/Program Team KPIs/Usage and Adoption/avg_t2l_bymonth_byprof_jan2021-june2021.csv")
write.csv(avg_t2l_bymonth_bycollegeunit, "U:/WorkSpaces/fkm_data/Program Team KPIs/Usage and Adoption/avg_t2l_bymonth_bycollegeunit_jan2021-june2021.csv")



##END



############################################ FEBRUARY WITH NEW METHODOLOGY
##STEP 1: Read in the data from Brett

# setwd("U:/WorkSpaces/fkm_data/Program Team KPIs/")
# Social_Logins <- read_csv("Raw/Users,_last_login,_social_studio-2021-05-01.csv")
# MC_Logins <- read_csv("Raw/Users,_last_login,_marketing_cloud-2021-05-01.csv")

##look at data to see format and variables
glimpse(MC_Logins)
glimpse(Social_Logins)

# perms2prods<-read_xlsx("Raw/Permissionsets_mapped_to_Products.xlsx")
# 
# ############################################
# ##STEP 2: Import data from SF ------ Paused for now while Brett fixes something
# 
# #Oauth
# sf_auth()
# # 
# # sf_auth(login_url="https://login.salesforce.com")
# 
# ###bring in Contact Records
# my_soql_contact <- sprintf("SELECT Id,	Emplid__c, Email, User__c, Primary_Department__r.Name,
#                            NetId__c, hed__Primary_Organization__c, MDM_Primary_Type__c
#                            FROM Contact")
#
contact_records <- sf_query(my_soql_contact, object_name="Contact", api_type="Bulk 1.0")
#
# ###bring in User object fields
# soql_users<-sprintf("select Id, Email, UserType, Name, NetID__c, Profile.Name, ContactId, CreatedDate,
#                     Department, ProfileId, UserRoleId,
#                     UserRole.Name, Title, Username, LastLoginDate
#                     from User
#                     WHERE IsActive=TRUE ")
users_SF <- sf_query(soql_users, object_name="User", api_type="Bulk 1.0")
#
# #####Bring In User Login History
# solq_logins<-sprintf("Select UserId, Browser, LoginTime, LoginType, Platform, Status from LoginHistory
#                      WHERE LoginTime > 2019-10-01T00:00:00.000Z")
user_logins <- sf_query(solq_logins, object_name="LoginHistory", api_type="Bulk 1.0")
#
# ###bring in affiliation Records
# my_soql_aff <- sprintf("SELECT Academic_Department__c, hed__Affiliation_Type__c, hed__Contact__c,
#                         hed__Primary__c, hed__Account__r.Name, Parent_Organization__c
#                            FROM hed__Affiliation__c")
#
affiliations <- sf_query(my_soql_aff, object_name="hed__Affiliation__c", api_type="Bulk 1.0")

#
# ###bring in permissionsets Records
# solq_perms<-sprintf("select AssigneeId, PermissionSet.Name, PermissionSet.Type, PermissionSet.ProfileId,
#                     PermissionSetGroupId from PermissionSetAssignment")
permissionsets <- sf_query(solq_perms, object_name="PermissionSetAssignment", api_type="Bulk 1.0")
#
#
# library(readxl)
# perms2prods <- read_excel("Raw/Permissionsets_mapped_to_Products.xlsx")
# View(perms2prods)
#
users<-read.csv("Raw/MtD Licensed Users Logged In-2021-04-30-17-00-04.csv")

############################################
##STEP 3: Merge files

##restrict to users with last logins this month

str(MC_Logins)

#MC_Logins<-subset(MC_Logins, month(as.Date(MC_Logins$"max(timestamp)", "%m/%d/%Y")) == this_month & year(as.Date(MC_Logins$"max(timestamp)", "%m/%d/%Y"))==this_year)
#Social_Logins<-subset(Social_Logins, month(as.Date(Social_Logins$"max(timestamp)", "%m/%d/%Y")) == this_month & year(as.Date(Social_Logins$"max(timestamp)", "%m/%d/%Y")) == this_year)
MC_Logins<-subset(MC_Logins, month(as.Date(MC_Logins$`Last Login`)) == 2 & year(as.Date(MC_Logins$`Last Login`))==this_year)
Social_Logins<-subset(Social_Logins, month(as.Date(Social_Logins$lastlogindatetime)) == 2 & year(as.Date(Social_Logins$lastlogindatetime)) == this_year)

#users<-subset(user_logins, month(user_logins$LoginTime)== this_month & year(user_logins$LoginTime)==this_year)

users<-subset(user_logins, month(user_logins$LoginTime)==2 & year(user_logins$LoginTime)==this_year)
users<-merge(users_SF,users, by.x = "Id", by.y = "UserId")
users<-users[,-c(7,10,13,16:20)]
users<-unique(users)

#rename var indicating that login was for a particular tool
MC_Logins$Profile.Name<-"MC"
Social_Logins$Profile.Name<-"Social"
#Social_Logins<-subset(Social_Logins, !is.na(Social_Logins$netid))

users %>% group_by(Profile.Name) %>% tally
##restrict User file to those we want
users<-subset(users, users$Profile.Name %in% c("Salesforce Base", "Advising Base", "Service Desk Base", "System Administrator", "Trellis USer"))

##reshape the permissionsets file

# permset_tally<-permissionsets %>% group_by(PermissionSet.Name) %>% tally
# write.csv(permset_tally,"permissionsets.csv")
# permissionsets %>% group_by(PermissionSet.Type) %>% tally

names(users)
names(permissionsets)
names(perms2prods)
#perms2prods<-perms2prods[,-c(2)]

users_perms<-merge(users, permissionsets, by.x = "Id", by.y = "AssigneeId")
users_perms_prods<-merge(users_perms, perms2prods, by.x = "PermissionSet.Name", by.y = "PermissionSet.Name", all.x = TRUE)
names(users_perms_prods)
users_perms_prods<-users_perms_prods[, -c(10,14,15,16)]

#
#merge MC into contacts
#
names(MC_Logins) 
contact_records<-subset(contact_records, !is.na(contact_records$NetID__c))
users$NetID__c[users$NetID__c=="Mlfink3"]<-"mlfink3"

##need to check to make sure the email addresses are @email.arizona.edu, not @arizona.edu before merge - ok
# MC_Logins_Feb_2021$pre<-sapply(strsplit(MC_Logins_Feb_2021$`Email Address`, "@"), "[", 1)
# MC_Logins_Feb_2021$emailright<-sapply(strsplit(MC_Logins_Feb_2021$`Email Address`, "@"), "[", 2)
# MC_Logins_Feb_2021$dummyemail<-paste(MC_Logins_Feb_2021$pre,"email.arizona.edu", sep = "@", collapse = NULL)
# MC_Logins_Feb_2021$`Email Address`[MC_Logins_Feb_2021$emailright=="arizona.edu"]<-MC_Logins_Feb_2021$dummyemail


colnames(MC_Logins)[4]<-"Email"
colnames(MC_Logins)[6]<-"LastLoginDate"
colnames(Social_Logins)[1]<-"NetID__c"
colnames(Social_Logins)[2]<-"LastLoginDate"

# MC_Logins$Email[MC_Logins$Email=="letsonn@arizona.edu"]<-"letsonn@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="abarela1@arizona.edu"]<-"abarela1@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="dumonta@arizona.edu"]<-"dumonta@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="ksouth@arizona.edu"]<-"ksouth@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="lisas@arizona.edu"]<-"lisas@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="mveres@arizona.edu"]<-"mveres@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="mdenham@arizona.edu"]<-"mdenham@email.arizona.edu"
# MC_Logins$Email[MC_Logins$Email=="nprevenas@arizona.edu"]<-"nprevenas@email.arizona.edu"

MC_contacts<-merge(contact_records, MC_Logins, by.x = "Email", by.y = "Email", all = TRUE)
#MC_contacts<-subset(MC_contacts, is.na(MC_contacts$MDM_Primary_Type__c) | MC_contacts$MDM_Primary_Type__c %in% c("former_student","former-member","student","staff", "studentworker", "gradasst", "staff", "dcc", "admit", "affiliate", "faculty"))

##see which email addresses in MC have no contact match (n=11) - go back and change these email addresses in the MC file
pop_density_no_match <- anti_join(MC_Logins, contact_records, 
                                  by = c("Email" = "Email"))

#
#Merge social into contacts plus MC
#
MC_contacts_social<-merge(MC_contacts, Social_Logins, by.x = c("NetID__c"), by.y = c("NetID__c"), all = TRUE)

MC_contacts_social %>% group_by(MDM_Primary_Type__c) %>% tally

#
#Merge users into contacts plus MC plus social
#
length(unique(MC_contacts_social$NetID__c))
names(MC_contacts_social)
#MC_contacts_social <- MC_contacts_social[, c(1:8,10,13,18:21)]

colnames(MC_contacts_social)[13]<-"MC_LastLoginDate"
colnames(MC_contacts_social)[18]<-"MCProfile"
colnames(MC_contacts_social)[19]<-"Social_LastLoginDate"
colnames(MC_contacts_social)[21]<-"SocialProfile"

names(users_perms_prods)
upp_clean <- users_perms_prods[, -c(9,10)]
upp_clean<-unique(upp_clean)
upp_clean<-subset(upp_clean, upp_clean$Profile.Name!="System Administrator")
upp_clean %>% group_by(Profile.Name) %>% tally 
upp_clean<-subset(upp_clean, !is.na(upp_clean$Product))

upp_clean <- rename(upp_clean,c("SFUserId" = "Id", "SFCreatedDate" = "CreatedDate", "SFProduct" = "Product"))

MC_s_c_u<-merge(MC_contacts_social, upp_clean, by.x = c("NetID__c"), by.y = c("NetID__c"), all=TRUE)
names(MC_s_c_u)
colnames(MC_s_c_u)[28]<-"SF_LastLoginDate"

MC_s_c_u <- MC_s_c_u[, -c(6:12,14:17,20,22)]

MC_s_c_u_foruse<-subset(MC_s_c_u, (!is.na(MC_s_c_u$SF_LastLoginDate) | !is.na(MC_s_c_u$Social_LastLoginDate) | !is.na(MC_s_c_u$MC_LastLoginDate)))

names(MC_s_c_u_foruse)
length(unique(MC_s_c_u_foruse$Id[MC_s_c_u_foruse$SocialProfile=="Social"]))
test<-subset(MC_s_c_u_foruse, MC_s_c_u_foruse$SocialProfile=="Social")
#
#Merge in Affiliations
#
names(affiliations)
affiliation1s <- subset(affiliations, affiliations$hed__Primary__c==TRUE)
affiliation1s <- affiliation1s[, -c(4)]
affiliation1s<-distinct(affiliation1s)

df_foruse<-merge(MC_s_c_u_foruse, affiliation1s, by.x = "Id", by.y = "hed__Contact__c", all.x = TRUE)
##take out all Trellis team members from counts
df_foruse<-subset(df_foruse, !(df_foruse$NetID__c %in% c("sananava", "fkmiller", "mariovasquez","karamcintyre", "rsrobrahn", "sarceneaux", "krusie", "gestautus", "cynthiavaldez", "puffvu", "cyrusmadrone")))

#check outcome
df_foruse %>%group_by(df_foruse$SFProduct)%>% tally
length(unique(df_foruse$Email.x[df_foruse$SocialProfile=="Social"]))

#restrict columns to those we need
names(df_foruse)
df_fortable <- df_foruse[, -c(1:4,6,8,12,13,14,15,16,19,20)]
df_fortable<-unique(df_fortable)

#########################################################
#########################################################
#######################################################
##table construction
names(df_fortable)
##total users by product
SFuserscount<-as.data.frame(table(df_fortable$SFProduct))
MCuserscount<-as.data.frame(table(df_fortable$MCProfile))
Socuserscount<-as.data.frame(table(df_fortable$SocialProfile))

user_counts_by_product<-rbind(SFuserscount, MCuserscount, Socuserscount)
colnames(user_counts_by_product)[1]<-"Product/Profile"

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

setwd<-"U:/WorkSpaces/fkm_data/Program Team KPIs/"

write.csv(user_counts_by_product, paste0("U:/WorkSpaces/fkm_data/Program Team KPIs/user_counts_by_product", 5, this_year, ".csv"), row.names = FALSE)
write.csv(user_cts_by_prd_unit, paste0("U:/WorkSpaces/fkm_data/Program Team KPIs/user_cts_by_prd_unit", 5, this_year, ".csv"), row.names = FALSE)
write.csv(users_by_unit, paste0("U:/WorkSpaces/fkm_data/Program Team KPIs/users_by_unit", 5, this_year, ".csv"), row.names = FALSE)
write.csv(prds_by_unit, paste0("U:/WorkSpaces/fkm_data/Program Team KPIs/prds_by_unit", 5, this_year, ".csv"), row.names = FALSE)
write.csv(prds_by_unit_reshaped, paste0("U:/WorkSpaces/fkm_data/Program Team KPIs/prds_by_unit_reshaped", 5, this_year, ".csv"), row.names = FALSE)



# ##for one file per table - this won't work from the RStudio server interface - will work from desktop
# # df.list<-list(epr_email_opens_byweek,epr_ack,epr_FT1, epr_FT2)
# # name.vec<-c('epr_email_opens_byweek','epr_ack','epr_FT1', 'epr_FT2')
# # 
# library(writexl)
# # 
# # sapply(1:length(df.list),
# #        function(i) write_xlsx(df.list[[i]], paste0(strEPR, name.vec[i],  '.xlsx'), col_names = TRUE, format_headers = TRUE))
# 
# ##for on sheet per table
# sheets <- list("epr_email_opens_byweek" = epr_email_opens_byweek, "epr_ack" = epr_ack, "email_ssri" = email_ssri,  "email_instructor" = email_instructor) #assume sheet1 and sheet2 are data frames
# write_xlsx(sheets, "C:/Users/fkmiller/Box/Trellis/Active Products/Trellis Early Progress Reports/Reporting/EPR Use Tables/UserBehavior.xlsx")
# 
# sheets <- list("epr_FT1" = epr_FT1, "epr_FT2" = epr_FT2, "epr_SFT1" = epr_SFT1, "epr_SFT2" = epr_SFT2) #assume sheet1 and sheet2 are data frames
# write_xlsx(sheets, "U:/WorkSpaces/fkm_data/FeedbackTypes.xlsx")
# 
# 
# users2 <- list("users2" = users) #assume sheet1 and sheet2 are data frames
# write_xlsx(users2, "C:/Users/fkmiller/Box/~FKM/Reporting/Affiliations plus users/20200316_users.xlsx")
