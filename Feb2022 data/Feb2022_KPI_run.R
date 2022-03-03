# Jung Mee Park
# attempt to reproduce Francis's tables
# 2021-01-20
# updated 2022-03-03

####Load Libraries####
library('tidyverse')
library('dplyr')
library(dbplyr)
library('readxl')
library('salesforcer')
# library('RForcecom') # not available in this version of R on 08 Dec 2021
library('lubridate')
library(reshape2)
library(seplyr)
# library(pointblank)

####check working directory####
getwd() #figure out the working directory
setwd("D:/Users/jmpark/Box/Trellis/Program Team/Trellis Metrics Reports/Trellis KPIs/Raw data")

rm(list=ls(all=TRUE)) 
options(digits=3)

today <- Sys.Date()

####capture current month####
this_month <- month(today)
last_month<-month(today() - months(1))
this_year <- year(today)
# last_year <-year(today() - years(1))

format(today, format="%B %d %Y")

####Import data from SF ####
#------ Paused for now while Brett fixes something
#Oauth
sf_auth()
# 
# sf_auth(login_url="https://login.salesforce.com")

###bring in Contact Records
my_soql_contact <- sprintf("SELECT Id,	Emplid__c, Email, User__c, Primary_Department__r.Name, 
                           NetId__c, hed__Primary_Organization__c, MDM_Primary_Type__c
                           FROM Contact")

contact_records <- sf_query(my_soql_contact, object_name="Contact", api_type="Bulk 1.0")

####bring in User object fields####
soql_users<-sprintf("select Id, Email, UserType, Name, NetID__c, Profile.Name, ContactId, CreatedDate,
                    Department, ProfileId, UserRoleId, 
                    UserRole.Name, Title, Username, LastLoginDate 
                    from User
                    WHERE IsActive=TRUE ")
users_SF <- sf_query(soql_users, object_name="User", api_type="Bulk 1.0")

#####Bring In User Login History####
solq_logins<-sprintf("Select UserId, Browser, LoginTime, LoginType, Platform, Status from LoginHistory
                     WHERE LoginTime > 2019-10-01T00:00:00.000Z")
user_logins <- sf_query(solq_logins, object_name="LoginHistory", api_type="Bulk 1.0")

###bring in affiliation Records####
my_soql_aff <- sprintf("SELECT Academic_Department__c, hed__Affiliation_Type__c, hed__Contact__c, 
                        hed__Primary__c, hed__Account__r.Name, Parent_Organization__c
                           FROM hed__Affiliation__c")

affiliations <- sf_query(my_soql_aff, object_name="hed__Affiliation__c", api_type="Bulk 1.0")


###bring in permissionsets Records####
solq_perms<-sprintf("select AssigneeId, PermissionSet.Name, PermissionSet.Type, PermissionSet.ProfileId, 
                    PermissionSetGroupId from PermissionSetAssignment")
permissionsets <- sf_query(solq_perms, object_name="PermissionSetAssignment", api_type="Bulk 1.0")

#save to csv
# write.csv(contact_records, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/contact_records", this_month, this_year, ".csv"), row.names = FALSE)
# write.csv(user_logins, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/user_logins", this_month, this_year, ".csv"), row.names = FALSE)
# write.csv(users_SF, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/users_SF", this_month, this_year, ".csv"), row.names = FALSE)
# write.csv(affiliations, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/affiliations", this_month, this_year, ".csv"), row.names = FALSE)
# write.csv(permissionsets, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/permissionsets", this_month, this_year, ".csv"), row.names = FALSE)

#### perms2prods####
perms2prods<-read_xlsx("20220224_Permissionsets_mapped_to_Products.xlsx") # from 2/24/22

# perms2prods <- read.csv("Raw data/user_perms_prods.csv") #for other months
perms2prod1 <- perms2prods %>% 
  group_by(Product) %>% 
  dplyr::mutate(occurence=sum(!(is.na(n))), sum = sum(n, na.rm = T))

#### function to write copy files into csv####
write_named_csv <- function(x) 
  write_csv(x, file = paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/Feb 2022 data files/",
                             deparse(substitute(x)), last_month, this_year,".csv"))
#### write named csv on March 1 ####
write_named_csv(affiliations)
write_named_csv(contact_records)
write_named_csv(permissionsets)
write_named_csv(user_logins)
write_named_csv(users_SF)

#### read in files from March 1, 2022 ####
directory <- paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/")
directory
affiliations <- read.csv(paste0(directory,"Feb 2022 data files/affiliations", last_month, this_year,".csv"))
contact_records <- read.csv(paste0(directory,"Feb 2022 data files/contact_records",last_month, this_year,".csv"))
permissionsets <- read.csv(paste0(directory,"Feb 2022 data files/permissionsets",last_month, this_year,".csv"))
user_logins <- read.csv(paste0(directory,"Feb 2022 data files/user_logins",last_month, this_year,".csv"))
users_SF <- read.csv(paste0(directory,"Feb 2022 data files/users_SF",last_month, this_year,".csv"))

user_logins$LoginTime <- as.Date(user_logins$LoginTime)
# user_logins_Jan <- user_logins[format.Date(user_logins$LoginTime, "%m")=="01" &
#                     format.Date(user_logins$LoginTime, "%Y")=="2022" 
#                     & !is.na(user_logins$LoginTime),]
#                     
############################################
####STEP 2: Read in the data from Box Folders####
# splunk reports
splunk_reports <- list.files("./2022 Splunk Reports")  # Identify file names
splunk_reports

#Users,_last_login,_social_studio-2022-03-01.csv
# name <- paste0(this_year, " Splunk Reports/", month.name[last_month],
#                " ",this_year,"/Users,_last_login,_social_studio-",
#                this_year,"-0",this_month,"-01.csv")
# name
Social_Logins <- read_csv(paste0(this_year, " Splunk Reports/", month.name[last_month],
                                 " ",this_year,"/Users,_last_login,_social_studio-",
                                 this_year,"-0",this_month,"-01.csv"))
# Users,_last_login,_marketing_cloud-2022-03-01
MC_Logins <- read_csv(paste0(this_year, " Splunk Reports/", month.name[last_month],
                             " ",this_year,"/Users,_last_login,_marketing_cloud-",
                             this_year,"-0",this_month,"-01.csv"))

####STEP 3: Merge files####
##restrict to users with last logins this month
str(MC_Logins)

# clean up MC date
MC_Logins$MC_dates <- as.Date(parse_date_time(MC_Logins$"max(timestamp)", c('mdy')))
MC_Logins <- MC_Logins[, -c(2)] # to remove "max(timestamp"
# MC_Logins <- MC_Logins[ , !names(MC_Logins) %in% c("max(timestamp)")] 

#### subset social date####
Social_Logins$social_dates <- as.Date(parse_date_time(Social_Logins$"max(timestamp)", c('mdy')))
Social_Logins <- Social_Logins[, -c(2)] # to remove "max(timestamp)"

# Social_Logins_Jan <- Social_Logins[format.Date(Social_Logins$social_dates, "%m")=="1" &
#                     format.Date(Social_Logins$social_dates, "%Y")=="2022" &
#                     !is.na(Social_Logins$social_dates),]
# Social_Logins <- Social_Logins_Dec[, -c(2)]

#rename var indicating that login was for a particular tool
MC_Logins$Profile.Name<-"MC"
Social_Logins$Profile.Name<-"Social"

colnames(MC_Logins)[1]<-"NetID__c"
# colnames(MC_Logins)[2]<-"LastLoginDate"
colnames(Social_Logins)[1]<-"NetID__c"
# colnames(Social_Logins)[2]<-"LastLoginDate"

# read in users from last month
users <- read.csv("./2022 Splunk Reports/February 2022/MtD Licensed Users Logged In-2022-02-25-17-00-06.csv")
# users_SF <- subset(users_SF, month(users_SF$LastLoginDate)== 02 & year(users_SF$LastLoginDate)== 2022)
users<-merge(users_SF,users, by.x = "Email", by.y = "Username", all.y = TRUE) 


#### from original code####
# users <- subset(user_logins, month(user_logins$LoginTime)== 01 & year(user_logins$LoginTime)== 2022)
# 
# users<-merge(users_SF,users, by.x = "Id", by.y = "UserId", all = TRUE)

# parse SF users to DEC
# SF_user_dec <- users_SF[format.Date(users_SF$LastLoginDate, "%m")=="12" &
#                           format.Date(users_SF$LastLoginDate, "%Y")=="2021" &
#                           !is.na(users_SF$LastLoginDate),]
# 
# users<-merge(SF_user_dec,users, by.x = "Email", by.y = "Username")

users <- unique(users)

# users %>% group_by(Profile.Name) %>% tally() # sometimes appears as Profile
users %>% count(Profile.Name)
# users %>% count(Profile)

# subset users 
users <- subset(users, users$Profile.Name %in% c("Salesforce Base", 
                                                 "Advising Base", "Service Desk Base")) #down to 635
users$NetID__c[users$NetID__c=="Mlfink3"]<-"mlfink3"

# save to csv
write_named_csv(users)

##reshape the permissionsets
names(users)
names(permissionsets)
names(perms2prods)

users_perms<-merge(users, permissionsets, by.x = "Id", by.y = "AssigneeId") # n=2989
# users_perms<-merge(users, permissionsets, by.x = "User.ID", by.y = "AssigneeId") # n=3258

users_perms_prods<-merge(users_perms, perms2prods, by.x = "PermissionSet.Name", 
                         by.y = "PermissionSet.Name", all.x = TRUE) # n = 2989

#save to csv
write_named_csv(users_perms)
write_named_csv(users_perms_prods)

# subset contact records
contact_records <- subset(contact_records, !is.na(contact_records$NetID__c)) # 337326

#save to csv
write_named_csv(contact_records)

# merge MC contacts
MC_contacts <- merge(contact_records, MC_Logins, by.x = "NetID__c", 
                     by.y = "NetID__c", all = TRUE)

Å# check for no matches
pop_density_no_match <- anti_join(MC_Logins, contact_records, 
                                  by = c("NetID__c" = "NetID__c")) # zero is good

#Merge social into contacts plus MC
#
MC_contacts_social <- merge(MC_contacts, Social_Logins, by.x = c("NetID__c"), 
                            by.y = c("NetID__c"), all = TRUE)
# making sure that across the system, the ID's aren't the same. 
# MC_contacts_social<-merge(MC_contacts, Social_Logins_Dec, by.x = c("NetID__c"),
#                           by.y = c("user"), all = TRUE)

MC_contacts_social %>% count(MDM_Primary_Type__c)


length(unique(MC_contacts_social$NetID__c))
names(MC_contacts_social)

colnames(MC_contacts_social)[9]<-"MC_LastLoginDate" #change from MC_dates 
colnames(MC_contacts_social)[10]<-"MCProfile" # change from Profile.Name.x 
colnames(MC_contacts_social)[11]<-"Social_LastLoginDate" #change from Social_dates 
colnames(MC_contacts_social)[12]<-"SocialProfile" # change from Profile.Name.y

## count the MC and Social
MC_contacts_social %>% 
  distinct(Email,MCProfile) %>%
  group_by(MCProfile) %>% 
  count()%>% 
  ungroup()
MC_contacts_social %>% 
  distinct(Email, SocialProfile) %>%
  group_by(SocialProfile) %>% 
  count()%>% 
  ungroup()

# write csv
write_named_csv(MC_contacts_social)

names(users_perms_prods)
# upp_clean <- users_perms_prods[, c(1,2,4,7,8,11:13)] # from FKM
# upp_clean <- users_perms_prods[, -c(1,3,4,6,7,12:15)]
# keep_variables <- c("PermissionSet.Name", "Department", "Username", "NetID__c", "User.ID", "Last.Login",  "Created.Date", "Profile", "Product")

#### upp_clean creation ####
# this is only for SF products
upp_clean <- users_perms_prods[, colnames(users_perms_prods) %in% 
                                 c("Id",
                                   # "Department", 
                                   "Username", "NetID__c", "LastLoginDate", 
                                   # "Last.Login",
                                   "LoginTime", "CreatedDate", "Profile.Name", 
                                   "Product")]
upp_clean<-subset(upp_clean, !is.na(upp_clean$Product))
upp_clean<-subset(upp_clean, upp_clean$Profile.Name!="System Administrator")

upp_clean <- rename(upp_clean, c("SFUserId" = "Id", "SFCreatedDate" = "CreatedDate", 
                                 "SFProduct" = "Product"))
# upp_clean<-subset(upp_clean, !is.na(upp_clean$Product))
upp_clean<-unique(upp_clean)
# upp_clean<-subset(upp_clean, upp_clean$Profile!="System Administrator") #36940
upp_clean %>% count(Profile.Name)
# names(upp_clean)
####MC_s_c_u creation####
MC_s_c_u <- merge(MC_contacts_social, upp_clean, by.x = c("NetID__c"), 
                  by.y = c("NetID__c"), all=TRUE)

names(MC_s_c_u) # marketing social contacts users

# what is this renaming?
colnames(MC_s_c_u)[15]<-"SF_LastLoginDate" # change from "LastLoginDate" 
MC_s_c_u$SF_LastLoginDate <- as.Date(MC_s_c_u$SF_LastLoginDate)

# clean up last login date 
names(MC_s_c_u)
# MC_s_c_u <- MC_s_c_u[, -c(4,5, 7, 16)]
MC_s_c_u <- MC_s_c_u[ , !names(MC_s_c_u) %in% c("Emplid__c", "hed__Primary_Organization__c",
                                                "User__c", "MDM_Primary_Type__c")] #remove these variables


# this removes too many things
# MC_s_c_u <- subset(MC_s_c_u, month(MC_s_c_u$SF_LastLoginDate)== 02 &
#                      year(MC_s_c_u$SF_LastLoginDate)== 2022) # n=3530

##
MC_s_c_u %>% 
  distinct(Email,MCProfile) %>%
  group_by(MCProfile) %>% 
  count()%>% 
  ungroup() # 153, lost none
MC_s_c_u %>% 
  distinct(Email, SocialProfile) %>%
  group_by(SocialProfile) %>% 
  count()%>% 
  ungroup()

MC_s_c_u %>% 
  distinct(Email, SFProduct, SF_LastLoginDate) %>%
  group_by(SFProduct) %>% 
  dplyr::filter(SF_LastLoginDate >= "2022-02-01" & SF_LastLoginDate <="2022-02-28") %>%
  count()%>% 
  ungroup()

MC_s_c_u_events <- MC_s_c_u %>% 
  select(Email, SFProduct, SF_LastLoginDate) %>%
  # dplyr::filter(between(SF_LastLoginDate, as.Date("2022-01-31"), as.Date("2022-03-01"))) %>% 
  group_by(SFProduct) %>% 
  dplyr::filter(SF_LastLoginDate >= "2022-02-01" & SF_LastLoginDate <="2022-02-28") %>%
  dplyr::filter(SFProduct == "Events") 
# %>% 
#   count()%>% 
#   ungroup()
# find missing MC

#### get rid of NA dates####
# original code
# MC_s_c_u_foruse <- subset(MC_s_c_u, (!is.na(MC_s_c_u$SF_LastLoginDate) | !is.na(MC_s_c_u$Social_LastLoginDate) | !is.na(MC_s_c_u$MC_LastLoginDate)))
MC_s_c_u_foruse <- MC_s_c_u %>%
  mutate(num_na_date = is.na(MC_LastLoginDate) + is.na(Social_LastLoginDate) + is.na(SF_LastLoginDate)) %>%
  filter(num_na_date < 3) %>%
  dplyr::distinct()

# MC_s_c_u_foruse <- MC_s_c_u %>% 
#   mutate(num_na_date = is.na(MC_LastLoginDate) + is.na(Social_LastLoginDate) + is.na(SF_LastLoginDate)) %>% 
#   filter(num_na_date < 3) %>% 
#   dplyr::distinct()
names(MC_s_c_u_foruse)
# length(unique(MC_s_c_u_foruse$Id[MC_s_c_u_foruse$MCProfile=="MC"])) # 154
# length(unique(MC_s_c_u_foruse$Id[MC_s_c_u_foruse$SocialProfile=="Social"])) # 83

MC_s_c_u_foruse %>% 
  select(Id, SFProduct, SF_LastLoginDate) %>%
  dplyr::filter(SF_LastLoginDate >= "2022-02-01" & SF_LastLoginDate <="2022-02-28") %>%
  # dplyr::filter(between(SF_LastLoginDate, as.Date("2022-01-31"), as.Date("2022-03-01"))) %>% 
  group_by(SFProduct) %>% 
  count()%>% 
  ungroup()

test<-subset(MC_s_c_u_foruse, MC_s_c_u_foruse$SocialProfile=="Social")

# write to csv
write_named_csv(MC_s_c_u_foruse)

#Merge in Affiliations
names(affiliations)
affiliation1s <- subset(affiliations, affiliations$hed__Primary__c==TRUE)
affiliation1s <- affiliation1s[, -c(4)] #remove "hed__Primary__c"
affiliation1s <- distinct(affiliation1s)

# write to csv
write_named_csv(affiliation1s)

df_foruse <- merge(MC_s_c_u_foruse, affiliation1s, by.x = "Id", by.y = "hed__Contact__c", all.x = TRUE) 
# hed_ higher ed prefix from SF. THat is not a native salesforce thing. A managed package or something else like blackthorn

##take out all Trellis team members from counts
# df_foruse <- df_foruse[ , !names(df_foruse) %in% c("NetID__c", "Id", "SFUserId", "Academic_Department__c")] #drop col NetID__c.y
df_foruse <- subset(df_foruse, !(df_foruse$NetID__c %in% c("sananava", "fkmiller", "jmpark", "mariovasquez","karamcintyre", "rsrobrahn", "sarceneaux", "krusie", "gestautus", "cynthiavaldez", "puffvu", "cyrusmadrone")))

df_foruse %>% 
  distinct(Id, MCProfile) %>%
  group_by(MCProfile) %>% 
  count()%>% 
  ungroup()

df_foruse %>% 
  distinct(Id, SocialProfile) %>%
  group_by(SocialProfile) %>% 
  count()%>% 
  ungroup()
df_foruse %>% 
  distinct(Id, SFProduct, SF_LastLoginDate) %>%
  dplyr::filter(SF_LastLoginDate >= "2022-02-01" & SF_LastLoginDate <="2022-02-28") %>%
  # dplyr::filter(between(SF_LastLoginDate, as.Date("2022-01-31"), as.Date("2022-03-01"))) %>% 
  group_by(SFProduct) %>% 
  count()%>% 
  ungroup()

#check outcome
# df_foruse %>% count(df_foruse$MCProfile)
# length(unique(df_foruse$Email[df_foruse$MCProfile=="MC"])) # 151

# write to csv
write_named_csv(df_foruse)

# creating fortable
names(df_foruse)
# df_fortable <- df_foruse[, -c(1,2,4,7,9,10,11,12,13,15,16)] # drop ID, NetID_c, Primary_Dept, 
df_fortable <- df_foruse %>% 
  dplyr::select(-c(Id, SFUserId, hed__Affiliation_Type__c, Academic_Department__c)) %>% 
  # select(Id, Email, NetID__c, MCProfile, MC_LastLoginDate, SocialProfile, Social_LastLoginDate,  
  #        SFProduct, SF_LastLoginDate, SFCreatedDate, Primary_Department__r.Name, 
  #        Profile.Name, Parent_Organization__c, hed__Affiliation_Type__c) %>%
  # dplyr::filter(SF_LastLoginDate >= "2022-02-01" & SF_LastLoginDate <="2022-02-28") %>% 
  distinct()

names(df_fortable)
write_named_csv(df_fortable)

### checking for distinct 
# but lost way more with the dropped variables
df_fortable %>% 
  distinct(Email, SFProduct, SF_LastLoginDate) %>%
  dplyr::filter(SF_LastLoginDate >= "2022-02-01" & SF_LastLoginDate <="2022-02-28") %>% 
  group_by(SFProduct) %>% 
  count()%>% 
  ungroup()

##### 
df_fortable %>% 
  distinct(Email,MCProfile) %>%
  group_by(MCProfile) %>% 
  count()%>% 
  ungroup() # dropped 3 

df_fortable %>% 
  distinct(Email, SocialProfile) %>%
  group_by(SocialProfile) %>% 
  count()%>% 
  ungroup()
df_foruse %>% 
  distinct(Email, SFProduct) %>%
  group_by(SFProduct) %>% 
  count()%>% 
  ungroup()
## drop none logins
df_foruse_non_na <- df_foruse %>%
  mutate(num_na_date = is.na(MC_LastLoginDate) + is.na(Social_LastLoginDate) + is.na(SF_LastLoginDate)) %>%
  filter(num_na_date < 3) %>%
  dplyr::distinct()


df_foruse %>% 
  distinct(Email, MCProfile) %>%
  group_by(MCProfile) %>% 
  count()%>% 
  ungroup() #150

df_foruse_non_na %>% 
  distinct(Email, MCProfile) %>%
  group_by(MCProfile) %>% 
  count()%>% 
  ungroup() #150
# ### create a new dataset with month floor ###
# df_month2 <- df_foruse_non_na %>%
#   mutate(MCLoginMonth = floor_date(MC_LastLoginDate, "month")) %>% 
#   mutate(SFLoginMonth = floor_date(as_date(SF_LastLoginDate), "month")) %>% 
#   mutate(SocialLoginMonth = floor_date(as_date(Social_LastLoginDate), "month")) 
# 
# df_month2 <- df_month2 %>% 
#   select(-MC_LastLoginDate, -SF_LastLoginDate, -Social_LastLoginDate, -Last.Login) %>% 
#   distinct() 
# 
# # write csv
# write_named_csv(df_month2)
# 
# # distinct for df_month2, close-ish to Francis
# df_month2 %>% 
#   distinct(Email,SFProduct) %>%
#   group_by(SFProduct) %>% 
#   count() %>% 
#   ungroup()
# 
# df_month2 %>% 
#   distinct(Email,MCProfile, SocialProfile) %>%
#   group_by(MCProfile, SocialProfile) %>% 
#   count()%>% 
#   ungroup()

# removing Student & Acad Technologies
df_fortable2 <- df_month2[df_month2$hed__Account__r.Name != "Student & Acad Technologies", ]

# # distinct for df_fortable2
# df_month2 %>% 
#   distinct(Email, SocialProfile) %>%
#   group_by(SocialProfile) %>% 
#   count()%>% 
#   ungroup()
# 
# df_fortable2 %>% 
#   distinct(Email,MCProfile, SocialProfile) %>%
#   group_by(MCProfile, SocialProfile) %>% 
#   count()%>% 
#   ungroup()
# 
# #### new data frame for nonNA with products ####
# # dropNA if MC, Social, or SF Product is missing
# df_fortable_non_na <- df_fortable2 %>% 
#   mutate(num_na = is.na(MCProfile) + is.na(SocialProfile) + is.na(SFProduct)) %>% 
#   filter(num_na < 3)
# 
# write_named_csv(df_fortable_non_na)
# 
# # distinct for df_fortable_non_na
# df_fortable_non_na %>% 
#   distinct(Email,SFProduct) %>%
#   group_by(SFProduct) %>% 
#   count()%>% 
#   ungroup()
# 
# df_fortable_non_na %>% 
#   distinct(Email,MCProfile) %>%
#   group_by(MCProfile) %>% 
#   count()%>% 
#   ungroup()
# df_fortable_non_na %>% 
#   distinct(Email, SocialProfile) %>%
#   group_by(SocialProfile) %>% 
#   count()%>% 
#   ungroup()

# # remove non-specific month social dates
# Jan22_logins <- df_foruse_non_na[format.Date(df_foruse_non_na$Social_LastLoginDate, "%m")=="1" &
#                                      format.Date(df_foruse_non_na$Social_LastLoginDate, "%Y")=="2022" &
#                                      !is.na(df_foruse_non_na$Social_LastLoginDate),]
# # write to csv
# library(seplyr)
# df_foruse_non_na <- df_foruse_non_na %>% 
#   deselect(., c("Id", "SFUserId", "Academic_Department__c"))
# df_foruse_non_na <- unique(df_foruse_non_na)


# #restrict columns to those we need
# names(df_foruse)
# 
# #### using distinct for tables ####
# # using distinct to count distinct SFProduct
# df_foruse_non_na %>% 
#   distinct(Email,SFProduct) %>%
#   group_by(SFProduct) %>% 
#   count()%>% 
#   ungroup()
# 
# #
# # using distinct to count distinct dept
# df_foruse_non_na %>% 
#   distinct(Department, SFProduct) %>%
#   group_by(Department, SFProduct) %>% 
#   count()%>% 
#   ungroup()

# # 
# # using distinct to count distinct SFProduct
# df_foruse_non_na %>% 
#   distinct(Email, MCProfile, SocialProfile, SFProduct) %>%
#   group_by(SFProduct) %>% 
#   ungroup()

# more on unique
map(df_fortable, unique)

df_fortable %>% 
  select(where(is.character)) %>% 
  map(~unique(.x) %>% length())

df_fortable %>% 
  count(Profile.Name)

df_fortable_non_na %>% 
  count(Profile.Name)


# df_fortable <- df_foruse[, -c(1,2,4,6,8,10,11,12,13,15,16)] #from FKM
# df_fortable <- df_foruse[, -c(1,4,6,8,13,14,15,16,21,22)] # what jung mee wants to do.
# df_fortable <- df_foruse[, c(2,4,5,6, 7,8,9,13,14,17,18)]
# df_fortable <- df_foruse[, c(2,5,7,9,13,14,17,18)]

#run without dates
df_fortable <- df_foruse %>% select(Email, Primary_Department__r.Name,	MCProfile,
                                    SocialProfile,SFProduct,Parent_Organization__c,hed__Account__r.Name)
df_fortable<-unique(df_fortable) # 1115

# #run with dates
# df_fortable <- df_foruse_non_na %>% select(Email, Primary_Department__r.Name,	MCProfile, MC_LastLoginDate,
#                                     SocialProfile,Social_LastLoginDate, SFProduct, SF_LastLoginDate,
# Parent_Organization__c,hed__Account__r.Name, num_na)
# df_month <- df_fortable %>%
#   mutate(MCLoginMonth = floor_date(MC_LastLoginDate, "month")) %>% 
#   mutate(SFLoginMonth = floor_date(as_date(SF_LastLoginDate), "month")) %>% 
#   mutate(SocialLoginMonth = floor_date(as_date(Social_LastLoginDate), "month")) 
# 
# df_month <- df_month %>% 
#   select(-MC_LastLoginDate, -SF_LastLoginDate, -Social_LastLoginDate)
# 
# df_month<-unique(df_month) # 1115

# df_foruse_non_na
# df_fortable1 <- df_foruse_non_na[, c(2,5,7,9,13,14,17,18)]
# df_fortable1<-unique(df_fortable1) # 1145

# remove duplicates 
# df_fortable2 <- df_fortable[!duplicated(df_fortable$Last.Login), ]
# df_fortable2<-unique(df_fortable2)
# df_fortable2 <- df_fortable %>% 
#   group_by(SFProduct) %>% 
#   dplyr::mutate(count=sum(!(is.na(n))), sum = sum(n, na.rm = T))

# # write to csv
# write.csv(df_fortable, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/df_fortable", last_month, this_year, ".csv"), row.names = FALSE)
# write.csv(df_fortable2, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/df_fortable_two", last_month, this_year, ".csv"), row.names = FALSE)

# for df_month
SFuserscount<-as.data.frame(table(df_fortable2$SFProduct))
# SFuserscount<-as.data.frame(table(df_fortable$Product))
MCuserscount<-as.data.frame(table(df_fortable2$MCProfile))
# MCuserscount<-as.data.frame(table(df_fortable$Profile))
Socuserscount<-as.data.frame(table(df_fortable2$SocialProfile))

user_counts_by_product<-rbind(SFuserscount, MCuserscount, Socuserscount) 

# # older code for df_fortable
# names(df_fortable)
# # ##total users by product
# SFuserscount<-as.data.frame(table(df_fortable2$SFProduct))
# # SFuserscount<-as.data.frame(table(df_fortable$Product))
# MCuserscount<-as.data.frame(table(df_fortable2$MCProfile))
# # MCuserscount<-as.data.frame(table(df_fortable$Profile))
# Socuserscount<-as.data.frame(table(df_fortable2$SocialProfile))
# 
# user_counts_by_product<-rbind(SFuserscount, MCuserscount, Socuserscount) 
# colnames(user_counts_by_product)[1]<-"Product/Profile"

##total users by product by unit
sftest<-df_fortable %>% count(SFProduct, Parent_Organization__c)

SFUC_unit<-as.data.frame(table(df_fortable2$SFProduct, by=df_fortable2$Parent_Organization__c))
SFUC_unit<-subset(SFUC_unit, SFUC_unit$Freq!=0)

MCUC_unit<-as.data.frame(table(df_fortable2$MCProfile, by=df_fortable2$Parent_Organization__c))
MCUC_unit<-subset(MCUC_unit, MCUC_unit$Freq!=0)

SocUC_unit<-as.data.frame(table(df_fortable2$SocialProfile, by=df_fortable2$Parent_Organization__c))
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
prds_by_unit_reshaped<-reshape(for_reshape, v.names="counter", timevar="Var1", 
                               idvar="by", direction="wide")

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

# write.csv(user_counts_by_product, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/user_counts_by_product", last_month, last_year, ".csv"), row.names = FALSE)
# write.csv(user_cts_by_prd_unit, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/user_cts_by_prd_unit", last_month, last_year, ".csv"), row.names = FALSE)
# write.csv(users_by_unit, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/users_by_unit", last_month, last_year, ".csv"), row.names = FALSE)
# write.csv(prds_by_unit, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/prds_by_unit", last_month, last_year, ".csv"), row.names = FALSE)
# write.csv(prds_by_unit_reshaped, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/prds_by_unit_reshaped", last_month, last_year, ".csv"), row.names = FALSE)

#### merge df_fortable2 to df_foruse
