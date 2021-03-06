# no login user counts
# Jung Mee Park
# jmpark@arizona.edu
# 2022-03-28
# updated 2022-06-30 for all_marketing_cloud

####Load Libraries####
library('tidyverse')
library('dplyr')
library(dbplyr)
library('salesforcer')
library('lubridate')
library(reshape2)
library(readxl)
library(Rserve)


####check working directory####
getwd() #figure out the working directory
setwd("D:/Users/jmpark/Box/Trellis/Program Team/Trellis Metrics Reports/Trellis KPIs/Raw data")
# setwd("~/monthly_report_SF_trellis_users")

rm(list=ls(all=TRUE)) 
options(digits=3)

####capture current month####
today <- Sys.Date()
this_month <- month(today)
last_month<-month(today() - months(1))
this_year <- year(today)
# last_year <-year(today() - years(1))

format(today, format="%B %d %Y")
#### function to write copy files into csv####
write_named_csv <- function(x) 
  write_csv(x, file = paste0(
    # "D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/Apr 2022 data files/",
    # "D:/Users/jmpark/Box/My Box Notes/June 2022 data/",
    deparse(substitute(x)),"_", last_month, "_",this_year,".csv"))

####Import data from SF####
sf_auth()
###bring in Contact Records
my_soql_contact <- sprintf("SELECT Id,	Emplid__c, Email, User__c, Primary_Department__r.Name, 
                           NetId__c, hed__Primary_Organization__c, MDM_Primary_Type__c
                           FROM Contact")

contact_records <- sf_query(my_soql_contact, object_name="Contact", api_type="Bulk 1.0")

###bring in User object fields
soql_users<-sprintf("select Id, Email, UserType, Name, NetID__c, Profile.Name, ContactId, CreatedDate,
                    Department, ProfileId, UserRoleId, 
                    UserRole.Name, Title, Username 
                    from User
                    WHERE IsActive=TRUE ")
users_SF <- sf_query(soql_users, object_name="User", api_type="Bulk 1.0")

###Bring In User Login History
# solq_logins<-sprintf("Select UserId, Browser, LoginType, Platform, Status from LoginHistory")
# user_logins <- sf_query(solq_logins, object_name="LoginHistory", api_type="Bulk 1.0")

###bring in affiliation Records
my_soql_aff <- sprintf("SELECT Academic_Department__c, hed__Affiliation_Type__c, hed__Contact__c, 
                        hed__Primary__c, hed__Account__r.Name, Parent_Organization__c
                           FROM hed__Affiliation__c")

affiliations <- sf_query(my_soql_aff, object_name="hed__Affiliation__c", api_type="Bulk 1.0")

# solq_logins<-sprintf("Select UserId, Browser, LoginTime, LoginType, Platform, Status from LoginHistory
#                      WHERE LoginTime > 2022-01-01T00:00:00.000Z")
# user_logins <- sf_query(solq_logins, object_name="LoginHistory", api_type="Bulk 1.0")


###bring in permissionsets Records
solq_perms<-sprintf("select AssigneeId, PermissionSet.Name, PermissionSet.Type, PermissionSet.ProfileId, 
                    PermissionSetGroupId from PermissionSetAssignment")
permissionsets <- sf_query(solq_perms, object_name="PermissionSetAssignment", api_type="Bulk 1.0")

#### perms2prods ####
perms2prods <- read_excel("20220224_Permissionsets_mapped_to_Products.xlsx")

#### splunk reports ####
# splunk_reports <- list.files("./2022 Splunk Reports")  # Identify file names
# splunk_reports
# # 
# Social_Logins <- read_csv(paste0(this_year, " Splunk Reports/", month.name[last_month],
#                                  " ",this_year,"/Users,_last_login,_social_studio-",
#                                  this_year,"-0",this_month,"-01.csv"))
# # Social_Logins <- read_csv("social_studio_logins,_last_12_months-2022-05-01.csv")
# 
# # Users,_last_login,_marketing_cloud-2022-03-01
# MC_Logins <- read_csv(paste0(this_year, " Splunk Reports/", month.name[last_month],
#                              " ",this_year,"/Users,_last_login,_marketing_cloud-",
#                              this_year,"-0",this_month,"-01.csv"))
# # MC_Logins <- read.csv("Users,_last_login,_marketing_cloud-2022-05-01.csv")
# 
# # # create a netid column
# # library(stringr)
# # library(stringi)
# # 
# # MC_Logins$NetID__c <- stri_match_first_regex(MC_Logins$Email, "(.*?)\\@")[,2]

# SF Query as of June 1
write_named_csv(affiliations)
write_named_csv(contact_records)
write_named_csv(permissionsets)
write_named_csv(users_SF)

#### Marketing Cloud data ####
MC_Logins <- read.csv("all_marketing_cloud.csv")

#rename var indicating that login was for a particular tool
MC_Logins$Profile.Name<-"MC"
# Social_Logins$Profile.Name<-"Social"

# colnames(MC_Logins)[1]<-"NetID__c"
library(stringr)
library(stringi)

MC_Logins$NetID__c <- stri_match_first_regex(MC_Logins$Email, "(.*?)\\@")[,2]

MC_Logins <- MC_Logins %>%
  relocate(NetID__c, .before = Email)


# drop some columns from MC
names(MC_Logins)
# MC_Logins <- MC_Logins[,-c(1,2, 6,7)]
# colnames(MC_Logins)[1]<-"Email" # change campaign.member.email to email
# colnames(MC_Logins)[1]<-"NetID__c"
# users <- read.csv("./2022 Splunk Reports/February 2022/MtD Licensed Users Logged In-2022-02-25-17-00-06.csv")

####STEP 3: Merge files####
# from Frances
names(users_SF)

users<-users_SF[,-c(5,8:12)] # remove Email,"ProfileId", "Title", "Username","UserRoleId","UserType" 
users<-unique(users)

names(users)
names(permissionsets)
names(perms2prods)
perms2prods<-perms2prods[,-c(2)] # drop 'n'

users_perms<-merge(users, permissionsets, by.x = "Id", by.y = "AssigneeId")
users_perms_prods<-merge(users_perms, perms2prods, by.x = "PermissionSet.Name", by.y = "PermissionSet.Name", all.x = TRUE)
names(users_perms_prods)

upp_c<-merge(users_perms_prods, contact_records, by.x = "Id", by.y = "User__c", all.x = TRUE)
names(upp_c)

upp_c %>% count(Profile.Name)

upp_c <-distinct(upp_c)
# upp_c<-subset(upp_c, upp_c$PermissionSet.Name!="X00ef4000001uSgMAAU" & upp_c$PermissionSet.Name!="X00e2S000000G6SIQA0")

#### create 'base' product from profile name ####
upp_c$Product[upp_c$UserRole.Name=="Data Analyst" & is.na(upp_c$Product)]<-"Reports"
upp_c$Product[(upp_c$UserRole.Name=="SAFER Volunteer" | upp_c$UserRole.Name=="SAFER") & is.na(upp_c$Product)]<-"SAFER"
upp_c$Product[upp_c$Profile.Name=="Advising Base" & is.na(upp_c$Product)]<-"Scheduling/Notes"
upp_c$Product[upp_c$Profile.Name=="Service Desk Base" & is.na(upp_c$Product)]<-"Service Desk"
upp_c$Product[upp_c$UserRole.Name=="Marketer" & is.na(upp_c$Product)]<-"Marketing - SF"

upp_c2<-unique(upp_c)

# upp_c2<-subset(upp_c2, !is.na(upp_c2$Product)) # consider taking this out

####MC_s_c_u creation####
# subset contact records
contact_records <- subset(contact_records, !is.na(contact_records$NetID__c)) # 337326

# merge MC contacts
MC_contacts <- merge(contact_records, MC_Logins, by.x = "NetID__c", 
                     by.y = "NetID__c", all = TRUE)

# MC_contacts_social <- merge(MC_contacts, Social_Logins, by.x = c("NetID__c"), 
#                             by.y = c("NetID__c"), all = TRUE)
MC_s_c_u <- merge(MC_contacts, upp_c, by.x = c("NetID__c"),
                  by.y = c("NetID__c.x"), all=TRUE)
# MC_s_c_u <- merge(MC_contacts_social, upp_c, by.x = c("Id"), 
#                   by.y = c("Id"), all=TRUE)
names(MC_s_c_u) # marketing contacts users

# create a single column for Trellis Products
# product_longer <- MC_s_c_u %>% 
#   pivot_longer(cols = c(Profile.Name.x, Product),
#                names_to = "col_name", 
#                values_to = "MC-SFProduct") %>% 
#   drop_na("MC-SFProduct") %>% 
#   distinct()

product_longer <- MC_s_c_u %>% 
  pivot_longer(cols = c(Profile.Name.x, Product),
               names_to = "col_name", 
               values_to = "MC-SFProduct") %>% 
  drop_na("MC-SFProduct") %>% 
  distinct()
names(product_longer)

# select key variables
MC_s_c_u2 <- product_longer %>%
  select(NetID__c,Name.x,UserRole.Name,  CreatedDate.y, Name.y, Id.y, 
         "MC-SFProduct", PermissionSet.Name, Profile.Name.y,
         hed__Primary_Organization__c.x, Id.x,  
         Primary_Department__r.Name.y) %>% 
  distinct()

MC_s_c_u2 %>% 
  distinct(NetID__c, `MC-SFProduct`) %>%
  group_by(`MC-SFProduct`) %>% 
  count()%>% 
  ungroup()

write_named_csv(MC_s_c_u2)

#Merge in Affiliations
names(affiliations)
affiliation1s <- subset(affiliations, affiliations$hed__Primary__c==TRUE)
affiliation1s <- affiliation1s[, -c(4)] # remove "hed__Primary__c"   
affiliation1s<-distinct(affiliation1s)

df_foruse <- merge(MC_s_c_u2, affiliation1s, by.x = "Id.x", by.y = "hed__Contact__c", all.x = TRUE)
write_named_csv(df_foruse)

# distinct tables with select variables after merged into affiliations
names(df_foruse)
df_foruse2 <- df_foruse %>%
  select(NetID__c,Name.x,Id.x, UserRole.Name, Profile.Name.y, Id.y, Name.y, 
         "MC-SFProduct",  PermissionSet.Name, CreatedDate.y, 
         Parent_Organization__c, hed__Account__r.Name) %>% 
  distinct()

write_named_csv(df_foruse2)

# count unique
df_foruse2 %>% 
  distinct(NetID__c, `MC-SFProduct`) %>%
  group_by(`MC-SFProduct`) %>% 
  count()%>% 
  ungroup()

# figure out how many profile names there are
# count unique
df_foruse2 %>% 
  distinct(NetID__c, Profile.Name.y, Parent_Organization__c) %>%
  group_by(Parent_Organization__c) %>% 
  count()%>% 
  ungroup()


# filter based on profile name
library(dplyr)
target <- c("Marketing User", "System Administrator", "System User", 
            "Trellis API Read Only", "Trellis User")

df_fortable <- filter(df_foruse2, !Profile.Name.y %in% target)  # equivalently, dat %>% filter(name %in% target)
df_fortable %>% 
  distinct()

write_named_csv(df_fortable)

### add netid value if name present, add name if netid present
df_fortable2 <- df_fortable %>%
  dplyr::mutate(NetID = case_when(!is.na(NetID__c) ~ NetID__c,
                                  TRUE ~ Id.y))

### drop someone with netid crm-tech
df_fortable2 <- df_fortable2[!(df_fortable2$NetID=="crm-tech"),]

write_named_csv(df_fortable2)
# user role name
df_fortable2 %>% 
  distinct(NetID, Profile.Name.y) %>%
  group_by(Profile.Name.y) %>% 
  count()%>% 
  ungroup()

# df_fortable2 %>% 
#   distinct(NetID, Profile.Name.y) %>%
#   group_by(Profile.Name.y) %>% 
#   count()  %>% 
#   ungroup()

df_fortable2 %>% 
  distinct(NetID, `MC.SFProduct`) %>%
  group_by(`MC.SFProduct`) %>% 
  count()%>% 
  ungroup()


# 
# # group by percentages
# 
# df_fortable2_perc <- df_fortable2 %>% 
#   distinct(NetID, Parent_Organization__c, `MC-SFProduct`) %>% 
#   group_by(Parent_Organization__c)


# df_fortable2_perc <- df_fortable2 %>%                                    # Calculate percentage by group
#   group_by(Parent_Organization__c) %>%
#   mutate(perc = MC-SFProduct / sum(MC-SFProduct)) %>%
#   as.data.frame()
# df_fortable2_perc


# select key variables
df_fortable3 <- df_fortable2 %>%
  select(NetID, CreatedDate.y,UserRole.Name,
         `MC.SFProduct`,  Profile.Name.y,
         Parent_Organization__c, hed__Account__r.Name) %>% 
  distinct()

df_fortable3 %>% 
  distinct(NetID) %>%
  count()

write_named_csv(df_fortable3)

# ##reshape for one line per user
# names(df_fortable3)
# for_reshape<-df_fortable3
# for_reshape$counter<-1
# for_reshape<-unique(for_reshape)
# for_reshape %>% group_by(Profile.Name.y) %>% tally 

#test<-merge(for_reshape, users_stripped, by.x = "NetID__c.x", by.y = "NetID__c", all = TRUE)

# for_use<-reshape(for_reshape, v.names="counter", timevar="MC-SFProduct",
#                  idvar=c("CreatedDate.y", "NetID", "Parent_Organization__c", "Profile.Name.y", "UserRole.Name"),
#                  direction="wide")
# 

# #### Connect to Rserve ####
# Rserve(args=" --no-save --RS-conf ~/Documents/Rserv.cfg")

#### local login ####
setwd("~/Documents/Trellis/CRM-16425/CRM-16425-exploration/Jun2022 data")
df_fortable2 <- read.csv("df_fortable2_5_2022.csv")

# write named csv locally 
write_named_csv <- function(x) 
  write_csv(x, file = paste0(
    deparse(substitute(x)),"_", last_month, "_",this_year,".csv"))


#### flip data wider ####
wide_df_fortable2 <- df_fortable2 %>%
  select(NetID, Name.x, Name.y,CreatedDate.y, 
         Profile.Name.y,
         Parent_Organization__c, MC.SFProduct) %>% 
  distinct()

wide_df_fortable2 <- wide_df_fortable2 %>%
  pivot_wider(names_from = MC.SFProduct, values_from = MC.SFProduct) %>% 
  distinct()
# 
# # select key variables
# wide_df_fortable3 <- wide_df_fortable2 %>%
#   select(NetID, Name.x, Name.y,CreatedDate.y, 
#          Profile.Name.y,
#          Parent_Organization__c,   
#          `Service Desk`, `Scheduling/Notes`, Events, MC, `Marketing - SF`, 
#          `External Partners`, Reports, SAFER) %>% 
#   distinct()
# 

#### clean up name column ####
wide_df_fortable3 <- wide_df_fortable2  %>%
  dplyr::mutate(Name = case_when(!is.na(Name.y) ~ Name.y,
                                  TRUE ~ Name.x))


# Select columns except name.x and name.y
wide_df_fortable3 <-wide_df_fortable3 %>% select(-c(Name.x, Name.y)) 

# move name next to NetID
wide_df_fortable3 <- wide_df_fortable3 %>% relocate(Name)

# wide_df_fortable3 <- wide_df_fortable3 %>%
#   pivot_wider(names_from = Parent_Organization__c, values_from = Parent_Organization__c) %>% 
#   distinct()
# 
write.csv(wide_df_fortable3, "product_users_June2022_ParentOrg.csv")

# find duplicated netid rows
dup_fortable3 <- duplicated(wide_df_fortable3[,1:2])
duplicated <- wide_df_fortable3[dup_fortable3,]


fortable3_PO2 <- wide_df_fortable3 %>% 
  group_by(NetID) %>% 
  mutate(group = row_number()) %>% 
  pivot_wider(
    id_cols = NetID, 
    names_from = group,
    values_from = Parent_Organization__c,
    names_sort = TRUE
  ) %>% 
  ungroup() %>% 
  arrange(NetID)

# my_data %>%
#   group_by(id) %>%
#   mutate(
#     group = row_number()
#   ) %>%
#   pivot_wider(
#     id_cols = id, 
#     names_from = group,
#     values_from = org,
#     names_sort = TRUE
#   ) %>%
#   ungroup() %>%
#   arrange(id)

# merge this to the existing dataframce
# users_perms_prods<-merge(users_perms, perms2prods, by.x = "PermissionSet.Name", by.y = "PermissionSet.Name", all.x = TRUE)

# remove the parent org
wide_df_fortable4 <-wide_df_fortable3 %>% 
  select(-c(Parent_Organization__c)) %>% 
  distinct()

fortable4 <- merge(wide_df_fortable4, fortable3_PO2, by.x = "NetID", by.y = "NetID", all = TRUE)

# rename columns
colnames(fortable4)[13] = "Parent_Org_1"
colnames(fortable4)[14] = "Parent_Org_2"
colnames(fortable4)[15] = "Parent_Org_3"

write_named_csv(fortable4)
