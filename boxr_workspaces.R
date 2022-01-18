# Jung Mee Park
# jmpark@email.arizona.edu
# 2022-01-04

# working on scripting a automated system
library(tidyverse)
library(dplyr)
library(lubridate)
library(boxr) #https://r-box.github.io/boxr/
library(fs)
library(jose)
library(readxl)
library(readr)

# set local directory
# setwd("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/")
setwd("D:/Users/jmpark/Box/Trellis/Program Team/Trellis Metrics Reports/Trellis KPIs/Raw data")

# # authenticate
# dir_create("~/.boxr-auth", mode = 700)
# box_auth_service()
# box_auth(client_id = "your_client_id", client_secret = "your_client_secret")
# box_auth()

# download a file from box
# using directory
data_files <- list.files()  # Identify file names
data_files

### read data for user permissions and products
library(readxl)
perms2prods <- read.csv("./user_perms_prods.csv")  # Identify file names

# users <- read.csv("D:/Users/jmpark/Box/Trellis/Program Team/Trellis Metrics Reports/Trellis KPIs/Raw data/MtD Licensed Users Logged In-2021-04-30-17-00-04.csv")
users <- read.csv("./2021/December Splunk Reports/MtD Licensed Users Logged In-2021-12-31-17-00-15.csv")

# splunk reports
splunk_reports <- list.files("./2021")  # Identify file names
splunk_reports

# read in this year
today <- Sys.Date()

#capture current month
this_month <- month(today)
last_month<-month(today() - months(1))
this_year <- year(today)
last_year <- year(today() - years(1))

format(today, format="%B %d %Y")

##STEP 1: Read in Data from Box
Social_Logins <- read_csv("2021/December Splunk Reports/social_studio_logins,_last_12_months-2022-01-01.csv")
MC_Logins <- read_csv("2021/December Splunk Reports/Users,_last_login,_marketing_cloud-2022-01-01.csv")

##STEP 2: Import data from SF
#Oauth
sf_auth() #custom domain is ua-trellis

###bring in Contact Records
my_soql_contact <- sprintf("SELECT Id,	Emplid__c, Email, hed__Primary_Organization__c,
                            MDM_Primary_Type__c, NetId__c, Primary_Department__r.Name, 
                            User__c 
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

##STEP 3: Merge files
users<-merge(users_SF,users, by.x = "Email", by.y = "Username")

#rename var indicating that login was for a particular tool
MC_Logins$Profile.Name<-"MC"
Social_Logins$Profile.Name<-"Social"

users<-subset(users, users$Profile.Name %in% c("Salesforce Base", "Advising Base", "Service Desk Base"))
users$NetID__c[users$NetID__c=="Mlfink3"]<-"mlfink3"

# save to csv
# for last month and last year
write.csv(users, paste0("D:/Users/jmpark/WorkSpaces/jmpark_data/Program Team KPIs/users", last_month, last_year, ".csv"), row.names = FALSE)

##reshape the permissionsets file
users_perms<-merge(users, permissionsets, by.x = "Id", by.y = "AssigneeId")
users_perms_prods<-merge(users_perms, perms2prods, by.x = "PermissionSet.Name", 
                         by.y = "PermissionSet.Name", all.x = TRUE)
