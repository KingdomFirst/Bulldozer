# Bulldozer CSV Maps

### Person Info

#### 01 - Individual.csv
```
FamilyId
FamilyName
CreatedDate
PersonId
Prefix
FirstName
NickName
MiddleName
LastName
Suffix
FamilyRole
MaritalStatus
ConnectionStatus
RecordStatus
IsDeceased
HomePhone
MobilePhone
WorkPhone
AllowSMS
Email
IsEmailActive
AllowBulkEmail
Gender
DateOfBirth
School
GraduationDate
Anniversary
GeneralNote
MedicalNote
SecurityNote
```
##### Person Note Alerts
* If a Person Note begins with `[ALERT]` then the Rock Alert flag will be set.

##### Any Additional Fields will be Person Attributes

1. If no caret is in the Attribute Name (`Baptism Pastor`), just tries to match to an existing attribute by the name. If created, it's created as Text
2. If only one caret is in the AttributeName (`Baptism Pastor^V`), the Attribute Name and AttributeType are used. Note: D (Date), B (Boolean), V (Defined Value), E (Encrypted Text), L (Value List [^ separated values]), VL (Value List using Defined Type [^ separated values]) are specific types, anything else will be Text.
3. If two carets are in the Attribute Name(`^Membership^Baptism Pastor`), Attribute Category and AttributeName are used.
4. If three carets are in the AttributeName (`^Membership^Baptism Pastor^V`), Attribute Category, Attribute Name,and Attribute Type are used.
5. If four carets are in the AttributeName (`^Membership^Baptism Pastor^V^123`), Attribute Category, Attribute Name,Attribute Type, and Attribute Foreign Key are used.
6. If five carets are in the AttributeName (`^Membership^Baptism Pastor^V^123^456`), Attribute Category, Attribute Name,Attribute Type, Attribute Foreign Key, and Defined Type Foreign Key are used.

Note: The power of the five carets is that the Defined Type list created for an attribute can be used by multiple Defined Value Attributes. This is helpful if there are attributes used as surveys which have the same options used multiple times.

#### 02 - Family.csv
```
FamilyId
FamilyName
CreatedDate
Campus
Address
Address2
City
State
Zip
Country
SecondaryAddress
SecondaryAddress2
SecondaryCity
SecondaryState
SecondaryZip
SecondaryCountry
```


### Contributions

#### 03 - Account.csv
```
FinancialFundID
FinancialFundName
FinancialFundDescription
FinancialFundGLAccount
FinancialFundIsActive
FinancialFundStartDate
FinancialFundEndDate
FinancialFundOrder
FinancialFundParentID
FinancialFundPublicName
FinancialFundIsTaxDeductible
```
##### Any Additional Fields will be Account Attributes

1. If no caret is in the Attribute Name (`Bank Account`), just tries to match to an existingattribute by the name. If created, it's created as Text
2. If only one caret is in the AttributeName (`Bank Account^I`), the Attribute Name and AttributeType are used. Note: D (Date), B (Boolean), V (Defined Value), E (Encrypted Text), L (Value List [^ separated values]), VL (Value List using Defined Type [^ separated values]) are specific types, anything else will be Text.
3. If two carets are in the Attribute Name(`^General Ledger Export^Bank Account`), Attribute Category and AttributeName are used.
4. If three carets are in the AttributeName (`^General Ledger Export^Bank Account^I`), Attribute Category, Attribute Name,and Attribute Type are used.
5. If four carets are in the AttributeName (`^General Ledger Export^Bank Account^V^123`), Attribute Category, Attribute Name,Attribute Type, and Attribute Foreign Key are used.
6. If five carets are in the AttributeName (`^General Ledger Export^Bank Account^V^123^456`), Attribute Category, Attribute Name, AttributeType, Attribute Foreign Key, and Defined Type Foreign Key are used.

Note: The power of the five carets is that the Defined Type list created for an attribute can be used by multiple Defined Value Attributes. This is helpful if there are attributes used as surveys which have the same options used multiple times.

#### 04 - Batch.csv
```
BatchID
BatchName
BatchDate
BatchAmount
```
##### Any Additional Fields will be Batch Attributes

1. If no caret is in the Attribute Name (`Date Exported`), just tries to match to an existingattribute by the name. If created, it's created as Text
2. If only one caret is in the AttributeName (`Date Exported^D`), the Attribute Name and AttributeType are used. Note: D (Date), B (Boolean), V (Defined Value), E (Encrypted Text), L (Value List [^ separated values]), VL (Value List using Defined Type [^ separated values]) are specific types, anything else will be Text.
3. If two carets are in the Attribute Name(`^General Ledger Export^Date Exported`), Attribute Category and AttributeName are used.
4. If three carets are in the AttributeName (`^General Ledger Export^Date Exported^D`), Attribute Category, Attribute Name,and Attribute Type are used.
5. If four carets are in the AttributeName (`^General Ledger Export^Bank Account^V^123`), Attribute Category, Attribute Name,Attribute Type, and Attribute Foreign Key are used.
6. If five carets are in the AttributeName (`^General Ledger Export^Bank Account^V^123^456`), Attribute Category, Attribute Name, AttributeType, Attribute Foreign Key, and Defined Type Foreign Key are used.

Note: The power of the five carets is that the Defined Type list created for an attribute can be used by multiple Defined Value Attributes. This is helpful if there are attributes used as surveys which have the same options used multiple times.

#### 05 - Pledge.csv
```
IndividualID
FundName
SubFundName
FundGLAccount
SubFundGLAccount
FundIsActive
SubFundIsActive
PledgeFrequencyName
TotalPledge
StartDate
EndDate
PledgeId
PledgeCreatedDate
PledgeModifiedDate
```
#### 06 - Contribution.csv
```
IndividualID
FundName
SubFundName
FundGLAccount
SubFundGLAccount
FundIsActive
SubFundIsActive
ReceivedDate
CheckNumber
Memo
ContributionTypeName
Amount
StatedValue
ContributionID
ContributionBatchID
ContributionCreditCardType
IsAnonymous
Gateway
ScheduledTransactionForeignKey
```
##### Any Additional Fields will be Transaction Attributes

1. If no caret is in the Attribute Name (`Project`), just tries to match to an existing attribute by the name.If created, it's created as Text
2. If only one caret is in the AttributeName (`Project^V`), the Attribute Name and AttributeType are used. Note: D (Date), B (Boolean), V (Defined Value), E (Encrypted Text), L (Value List [^ separated values]), VL (Value List using Defined Type [^ separated values]) are specific types, anything else will be Text.
3. If two carets are in the Attribute Name(`^Projects^Project`), Attribute Category and AttributeName are used.
4. If three carets are in the AttributeName (`^Projects^Projects^V`), Attribute Category, Attribute Name,and Attribute Type are used.
5. If four carets are in the AttributeName (`^Projects^Project^V^123`), Attribute Category, Attribute Name,Attribute Type, and Attribute Foreign Key are used.
6. If five carets are in the AttributeName (`^Projects^Project^V^123^456`), Attribute Category, Attribute Name,Attribute Type, Attribute Foreign Key, and Defined Type Foreign Key are used.

Note: The power of the five carets is that the Defined Type list created for an attribute can be used by multiple Defined Value Attributes. This is helpful if there are attributes used as surveys which have the same options used multiple times.

#### 07 - ScheduledTransaction.csv

##### For best performance, group scheduled transactions by ID.

Create or modify the existing defined values for `Credit Card Type` before running the import to match any `ScheduledTransactionCreditCardType` values in CSV.  

Gateways will be created as new `ScheduledTransactionGateway` names are encountered which do not exist.  The Test Gateway Entity is used to create the new gateway.  This should be updated to the appropriate Gateway Entity (provider) post-migration.

```
ScheduledTransactionId                /* String [Required] */
ScheduledTransactionPersonId          /* String [Required] */
ScheduledTransactionCreatedDate       /* DateTime (mm/dd/yyyy hh:mm) [OptionalDefault=Now] */
ScheduledTransactionStartDate         /* Date (mm/dd/yyyy) [Required] */
ScheduledTransactionEndDate           /* Date (mm/dd/yyyy) [Optional] */
ScheduledTransactionNextPaymentDate   /* Date (mm/dd/yyyy) [Optional] */
ScheduledTransactionActive            /* Bool (TRUE|FALSE|1|0) [Optional Default=TRUE] */
ScheduledTransactionFrequency         /* String | Int [Required] */
ScheduledTransactionNumberOfPayments  /* Int [Optional] */
ScheduledTransactionTransactionCode   /* String [Required] */
ScheduledTransactionGatewaySchedule   /* String [Required] */
ScheduledTransactionGateway           /* String | Int [Required] */
ScheduledTransactionAccount           /* String | Int [Required] */
ScheduledTransactionAmount            /* Decimal [Required] */
ScheduledTransactionCurrencyType      /* String [Required] 'ACH' or 'CreditCard' */
ScheduledTransactionCreditCardType    /* String [Optional] */
```


### Groups and Attendance

#### 08 - NamedLocation.csv file
```
NamedLocationId                  /* String | Int [Requred] */
NamedLocationName                /* String [Requred] */
NamedLocationCreatedDate         /* Date (mm/dd/yyyy) [Optional] */
NamedLocationType                /* Guid | Int | String [Optional] */
NamedLocationParent              /* String | Int [Optional] */
NamedLocationSoftRoomThreshold   /* Int [Optional] */
NamedLocationFirmRoomThreshold   /* Int [Optional] */
```
#### 09 - GroupType.csv file
```
GroupTypeId                      /* String | Int [Requred] */
GroupTypeName                    /* String [Requred] */
GroupTypeCreatedDate             /* Date (mm/dd/yyyy) [Optional] */
GroupTypePurpose                 /* Guid | Int | String [Optional] */
GroupTypeInheritedGroupType      /* Guid | Int | String [Optional] */
GroupTypeTakesAttendance         /* "YES" | "NO" [Optional] */
GroupTypeWeekendService          /* "YES" | "NO" [Optional] */
GroupTypeShowInGroupList         /* "YES" | "NO" [Optional] */
GroupTypeShowInNav               /* "YES" | "NO" [Optional] */
GroupTypeParentId                /* String | Int [Optional] */
GroupTypeSelfReference           /* "YES" | "NO" [Optional] */
GroupTypeWeeklySchedule          /* "YES" | "NO" [Optional] */
```
#### 10 - Group.csv
```
GroupId                          /* String | Int [Requred] */
GroupName                        /* String [Requred] */
GroupCreatedDate                 /* Date (mm/dd/yyyy) [Optional] */
GroupType                        /* Guid | Int | String [Required] */
GroupParentGroupId               /* String | Int [Optional] */
GroupActive                      /* "YES" | "NO" [Optional] */
GroupOrder                       /* Int [Optional] */
GroupCampus                      /* String [Required] */
GroupAddress                     /* String [Optional] */
GroupAddress2                    /* String [Optional] */
GroupCity                        /* String [Optional] */
GroupState                       /* String [Optional] */
GroupZip                         /* String [Optional] */
GroupCountry                     /* String [Optional] */
GroupSecondaryAddress            /* String [Optional] */
GroupSecondaryAddress2           /* String [Optional] */
GroupSecondaryCity               /* String [Optional] */
GroupSecondaryState              /* String [Optional] */
GroupSecondaryZip                /* String [Optional] */
GroupSecondaryCountry            /* String [Optional] */
GroupNamedLocation               /* String [Optional] */
GroupDayOfWeek                   /* String [Optional] */
GroupTime                        /* String [Optional] */
GroupDescription                 /* String [Optional] */
```
##### Any Additional Fields will be Group Attributes

1. If no caret is in the Attribute Name (`Topic`), just tries to match to an existing attribute by the name.If created, it's created as Text
2. If only one caret is in the AttributeName (`Topic^V`), the Attribute Name and AttributeType are used. Note: D (Date), B (Boolean), V (Defined Value), E (Encrypted Text), L (Value List [^ separated values]), VL (Value List using Defined Type [^ separated values]) are specific types, anything else will be Text.
3. If two carets are in the Attribute Name(`^Topic^Topic`), Attribute Category and AttributeName are used.
4. If three carets are in the AttributeName (`^Topic^Topic^V`), Attribute Category, Attribute Name,and Attribute Type are used.
5. If four carets are in the AttributeName (`^Topic^Topic^V^123`), Attribute Category, Attribute Name,Attribute Type, and Attribute Foreign Key are used.
6. If five carets are in the AttributeName (`^Topic^Topic^V^123^456`), Attribute Category, Attribute Name,Attribute Type, Attribute Foreign Key, and Defined Type Foreign Key are used.

Note: The power of the five carets is that the Defined Type list created for an attribute can be used by multiple Defined Value Attributes. This is helpful if there are attributes used as surveys which have the same options used multiple times.

#### 11 - GroupMember.csv
```
GroupMemberId                    /* String [Required] */
GroupMemberGroupId               /* String | Int [Requred] */
GroupMemberPersonId              /* String | Int [Required] */
GroupMemberCreatedDate           /* Date (mm/dd/yyyy) [Optional] */
GroupMemberRole                  /* String [Optional] */
GroupMemberActive                /* "YES" | "NO" | "PENDING" [Optional, Default */
```

#### 11 - Relationship.csv
```
GroupMemberId                    /* String [Required] */
GroupMemberGroupId               /* String | Int [Requred] */
GroupMemberPersonId              /* String | Int [Required] */
GroupMemberCreatedDate           /* Date (mm/dd/yyyy) [Optional] */
GroupMemberRole                  /* String [Optional] */
GroupMemberActive                /* "YES" | "NO" | "PENDING" [Optional, Default */
```
Note: `GroupMemberGroupId` for the Relationship file should be the Person Id of the inverse relationship.

#### 12 - Attendance.csv

##### For performance reasons, it is recommended that your csv file be ordered by GroupId. This reduces database overhead.
```
AttendanceId                     /* String | Int [Required] */
AttendanceGroupId                /* String | Int [Required] */
AttendancePersonId               /* String | Int [Required] */
AttendanceCreatedDate            /* Date (mm/dd/yyyy) [Optional] */
AttendanceDate                   /* Date (mm/dd/yyyy) [Required] */
AttendanceAttended               /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=TRUE] */
AttendanceLocationId             /* String | Int [Optional] */
```


### Advanced Imports

#### 13 - Metrics.csv
```
MetricCampus
MetricName
MetricValue
MetricService
MetricCategory
MetricNote
```
#### 14 - UserLogin.csv

```
CommonAuthentication Types:
com.minecartstudio.Authentication.Arena     - Password should be Hex string of Arena password
Rock.Security.Authentication.ActiveDirectory - Password should be blank
```
```
UserLoginId                      /* String | Int [Required] */
UserLoginPersonId                /* String | Int [Required] */
UserLoginUserName                /* String [Required] */
UserLoginPassword                /* Hex String [Optional] */
UserLoginDateCreated             /* Date (mm/dd/yyyy) [Optional] */
UserLoginAuthenticationType      /* String [Required] */
UserLoginIsConfirmed             /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=TRUE] */
```


### Content Channels

#### 15 - ContentChannel.csv
```
ContentChannelName               /* String [Required] */
ContentChannelTypeName           /* String [Required] */
ContentChannelDescription        /* String [Optional] */
ContentChannelId                 /* String | Int [Optional] */
ContentChannelRequiresApproval   /* "TRUE" | "FALSE"| 1 | 0 [Optional Default=FALSE] [Optional] */
ContentChannelParentId           /* String | Int [Optional] */
```
##### Any Additional Fields will be Content ChannelAttributes

1. If no caret is in the Attribute Name (`Author`), just tries to match to an existing attribute by the name.If created, it's created as Text
2. If only one caret is in the AttributeName (`Author^V`), the Attribute Name and AttributeType are used. Note: D (Date), B (Boolean), V (Defined Value), E (Encrypted Text), L (Value List [^ separated values]), VL (Value List using Defined Type [^ separated values]) are specific types, anything else will be Text.
3. If two carets are in the Attribute Name(`^Podcast^Author`), Attribute Category and AttributeName are used.
4. If three carets are in the AttributeName (`^Podcast^Author^V`), Attribute Category, Attribute Name,and Attribute Type are used.
5. If four carets are in the AttributeName (`^Podcast^Author^V^123`), Attribute Category, Attribute Name,Attribute Type, and Attribute Foreign Key are used.
6. If five carets are in the AttributeName (`^Podcast^Author^V^123^456`), Attribute Category, Attribute Name,Attribute Type, Attribute Foreign Key, and Defined Type Foreign Key are used.

Note: The power of the five carets is that the Defined Type list created for an attribute can be used by multiple Defined Value Attributes. This is helpful if there are attributes used as surveys which have the same options used multiple times.

#### 16 - ContentChannelItem.csv
```
ContentChannelName               /* String [Required] */
ItemTitle                        /* String [Required] */
ItemContent                      /* String [Optional] */
ItemStart                        /* DateTime [Optional] */
ItemExpire                       /* DateTime [Optional] */
ItemId                           /* String | Int [Optional] */
ItemParentId                     /* String | Int [Optional] */
```
##### Any Additional Fields will be Content Channel Item Attributes

1.  If no caret is in the Attribute Name (`Author`), just tries to match to an existing attribute by the name.If created, it's created as Text
2.  If only one caret is in the AttributeName (`Author^V`), the Attribute Name and AttributeType are used. Note: D (Date), B (Boolean), V (Defined Value), E (Encrypted Text), L (Value List [^ separated values]), VL (Value List using Defined Type [^ separated values]) are specific types, anything else will be Text.
3.  If two carets are in the Attribute Name(`^Podcast^Author`), Attribute Category and AttributeName are used.
4.  If three carets are in the AttributeName (`^Podcast^Author^V`), Attribute Category, Attribute Name, andAttribute Type are used.
5.  If four carets are in the AttributeName (`^Podcast^Author^V^123`), Attribute Category, Attribute Name,Attribute Type, and Attribute Foreign Key are used.
6.  If five carets are in the AttributeName (`^Podcast^Author^V^123^456`), Attribute Category, Attribute Name,Attribute Type, Attribute Foreign Key, and Defined Type Foreign Key are used.

Note: The power of the five carets is that the Defined Type list created for an attribute can be used by multiple Defined Value Attributes. This is helpful if there are attributes used as surveys which have the same options used multiple times.

#### 17 - GroupPolygon.csv
```
GroupId                          /* String | Int [Requred] */
GroupName                        /* String [Requred] */
GroupCreatedDate                 /* Date (mm/dd/yyyy) [Optional] */
GroupType                        /* Guid | Int | String [Required] */
GroupParentGroupId               /* String | Int [Optional] */
GroupActive                      /* "YES" | "NO" [Optional] */
GroupOrder                       /* Int [Optional] */
GroupCampus                      /* String [Required] */
Latitude                         /* String [Required] */
Longitude                        /* String [Required] */
```
#####Important Note about Polygons
Issues arise with the geospatial processing depending on SQL Versions.  If any exceptions are thrown, try installing `Microsoft System CLR Types for SQL Server 2012 (x64)` ( http://go.microsoft.com/fwlink/?LinkID=239644&clcid=0x409 ).

#### 18 - Notes.csv

```
NoteType                            /* String [Required] */
EntityTypeName                      /* String [Required] */
EntityForeignId                     /* Int [Required] */
NoteCaption                         /* String [Optional] */
NoteText                            /* String [Optional] */
NoteDate                            /* DateTime [Optional] */
CreatedById                         /* Int [Optional] */
IsAlert                             /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=FALSE] [Optional] */
IsPrivate                           /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=FALSE] [Optional] */
```

Note: EntityTypeName should be the fully-qualified Rock EntityType, e.g. `Rock.Model.Person.`  CreatedById is the ForeignId of the person who created the note.

#### 19 - PrayerRequest.csv

```
PrayerRequestCategory              /* String [Required] */
PrayerRequestText                  /* String [Required] */
PrayerRequestDate                  /* DateTime [Required] */
PrayerRequestId                    /* String [Optional] */
PrayerRequestFirstName             /* String [Required] */
PrayerRequestLastName              /* String [Optional] */
PrayerRequestEmail                 /* String [Optional] */
PrayerRequestExpireDate            /* DateTime [Optional] */
PrayerRequestAllowComments         /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=TRUE] [Optional] */
PrayerRequestIsPublic              /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=TRUE] [Optional] */
PrayerRequestIsApproved            /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=TRUE] [Optional] */
PrayerRequestApprovedDate          /* DateTime [Optional] */
PrayerRequestApprovedById          /* String [Optional] */
PrayerRequestCreatedById           /* String [Optional] */
PrayerRequestRequestedById         /* String [Optional] */
PrayerRequestAnswerText            /* String [Optional] */
```

#### 20 - PreviousLastName.csv

```
PreviousLastNamePersonId          /* String | Int [Required] */
PreviousLastName                  /* String [Required] */
PreviousLastNameId                /* String | Int | Guid [Optional] */
```

#### 21 - BankAccount.csv

```
FamilyId                          /* String | Int [Optional] */
RoutingNumber                     /* Int [Required] */
AccountNumber                     /* String | Int [Required] */
IndividualId                      /* String | Int [Required] */
```

#### 22 - PhoneNumber.csv

```
PhonePersonId                     /* String | Int [Required] */
PhoneType                         /* String [Required] */
Phone                             /* String [Required] */
PhoneIsMessagingEnabled           /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=FALSE] [Optional] */
PhoneIsUnlisted                   /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=FALSE] [Optional] */
PhoneId                           /* String | Int [Optional] */
```

#### 23 - EntityAttribute.csv

```
AttributeEntityTypeName           /* String [Required] */
AttributeId                       /* String | Int [Optional] */
AttributeRockKey                  /* String [Optional, defaults to AttributeName without whitespace]    Note: AttributeId will be added as FK/FID to attributes that match Entity and Attribute Key with null FK/FID */
AttributeName                     /* String [Required] */
AttributeCategoryName             /* String [Optional] */
AttributeType                     /* "D" | "B" | "V" | "E" | "L" | "VL" | "" */
AttributeDefinedTypeId            /* String | Int [Optional] */
AttributeEntityTypeQualifierName  /* String [Optional] */
AttributeEntityTypeQualifierValue /* String | Int [Optional] */
```

#### 24 - EntityAttributeValue.csv

```
AttributeEntityTypeName           /* String [Required] */
AttributeId                       /* String | Int [Optional] */
AttributeRockKey                  /* String [Optional, defaults to AttributeName without whitespace]    Note: AttributeId will be added as FK/FID to attributes that match Entity and Attribute Key with null FK/FID */
AttributeValueId                  /* String | Int [Optional] */
AttributeValueEntityId            /* String | Int [Required] */
AttributeValue                    /* String [Required] */
```

Note: Sort by the AttributeEntityTypeName, AttributeId, AttributeRockKey, then AttributeValueEntityId
