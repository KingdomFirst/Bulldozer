# Bulldozer F1 Maps

This readme file explains and highlights the data mapping from FellowshipOne (F1) to Rock.  This document is broken into sections for Person, Financial, Textual, Volunteer, Attendance, and Binary data.  Within those sections, the numbers on each table show the priority that Bulldozer will use when starting a new import or continuing an import.  Bulleted items within the table are all the fields that are mapped into Rock data.

Before starting the import, you'll need to run the `Preimport Fix.sql` script in MigrationUtilities\FellowshipOne to fix certain data pointers that OrcaMDF has trouble reading otherwise.  The FellowshipOne MDF will have to be mounted to a SQL instance to run the script.  Additionally, if your F1 client is multi-site, you'll need to add the Rock.Campus names and shortcodes so those campuses can be matched to Group and Financial records.

Note: Rock entity types and columns are referenced with capital letters for easier SQL lookups (ex: Person.FirstName).

### Person Data

1. Individual_Household

   This table contains critical person information in FellowshipOne as well as the foreign keys necessary to import other data types.  Bulldozer will not run unless this table is selected or imported people already exist in the system.

   - Individual_ID

     This field is mapped to Person.ForeignId as well as a PersonAlias.ForeignId.  If duplicate people are imported and Rock merges the Person records, the PersonAlias record will still have this ForeignId reference.

     Almost every other table in FellowshipOne relies on a match to Individual_Id.

   - Household_ID

     This field is mapped to Group.ForeignId for the Family this Person belongs to.

   - Household_Name

     This field is mapped to Group.Name for the Family this Person belongs to.

   - Household_Position

     This field is mapped to the Group Role for the Family this Person belongs to, either Adult or Child.  Visitors are mapped to their own Family with a Known Relationship between them and the original Family.

   - Last_Name

   - First_Name

   - Middle_Name

   - Goes_By

     This field is mapped to Person.NickName.

   - Former_Name

     This field is mapped to Person.PreviousName.

   - Prefix

     This field is mapped to Person.Prefix.  If a custom value is found, it's added to the list of Prefix defined values.

   - Suffix

     This field is mapped to Person.Suffix.  If a custom value is found, it's added to the list of Suffix defined values.

   - Gender

   - Date_Of_Birth

   - Marital_Status

     This field is mapped to Person.Marital Status.  If a custom value is found, it's added to the list of Marital Status defined values.

   - First_Record

     This field is mapped to the First Visit person attribute.

   - Occupation_Name

   - Occupation_Description

     Name and Description are mapped to the Position person attribute.

   - Employer

     This field is mapped to the Employer person attribute.

   - School_Name

     This field is mapped to the School person attribute.  

   - Former_Church

     This field is mapped to the Previous Church person attribute.

   - Status_Name

     This field is mapped to Person.Connection Status.  If a custom value is found, it's added to the list of Connection Status defined values.

   - Status_Date

     This field is mapped to the Membership Date person attribute if their Connection Status was Member.

   - Default_tag_comment

     This field is mapped to the Allergy person attribute.

   - SubStatus_Name

     This field is used as the Campus designation for multi-site churches. If multiple Campuses exist, the most common Campus designation for members of a household will be used as the Family.Campus. 

   - Status_Comment

     This field is mapped to Person.SystemNote.

2. Company

   This table contains all of the information needed to create a Rock Business.  A Person record and Family group will be created but with the appropriate Business Record type.  A company will not have an Individual_ID.

3. Attribute

   This table contains all the custom person attributes your organization has created.  If a Rock Person Attribute exists with the same name, the Rock attribute will be used instead of creating a new one.  Since attributes may need extra View/Edit security, custom Attribute Categories are not automatically added to the Person profile page.

   Note: if the Attribute name begins with `Baptism,` it will be imported as the `Baptism Date` person attribute.  If the Attribute name begins with `Benevolence,` a Benevolence Request will be created instead of a custom attribute.

   - Attribute_Group_Name

     This field is mapped to Attribute.Category.  If the name is prefixed with a Campus designation, the Campus is removed and an additional `[Attribute] Campus` will be created with this Person's Campus value.

   - Attribute_Name

     This field is mapped to Attribute.Name.  If the name is prefixed with a Campus designation, the Campus is removed and an additional `[Attribute] Campus` will be created with this Person's Campus value.

   - Start_Date

   - End_Date

     Start / End Date are mapped to the Attribute Value.  If Start and End are both empty, this Attribute will be created as a Text field type.

   - Comment

     This field is mapped as a Person.Note if this Attribute is a Date field type.  If this Attribute is a Text field type, the Comment is used as the Attribute Value.

   - Staff_Individual_ID

     This field is mapped as the Note.Creator if this Attribute is a Text field type.


4. Communication

   This table contains all the contact information for each person such as emails, phone numbers, and social media.  A custom attribute will also be created if anyone had an Infellowship Login.

   - Communication_Type

     This field will be used to create or update the Person.Email, Person.PhoneNumber, or social media person attributes (Twitter, Facebook, LinkedIn). 

   - Communication_Value

     This field will be mapped to the Attribute Value.

   - Listed

     This field will determine whether phone numbers are listed or emails can be bulk emailed.

   - Communication_Comment

     This field will be mapped as Email.Description or a PhoneNumber.Note.

5. Household_Address

   This table contains all the address information for each family.

   - Address_Type

     This field is mapped to the location type, either Primary, Business, Previous, Other, or custom type.

   - Address_1

   - Address_2

   - City

   - State

   - Postal_Code

   - country

     This field is lower-case and the MDF reader is case-sensitive, so this will break if the F1 schema is updated.

   - County

6. Requirement

   This table contains data for attributes that require additional clearance, such as Background Checks, Sexual Abuse Training, and any other custom requirements.  For each requirement, two custom attributes will be created: `[Requirement] Date` and `[Requirement] Status.` If a Rock attribute already exists by the same name (e.g.  `Background Check Date`), it will be used instead.

   - Requirement_Name

     This field is mapped to Attribute.Name.  The name without special characters is used as the Attribute.Key.

   - Requirement_Date

     This field is mapped to the `[Requirement] Date` person Attribute Value.

   - Requirement_Status_Name

     This field is mapped to the `[Requirement] Status` person Attribute Value.  If a custom value is found, it's added to the list of values available to this Attribute. 

   - Is_Confidential

     If this field is checked, the attribute is added to the `Confidential` attribute category.

   

### Financial Data

1. Batch

   This table contains the data required to reconcile contribution data with a General Ledger (GL) accounting system.  Batches need to be imported before Contributions so they can be matched properly.  All batches will be imported with a status of closed.

   - BatchID

     This field is mapped to FinancialBatch.ForeignId.

   - BatchName

     This field is mapped to FinancialBatch.Name.  If the name starts with a Campus designation, the Batch will be attributed to that Campus.

   - BatchDate

     This field is mapped to FinancialBatch.StartDate and FinancialBatch.EndDate.

   - BatchAmount

     This field is mapped to FinancialBatch.ControlAmount.

2. Contribution

   This table contains individual contribution and contribution fund data.  

   - ContributionID

     This field is mapped to FinancialTransaction.ForeignId.

   - BatchID

     This field is mapped to an existing FinancialBatch, or a global default FinancialBatch will be used.

   - Fund_Name

     This field is mapped to a new or existing FinancialAccount.

   - Sub_Fund_Name

     This field is mapped to a new or existing FinancialAccount, with the Fund_Name FinancialAccount as its parent.  If the subfund starts with a Campus designation, the FinancialAccount is attributed to that Campus.

   - Received_Date

     This field is mapped to FinancialTransaction.TransactionDateTime and FinancialTransaction.CreatedDateTime.

   - Check_Number

     This field is mapped to FinancialTransaction.TransactionCode.

   - Memo

     This field is mapped to FinancialTransaction.Summary.

   - Contribution_Type_Name

     This field is mapped to the FinancialTransaction.Currency Type: Cash, Check, ACH, or CC.  It also sets the  FinancialTransaction.Source Type: Onsite or Website.

   - Card_Type

     If this field exists, it will be mapped to FinancialPaymentDetail.CardTypeValue.

   - Last_Four

     If this field exists, it will be mapped to FinancialPaymentDetail.AccountNumberMasked.

   - Amount

     This field is mapped to FinancialTransactionDetail.Amount.  If the Amount is negative, a FinancialTransactionRefund will be created and associated with the FinancialTransaction.

   - Stated_Value

     If the Amount field is empty, this field is mapped to FinancialTransactionDetail.Amount.  If the Amount is negative, a FinancialTransactionRefund will be created and associated with the FinancialTransaction.

3. Account

   This table contains routing and checking account data in plain text.  If you plan to store the MDF for any length of time, truncate this table or take the proper measures to secure this data.  

   - Account_Type_Name

     This field designates whether an account is Checking or ACH.

   - Routing_Number

   - Account

     This field is encrypted with the routing number and created as FinancialPersonBankAccount.AccountNumberSecured.  The last four digits of the account number will be stored as FinancialPersonBankAccount.AccountNumberMasked.

4. Pledge

   This table contains individual pledge information per fund, frequency, and duration.

   - Fund_Name

     This field is mapped to a new or existing FinancialAccount.

   - Sub_Fund_Name

     This field is mapped to a new or existing FinancialAccount, with the Fund_Name FinancialAccount as its parent.  If the subfund starts with a Campus designation, the FinancialAccount is attributed to that Campus.

   - Pledge_Frequency_Name

     This field is mapped to the FinancialPledge.PledgeFrequency defined value.

   - Total_Pledge

     This field is mapped to FinancialPledge.TotalAmount.

   - Start_Date

   - End_Date



### Textual Data

1. Notes

   This table contains data for any individual profile notes, including security and care notes.

   - Note_Type_Name

     This field is mapped to a new or existing notetype by the same name.  Any new notetype will have the default security settings.  If this field starts with `General` then it will be mapped to the default Personal NoteType.

   - Note_Text

     This field is mapped to Note.Text and special HTML characters like line breaks and tabs are removed.

   - NoteCreatedByUserId

     If this field can be matched to an F1 portal user, the Note.CreatedByPersonAliasId is set and that Person's name will be displayed next to the Note.Caption.

2. ContactFormData

   This table contains information from F1 emails or requests submitted on paper forms and contact cards.  Emails are mapped to a Communication item while the initial request text is mapped as a person Note.

   - ContactInstItemId

     This field is mapped as the ForeignId on either the Communication or person Note.

   - ContactItemIndividualId

     This field is mapped to the Communication.Recipient or Note.Person.

   - ContactActivityDate

     This field is mapped to Communication.CreatedDate or Note.CreatedDate.

   - ContactFormName

     This field is `Email` for a Communication, otherwise it's mapped as the NoteType for a personal note. 

   - ContactItemName

     This field is mapped to Communication.Subject or Note.Caption.

   - ContactNote

   - ContactItemNote

     Both Note fields are combined into a single text field to map to Communication.Message or Note.Text.

   - ContactItemAssignedUserId

   - ContactAssignedUserId

   - InitialContactCreatedByUserId

     The most recently assigned user field is mapped to Communication.Sender or Note.CreatedByPerson

3. IndividualContactNotes

   This table contains follow-up notes from the initial ContactForm that was submitted. If the ContactForm was an email, a ContactNote record will exist without any text and will be skipped during import.

   - ContactInstItemId

     This field is used to look up the person Note previously created during the ContactForm import.

   - IndividualContactNote

     This text is appended to the ContactForm text and stored on the original person Note.

   - ConfidentialText

     Confidential text is mapped to a new person Note of the `Confidential` NoteType.

### Volunteer / Groups Data

1. ActivityMinistry

   This table contains the top-level F1 check-in and ministry group structure.  The entire structure will be imported under the parent group `Archived Groups.`  The ministry name is mapped as a GroupType and a holder group created for that level.  If a ministry or activity name starts with a Campus name or shortcode, a Campus group is created at the next level and subsequent groups stored below it.  The campus name or shortcode will be removed from the name. 

   - Ministry_Id

     This field is mapped to the ForeignKey of the ministry-level Group.

   - Activity_Id

     This field is mapped to the ForeignKey of the activity-level Group.

   - Ministry_Name

     This field is used to create the top-level GroupType as well as a ministry-level Group, e.g. `Children's Ministry.`

   - Activity_Name

     This field is used to create the activity-level Group, e.g. `Kids Weekend Check-in.`

   - Ministry_Active

     This field determines whether the ministry-level Group is active.

   - Activity_Active

     This field determines whether the activity-level Group is active.

2. Activity_Group

   This table contains the mid-level F1 check-in and ministry group structure.  This structure will be imported underneath the existing activity-level groups.  

   - Activity_Super_Group_Id

     If it exists, this field is mapped to the ForeignKey of the tertiary-level group.

   - Activity_Group_Id

     This field is mapped to the ForeignKey of the tertiary-level activity group.

   - Activity_Super_Group

     If it exists, this field is used to create a tertiary-level group, e.g. `Kids Elementary.`

   - Activity_Group_Name

     This field is mapped to the tertiary or quaternary group (if Super_Group exists), e.g. `Kids 2nd Grade.`

3. RLC

   This table contains the lowest-level group and locations for F1 check-in and ministry structure.  The groups will be imported underneath the existing activity-level groups and locations created for each group.  

   - RLC_Id

     This field is mapped to the ForeignKey of the lowest-level group.

   - RLC_Name

     This field is used to create the lowest-level group and rooms, e.g. `Kids 2nd Grade Room A.`  If this name starts with a Campus name or shortcode, a campus-level Location is created to contain the subsequent building and room locations.

   - Building_Name

     If it exists, this field is used to create a building-level Location.

   - Room_Name

     This field is used to create a room-level Location.  The RLC Group will be associated with this Location.

   - RLC_Active

     This field determines whether the lowest-level group is active.

4. Staffing_Assignment

   This table contains all the volunteer assignments used in F1 as well as their role description.

   - Job_Title

     This field is mapped to a new GroupRole for the respective Group (should be already imported).  If the title ends with `Leader` then the GroupRole is marked as a Leader role in Rock.

   - Individual_Id

     This field is mapped to the person that should receive this Group Membership.

   - Is_Active

     This field determines whether the assigned Group Membership is active.
     
5. Activity_Assignment

   This table contains all the event assignments used in F1 as well as their role.

   - Individual_Id

     This field is mapped to the person that should receive the event Group Membership.

   - AssignmentDateTime

     This field is mapped to GroupMember.CreatedDateTime.
     
   - Activity_Start_Time

     This field is mapped to GroupMember.DateTimeAdded.
     
   - Activity_End_Time

     This field determines if the event is Active or Future, making the Membership Active.
     
   - Activity_Time_Name

     This field determines the Member Role name for this event group.

6. Groups

   This table contains all the small groups, people lists, and any other rosters created through F1.  The entire structure will be imported under the parent group `Archived Groups.`  If a group type name starts with a Campus name or shortcode, a Campus group is created at the next level.  The campus name or shortcode will be removed from the name. 

   - Group_Id

     This field is mapped to the ForeignKey of the child Group.

   - Group_Type_Name

     This field is mapped to the GroupType that will be used for child Groups.  A placeholder Group will be created for the GroupType.

   - Group_Name

     This field is used to create the child Group.

   - Individual_Id

     This field is mapped to the person that should receive this Group Membership.

### Attendance Data

1. Attendance

   This table contains all the child and volunteer check-in attendance from F1.  It does not include small group attendance.

   - RLC_Id

     This field is mapped to the previously-imported check-in Group.  If that Group has a Campus designation, the Attendance.Campus is set as well.

   - Start_Date_Time

     This field is mapped to Attendance.StartDate.  Any attendance without a StartDate will be skipped.

   - BreakoutGroup_Name

     This field is mapped to Attendance.Note.

   - Check_In_Time

     This field is mapped to Attendance.CreatedDateTime.

   - Check_Out_Time

     This field is mapped to Attendance.EndDateTime.

   - Checkin_Machine_Name

     This field is used to create a new Device with the name of the machine.

   - Tag_Code

     This field was mapped to AttendanceCode, but commented out for performance reasons.

2. GroupsAttendance

   This table contains all the small group attendance from F1.  If a GroupId is referenced but cannot be matched to a previously-imported Group, that attendance will be skipped.

   Note: if the F1 MDF does not have a GroupsAttendance.Individual_Present field, there is no way to determine which group members attended and which did not.  Contact F1 to add that field to the export.

   - GroupID

     This field is mapped to the previously-imported Group.  If that Group has a Campus designation, the Attendance.Campus is set as well.

   - StartDateTime

     This field is mapped to Attendance.StartDate.  Any attendance without a StartDate will be skipped.

   - Comments

     This field is mapped to Attendance.Note.

   - Individual_Present

     This field is mapped to Attendance.DidAttend.  

   - CheckoutDateTime

     This field is mapped to Attendance.EndDateTime.

   - AttendanceCreatedDate

     This field is mapped to Attendance.CreatedDateTime.

### Metric Data

1. Headcount

   This table contains metric values for service headcounts, small groups and volunteer groups.  

   - Headcount_ID
   
     This field is mapped to the ForeignId of the new Metric.
     
   - Activity_ID
   
     This field is used to determine the new Metric's category hierarchy.

   - RLC_Id

     This field is used to determine the new Metric's category hierarchy.

   - RLC_name

     This field is used as the new Metric's Name. 

   - Start_Date_Time

     This field is used as the Metric Value's Date.

   - Attendance

     This field is mapped to MetricValue.YValue.
   
   - Meeting_note

     This field is mapped to the Metric Value note. 

   