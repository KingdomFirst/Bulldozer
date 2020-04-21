// <copyright>
// Copyright 2019 by Kingdom First Solutions
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>
//
using System;
using System.Collections.Generic;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;

namespace Bulldozer.Utility
{
    public static partial class CachedTypes
    {
        // Global flag to turn off Rock processing

        public static bool DisableAuditing = true;

        // Common Delimiters

        public static char[] ValidDelimiters = new char[] { '*', '-', '|', ':' };

        // Default Datetimes

        public static DateTime DefaultDateTime = new DateTime();
        public static DateTime DefaultSQLDateTime = new DateTime( 1900, 1, 1 );

        // Field Types

        public static int BooleanFieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.BOOLEAN.AsGuid() ).Id;
        public static int CampusFieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.CAMPUS.AsGuid() ).Id;
        public static int SingleSelectFieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.SINGLE_SELECT.AsGuid() ).Id;
        public static int DateFieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.DATE.AsGuid() ).Id;
        public static int DefinedValueFieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.DEFINED_VALUE.AsGuid() ).Id;
        public static int IntegerFieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.INTEGER.AsGuid() ).Id;
        public static int TextFieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.TEXT.AsGuid() ).Id;
        public static int EncryptedTextFieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.ENCRYPTED_TEXT.AsGuid() ).Id;
        public static int ValueListFieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.VALUE_LIST.AsGuid() ).Id;

        // Entity Types

        public static int AttributeEntityTypeId = EntityTypeCache.Get( typeof( Rock.Model.Attribute ) ).Id;
        public static int BatchEntityTypeId = EntityTypeCache.Get( typeof( FinancialBatch ) ).Id;
        public static int PersonEntityTypeId = EntityTypeCache.Get( typeof( Person ) ).Id;
        public static int UserLoginTypeId = EntityTypeCache.Get( typeof( UserLogin ) ).Id;
        public static int PrayerRequestTypeId = EntityTypeCache.Get( typeof( PrayerRequest ) ).Id;
        public static int TransactionEntityTypeId = EntityTypeCache.Get( typeof( FinancialTransaction ) ).Id;
        public static int DatabaseStorageTypeId = EntityTypeCache.Get( typeof( Rock.Storage.Provider.Database ) ).Id;
        public static int FileSystemStorageTypeId = EntityTypeCache.Get( typeof( Rock.Storage.Provider.FileSystem ) ).Id;
        public static int EmailCommunicationMediumTypeId = EntityTypeCache.Get( typeof( Rock.Communication.Medium.Email ) ).Id;
        public static int TestGatewayTypeId = EntityTypeCache.Get( typeof( Rock.Financial.TestGateway ) ).Id;

        public static int MetricCategoryEntityTypeId = EntityTypeCache.Get( typeof( MetricCategory ) ).Id;
        public static int CampusEntityTypeId = EntityTypeCache.Get( typeof( Campus ) ).Id;
        public static int ScheduleEntityTypeId = EntityTypeCache.Get( typeof( Schedule ) ).Id;

        // Group Types

        public static int FamilyGroupTypeId = GroupTypeCache.Get( Rock.SystemGuid.GroupType.GROUPTYPE_FAMILY.AsGuid() ).Id;
        public static int GeneralGroupTypeId = GroupTypeCache.Get( Rock.SystemGuid.GroupType.GROUPTYPE_GENERAL.AsGuid() ).Id;
        public static int SmallGroupTypeId = GroupTypeCache.Get( Rock.SystemGuid.GroupType.GROUPTYPE_SMALL_GROUP.AsGuid() ).Id;

        public static GroupTypeCache KnownRelationshipGroupType = GroupTypeCache.Get( Rock.SystemGuid.GroupType.GROUPTYPE_KNOWN_RELATIONSHIPS );
        public static GroupTypeCache ImpliedRelationshipGroupType = GroupTypeCache.Get( Rock.SystemGuid.GroupType.GROUPTYPE_PEER_NETWORK );

        // Group Location Types

        public static int HomeLocationTypeId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_HOME.AsGuid() ).Id;
        public static int PreviousLocationTypeId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_PREVIOUS.AsGuid() ).Id;
        public static int WorkLocationTypeId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_WORK.AsGuid() ).Id;

        // Defined Type/Value Types

        public static int PersonRecordTypeId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.PERSON_RECORD_TYPE_PERSON.AsGuid() ).Id;
        public static int BusinessRecordTypeId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.PERSON_RECORD_TYPE_BUSINESS.AsGuid() ).Id;

        private static DefinedValueService definedValueService = new DefinedValueService( new RockContext() );
        private static DefinedValue MemberConnectionStatusIdOrNull = definedValueService.Get( Rock.SystemGuid.DefinedValue.PERSON_CONNECTION_STATUS_MEMBER.AsGuid() );
        private static DefinedValue AttendeeConnectionStatusIdOrNull = definedValueService.Get( Rock.SystemGuid.DefinedValue.PERSON_CONNECTION_STATUS_ATTENDEE.AsGuid() );
        private static DefinedValue VisitorConnectionStatusIdOrNull = definedValueService.Get( Rock.SystemGuid.DefinedValue.PERSON_CONNECTION_STATUS_VISITOR.AsGuid() );

        public static int MemberConnectionStatusId = ( MemberConnectionStatusIdOrNull != null ? MemberConnectionStatusIdOrNull.Id : Extensions.AddDefinedValue( new RockContext(), Rock.SystemGuid.DefinedType.PERSON_CONNECTION_STATUS, "Member", Rock.SystemGuid.DefinedValue.PERSON_CONNECTION_STATUS_MEMBER ).Id );
        public static int AttendeeConnectionStatusId = ( AttendeeConnectionStatusIdOrNull != null ? AttendeeConnectionStatusIdOrNull.Id : Extensions.AddDefinedValue( new RockContext(), Rock.SystemGuid.DefinedType.PERSON_CONNECTION_STATUS, "Attendee", Rock.SystemGuid.DefinedValue.PERSON_CONNECTION_STATUS_ATTENDEE ).Id );
        public static int VisitorConnectionStatusId = ( VisitorConnectionStatusIdOrNull != null ? VisitorConnectionStatusIdOrNull.Id : Extensions.AddDefinedValue( new RockContext(), Rock.SystemGuid.DefinedType.PERSON_CONNECTION_STATUS, "Visitor", Rock.SystemGuid.DefinedValue.PERSON_CONNECTION_STATUS_VISITOR ).Id );

        public static int ConnectionStatusTypeId = DefinedTypeCache.Get( Rock.SystemGuid.DefinedType.PERSON_CONNECTION_STATUS.AsGuid() ).Id;
        public static int ActivePersonRecordStatusId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_ACTIVE.AsGuid() ).Id;
        public static int InactivePersonRecordStatusId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_INACTIVE.AsGuid() ).Id;
        public static int PendingPersonRecordStatusId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_PENDING.AsGuid() ).Id;
        public static int DeceasedPersonRecordReasonId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_REASON_DECEASED.AsGuid() ).Id;
        public static int NoActivityPersonRecordReasonId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_REASON_NO_ACTIVITY.AsGuid() ).Id;

        public static int TransactionTypeContributionId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.TRANSACTION_TYPE_CONTRIBUTION.AsGuid() ).Id;
        public static int TransactionSourceTypeOnsiteId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.FINANCIAL_SOURCE_TYPE_ONSITE_COLLECTION.AsGuid() ).Id;
        public static int TransactionSourceTypeWebsiteId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.FINANCIAL_SOURCE_TYPE_WEBSITE.AsGuid() ).Id;
        public static int TransactionSourceTypeKioskId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.FINANCIAL_SOURCE_TYPE_KIOSK.AsGuid() ).Id;

        public static int GroupTypeMeetingLocationId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_MEETING_LOCATION.AsGuid() ).Id;
        public static int GroupTypeCheckinTemplateId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.GROUPTYPE_PURPOSE_CHECKIN_TEMPLATE.AsGuid() ).Id;
        public static int DeviceTypeCheckinKioskId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.DEVICE_TYPE_CHECKIN_KIOSK.AsGuid() ).Id;

        public static int BenevolenceApprovedStatusId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.BENEVOLENCE_APPROVED.AsGuid() ).Id;
        public static int BenevolenceDeniedStatusId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.BENEVOLENCE_DENIED.AsGuid() ).Id;
        public static int BenevolencePendingStatusId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.BENEVOLENCE_PENDING.AsGuid() ).Id;

        public static int FinancialAccountTypeDefindedTypeId = DefinedTypeCache.Get( Rock.SystemGuid.DefinedType.FINANCIAL_ACCOUNT_TYPE.AsGuid() ).Id;

        // Campus Types

        public static List<CampusCache> CampusList = CampusCache.All();

        // Note Types

        public static int PersonalNoteTypeId = NoteTypeCache.Get( Rock.SystemGuid.NoteType.PERSON_TIMELINE_NOTE.AsGuid() ).Id;
        public static int PrayerNoteTypeId = NoteTypeCache.Get( Rock.SystemGuid.NoteType.PRAYER_COMMENT.AsGuid() ).Id;

        // Relationship Types

        private static readonly GroupTypeRoleService groupTypeRoleService = new GroupTypeRoleService( new RockContext() );
        public static int FamilyAdultRoleId = groupTypeRoleService.Get( Rock.SystemGuid.GroupRole.GROUPROLE_FAMILY_MEMBER_ADULT.AsGuid() ).Id;
        public static int FamilyChildRoleId = groupTypeRoleService.Get( Rock.SystemGuid.GroupRole.GROUPROLE_FAMILY_MEMBER_CHILD.AsGuid() ).Id;

        public static int InviteeKnownRelationshipId = groupTypeRoleService.Get( Rock.SystemGuid.GroupRole.GROUPROLE_KNOWN_RELATIONSHIPS_INVITED.AsGuid() ).Id;
        public static int InvitedByKnownRelationshipId = groupTypeRoleService.Get( Rock.SystemGuid.GroupRole.GROUPROLE_KNOWN_RELATIONSHIPS_INVITED_BY.AsGuid() ).Id;
        public static int CanCheckInKnownRelationshipId = groupTypeRoleService.Get( Rock.SystemGuid.GroupRole.GROUPROLE_KNOWN_RELATIONSHIPS_CAN_CHECK_IN.AsGuid() ).Id;
        public static int AllowCheckInByKnownRelationshipId = groupTypeRoleService.Get( Rock.SystemGuid.GroupRole.GROUPROLE_KNOWN_RELATIONSHIPS_ALLOW_CHECK_IN_BY.AsGuid() ).Id;
        public static int KnownRelationshipOwnerRoleId = groupTypeRoleService.Get( Rock.SystemGuid.GroupRole.GROUPROLE_KNOWN_RELATIONSHIPS_OWNER.AsGuid() ).Id;
        public static int ImpliedRelationshipOwnerRoleId = groupTypeRoleService.Get( Rock.SystemGuid.GroupRole.GROUPROLE_PEER_NETWORK_OWNER.AsGuid() ).Id;
        public static Guid KnownRelationshipOwnerRoleGuid = groupTypeRoleService.Get( Rock.SystemGuid.GroupRole.GROUPROLE_KNOWN_RELATIONSHIPS_OWNER.AsGuid() ).Guid;

        // Category Types

        public static int AllChurchCategoryId = CategoryCache.Get( "5A94E584-35F0-4214-91F1-D72531CC6325".AsGuid() ).Id; // Prayer Parent Cagetory for All Church

        /// <summary>
        /// Parses the benevolence status.
        /// </summary>
        /// <param name="value">The status value ID.</param>
        /// <returns></returns>
        public static int? ParseBenevolenceStatus( string value )
        {
            switch ( value.Trim() )
            {
                case "Approved":
                case "Given":
                case "Provided":
                    return BenevolenceApprovedStatusId;

                case "Declined":
                case "Refused":
                case "Denied":
                    return BenevolenceDeniedStatusId;

                default:
                    return BenevolencePendingStatusId;
            }
        }
    }
}