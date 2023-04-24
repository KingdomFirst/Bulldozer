using Bulldozer.Model;
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bulldozer.CSV
{
    partial class CSVComponent : BulldozerComponent
    {

        /// <summary>
        /// Updates the person from person import and returns whether there were any changes to the person record
        /// </summary>
        /// <param name="lookupPerson">The lookup person.</param>
        /// <param name="personImport">The person import.</param>
        /// <param name="attributeValuesLookup">The attribute values lookup.</param>
        /// <param name="familiesLookup">The families lookup.</param>
        /// <param name="foreignSystemKey">The foreign system key.</param>
        /// <param name="importDateTime">The import date time.</param>
        /// <param name="recordTypeId">The RecordType Id to use.</param>
        /// <returns></returns>
        private bool UpdatePersonFromPersonImport( Person lookupPerson, PersonImport personImport, Dictionary<int, List<AttributeValueCache>> attributeValuesLookup, Dictionary<string, Group> familiesLookup, DateTime importDateTime, int recordTypeId )
        {
            using ( var rockContextForPersonUpdate = new RockContext() )
            {
                rockContextForPersonUpdate.People.Attach( lookupPerson );
                var person = lookupPerson;

                // Update Person
                InitializePersonFromPersonImport( personImport, person, recordTypeId );

                // Update Phone Numbers
                var phoneNumberService = new PhoneNumberService( rockContextForPersonUpdate );
                var personPhoneNumberList = person.PhoneNumbers.Select( a => new
                {
                    a.Id,
                    a.Number
                } ).ToList();

                foreach ( var phoneNumberImport in personImport.PhoneNumbers )
                {
                    var hasPhoneNumber = personPhoneNumberList.Any( a => a.Number == PhoneNumber.CleanNumber( phoneNumberImport.Number ) );
                    if ( !hasPhoneNumber )
                    {
                        var newPhoneNumber = new PhoneNumber();
                        newPhoneNumber.PersonId = person.Id;
                        UpdatePhoneNumberFromPhoneNumberImport( phoneNumberImport, newPhoneNumber, importDateTime );
                        phoneNumberService.Add( newPhoneNumber );
                    }
                }

                // Remove any phonenumbers that are no longer in the PersonImport.PhoneNumbers list
                foreach ( var phone in personPhoneNumberList.Where( a => !personImport.PhoneNumbers.Any( x => PhoneNumber.CleanNumber( x.Number ) == a.Number ) ) )
                {
                    var personPhoneNumber = phoneNumberService.Get( phone.Id );
                    if ( personPhoneNumber != null )
                    {
                        phoneNumberService.Delete( personPhoneNumber );
                    }
                }

                var personAttributesUpdated = false;
                if ( personImport.AttributeValues.Any() )
                {
                    var attributeValues = attributeValuesLookup.GetValueOrNull( person.Id );

                    foreach ( AttributeValueImport attributeValueImport in personImport.AttributeValues )
                    {
                        var currentValue = attributeValues?.FirstOrDefault( a => a.AttributeId == attributeValueImport.AttributeId );

                        if ( ( currentValue == null ) || ( currentValue.Value != attributeValueImport.Value ) )
                        {
                            if ( person.Attributes == null )
                            {
                                person.LoadAttributes( rockContextForPersonUpdate );
                            }

                            var attributeCache = AttributeCache.Get( attributeValueImport.AttributeId );
                            if ( person.AttributeValues[attributeCache.Key].Value != attributeValueImport.Value )
                            {
                                person.SetAttributeValue( attributeCache.Key, attributeValueImport.Value );
                                personAttributesUpdated = true;
                            }
                        }
                    }
                }

                // update Addresses
                var addressesUpdated = false;
                if ( personImport.Addresses.Any() )
                {
                    var primaryFamily = familiesLookup.GetValueOrNull( personImport.FamilyForeignKey ?? string.Empty );

                    if ( primaryFamily != null )
                    {
                        // Import fails if re-importing a person who has addresses but is not assigned to a family. When initially imported,
                        // Rock creates a family group for these people and we need to locate the ID before checking for matching locations.
                        if ( primaryFamily.Id < 1 )
                        {
                            if ( person.PrimaryFamilyId.HasValue )
                            {
                                primaryFamily.Id = person.PrimaryFamilyId.Value;
                            }
                        }

                        var groupLocationService = new GroupLocationService( rockContextForPersonUpdate );
                        var primaryFamilyGroupLocations = groupLocationService.Queryable().Where( a => a.GroupId == primaryFamily.Id ).Include( a => a.Location ).AsNoTracking().ToList();
                        foreach ( var personAddressImport in personImport.Addresses )
                        {
                            bool addressAlreadyExistsExactMatch = primaryFamilyGroupLocations.Where( a =>
                                 a.GroupLocationTypeValueId == personAddressImport.GroupLocationTypeValueId
                                 && (
                                    a.Location.Street1 == personAddressImport.Street1
                                    && a.Location.Street2 == personAddressImport.Street2
                                    && a.Location.City == personAddressImport.City
                                    && a.Location.County == personAddressImport.County
                                    && a.Location.State == personAddressImport.State
                                    && a.Location.Country == personAddressImport.Country
                                    && a.Location.PostalCode == personAddressImport.PostalCode
                                 ) ).Any();

                            if ( !addressAlreadyExistsExactMatch )
                            {
                                var locationService = new LocationService( rockContextForPersonUpdate );

                                Location location = locationService.Get( personAddressImport.Street1, personAddressImport.Street2, personAddressImport.City, personAddressImport.State, personAddressImport.PostalCode, personAddressImport.Country, false );

                                if ( !primaryFamilyGroupLocations.Where( a => a.GroupLocationTypeValueId == personAddressImport.GroupLocationTypeValueId && a.LocationId == location.Id ).Any() )
                                {
                                    var groupLocation = new GroupLocation();
                                    groupLocation.GroupId = primaryFamily.Id;
                                    groupLocation.GroupLocationTypeValueId = personAddressImport.GroupLocationTypeValueId;
                                    groupLocation.IsMailingLocation = personAddressImport.IsMailingLocation;
                                    groupLocation.IsMappedLocation = personAddressImport.IsMappedLocation;

                                    if ( location.GeoPoint == null && personAddressImport.Latitude.HasValue && personAddressImport.Longitude.HasValue )
                                    {
                                        location.SetLocationPointFromLatLong( personAddressImport.Latitude.Value, personAddressImport.Longitude.Value );
                                    }

                                    groupLocation.LocationId = location.Id;
                                    groupLocationService.Add( groupLocation );

                                    addressesUpdated = true;
                                }
                            }
                        }
                    }
                }

                if ( personAttributesUpdated )
                {
                    person.SaveAttributeValues();
                }

                var updatedRecords = rockContextForPersonUpdate.SaveChanges( true );

                return addressesUpdated || personAttributesUpdated || updatedRecords > 0;
            }
        }

        /// <summary>
        /// Updates the person properties from person import.
        /// </summary>
        /// <param name="personImport">The person import.</param>
        /// <param name="person">The person.</param>
        /// <param name="recordTypePersonId">The Id for the Person RecordType.</param>
        private string InitializePersonFromPersonImport( PersonImport personImport, Person person, int recordTypePersonId )
        {
            var errors = string.Empty;
            person.RecordTypeValueId = personImport.RecordTypeValueId ?? recordTypePersonId;
            person.RecordStatusValueId = personImport.RecordStatusValueId;
            person.RecordStatusLastModifiedDateTime = personImport.RecordStatusLastModifiedDateTime.ToSQLSafeDate();
            person.RecordStatusReasonValueId = personImport.RecordStatusReasonValueId;
            person.ConnectionStatusValueId = personImport.ConnectionStatusValueId;
            person.ReviewReasonValueId = personImport.ReviewReasonValueId;
            person.IsDeceased = personImport.IsDeceased;
            person.TitleValueId = personImport.TitleValueId;
            person.FirstName = personImport.FirstName.FixCase().Left( 50 );
            person.NickName = personImport.NickName.FixCase().Left( 50 );

            if ( person.NickName.IsNullOrWhiteSpace() )
            {
                person.NickName = person.FirstName.Left( 50 );
            }

            if ( person.FirstName.IsNullOrWhiteSpace() )
            {
                person.FirstName = person.NickName.Left( 50 );
            }

            person.MiddleName = personImport.MiddleName.FixCase().Left( 50 );
            person.LastName = personImport.LastName.FixCase().Left( 50 );
            person.SuffixValueId = personImport.SuffixValueId;
            person.BirthDay = personImport.BirthDay;
            person.BirthMonth = personImport.BirthMonth;
            person.BirthYear = personImport.BirthYear;
            person.Gender = ( Gender ) personImport.Gender;
            person.MaritalStatusValueId = personImport.MaritalStatusValueId;
            person.AnniversaryDate = personImport.AnniversaryDate.ToSQLSafeDate();
            person.GraduationYear = personImport.GraduationYear;
            person.Email = personImport.Email.IsNotNullOrWhiteSpace() ? personImport.Email.Left( 75 ) : null;
            person.InactiveReasonNote = personImport.InactiveReasonNote.Left( 1000 );
            person.CreatedDateTime = personImport.CreatedDateTime.ToSQLSafeDate();
            person.ModifiedDateTime = personImport.ModifiedDateTime.ToSQLSafeDate();
            person.SystemNote = personImport.Note;
            person.ForeignId = personImport.PersonForeignId;
            person.ForeignKey = personImport.PersonForeignKey;

            // validate email or Rock will kick it back
            if ( person.Email != null && person.Email.IsValidEmail() && person.Email.IsEmail( this.EmailRegex ) )
            {
                person.IsEmailActive = person.IsEmailActive;
                person.EmailNote = person.EmailNote.Left( 250 );
                person.EmailPreference = ( EmailPreference ) person.EmailPreference;
            }
            else if ( person.Email != null )
            {
                errors += string.Format( "{0},{1},\"{2}\"\r\n", DateTime.Now.ToString(), "InvalidPersonEmail", $"PersonId: {person.ForeignKey.Substring( person.ForeignKey.IndexOf( "_" ) + 1 )} - Email: {person.Email}" );
                person.Email = null;
            }
            return errors;
        }

        /// <summary>
        /// Updates the phone number from phone number import.
        /// </summary>
        /// <param name="phoneNumberImport">The phone number import.</param>
        /// <param name="phoneNumberToInsert">The phone number to insert.</param>
        /// <param name="importDateTime">The import date time.</param>
        private void UpdatePhoneNumberFromPhoneNumberImport( PhoneNumberImport phoneNumberImport, PhoneNumber phoneNumberToInsert, DateTime importDateTime )
        {
            phoneNumberToInsert.NumberTypeValueId = phoneNumberImport.NumberTypeValueId;
            phoneNumberToInsert.CountryCode = phoneNumberImport.CountryCode?.ToString();
            phoneNumberToInsert.Number = PhoneNumber.CleanNumber( phoneNumberImport.Number );
            phoneNumberToInsert.NumberFormatted = PhoneNumber.FormattedNumber( phoneNumberToInsert.CountryCode, phoneNumberToInsert.Number );
            phoneNumberToInsert.Extension = phoneNumberImport.Extension;
            phoneNumberToInsert.IsMessagingEnabled = phoneNumberImport.IsMessagingEnabled;
            phoneNumberToInsert.IsUnlisted = phoneNumberImport.IsUnlisted;
            phoneNumberToInsert.CreatedDateTime = importDateTime;
            phoneNumberToInsert.ModifiedDateTime = importDateTime;
            phoneNumberToInsert.ForeignKey = this.ImportInstanceFKPrefix + "_" + phoneNumberImport.PhoneId;
            phoneNumberToInsert.ForeignId = phoneNumberImport.PhoneId.AsIntegerOrNull();
        }

        /// <summary>
        /// Updates the business properties from business import.
        /// </summary>
        /// <param name="businessImport">The person import.</param>
        /// <param name="business">The person.</param>
        /// <param name="recordTypeBusinessId">The Id for the Business RecordType.</param>
        private string InitializeBusinessFromPersonImport( PersonImport businessImport, Person business, int recordTypeBusinessId, string emailErrors = "" )
        {
            business.RecordTypeValueId = businessImport.RecordTypeValueId ?? recordTypeBusinessId;
            business.RecordStatusValueId = businessImport.RecordStatusValueId;
            business.RecordStatusLastModifiedDateTime = businessImport.RecordStatusLastModifiedDateTime.ToSQLSafeDate();
            business.RecordStatusReasonValueId = businessImport.RecordStatusReasonValueId;
            business.ConnectionStatusValueId = businessImport.ConnectionStatusValueId;
            business.ReviewReasonValueId = businessImport.ReviewReasonValueId;
            business.IsDeceased = businessImport.IsDeceased;
            business.LastName = businessImport.LastName.FixCase().Left( 50 );
            business.Gender = ( Gender ) businessImport.Gender;
            business.Email = businessImport.Email.IsNotNullOrWhiteSpace() ? businessImport.Email.Left( 75 ) : null;

            // validate email or Rock will kick it back
            if ( business.Email != null && business.Email.IsValidEmail() && business.Email.IsEmail( this.EmailRegex ) )
            {
                business.IsEmailActive = businessImport.IsEmailActive;
                business.EmailNote = businessImport.EmailNote.Left( 250 );
                business.EmailPreference = ( EmailPreference ) businessImport.EmailPreference;
            }
            else if ( business.Email != null )
            {
                emailErrors += string.Format( "{0},{1},\"{2}\"\r\n", DateTime.Now.ToString(), "InvalidBusinessEmail", $"BusinessId: {businessImport.PersonForeignKey.Substring( businessImport.PersonForeignKey.IndexOf( "_" ) + 1 )} - Email: {businessImport.Email}" );
                business.Email = null;
            }
            business.InactiveReasonNote = businessImport.InactiveReasonNote.Left( 1000 );
            business.CreatedDateTime = businessImport.CreatedDateTime.ToSQLSafeDate();
            business.ModifiedDateTime = businessImport.ModifiedDateTime.ToSQLSafeDate();
            business.SystemNote = businessImport.Note;
            business.ForeignId = businessImport.PersonForeignId;
            business.ForeignKey = businessImport.PersonForeignKey;

            return emailErrors;
        }


        private void InitializeGroupFromGroupImport( Group group, GroupImport groupImport, DateTime importedDateTime )
        {
            group.ForeignId = groupImport.GroupForeignId;
            group.ForeignKey = groupImport.GroupForeignKey;
            group.GroupTypeId = groupImport.GroupTypeId;

            if ( groupImport.Name.Length > 100 )
            {
                group.Name = groupImport.Name.Left( 100 );
                group.Description = groupImport.Name;
            }
            else
            {
                group.Name = groupImport.Name;
                group.Description = groupImport.Description;
            }

            if ( groupImport.CreatedDate.HasValue )
            {
                group.CreatedDateTime = groupImport.CreatedDate;
            }

            group.Order = groupImport.Order.GetValueOrDefault();
            group.CampusId = groupImport.CampusId;
            group.IsActive = groupImport.IsActive;
            group.IsPublic = groupImport.IsPublic;
            group.GroupCapacity = groupImport.Capacity;
            group.Guid = Guid.NewGuid();
        }

        //private bool UpdateGroupFromGroupImport( GroupImport groupImport, Group lookupGroup, Dictionary<int, List<AttributeValueCache>> attributeValuesLookup, DateTime importDateTime )
        private bool UpdateGroupFromGroupImport( GroupImport groupImport, Group lookupGroup, DateTime importDateTime )
        {
            using ( var rockContextForGroupUpdate = new RockContext() )
            {
                rockContextForGroupUpdate.Groups.Attach( lookupGroup );
                var group = lookupGroup;

                InitializeGroupFromGroupImport( group, groupImport, importDateTime );

                // update Attributes
                //var groupAttributesUpdated = false;
                //if ( groupImport.AttributeValues.Any() )
                //{
                //    var attributeValues = attributeValuesLookup.GetValueOrNull( group.Id );

                //    foreach ( var attributeValueImport in groupImport.AttributeValues )
                //    {
                //        var currentValue = attributeValues?.FirstOrDefault( a => a.AttributeId == attributeValueImport.AttributeId );

                //        if ( ( currentValue == null ) || ( currentValue.Value != attributeValueImport.Value ) )
                //        {
                //            if ( group.Attributes == null )
                //            {
                //                group.LoadAttributes( rockContextForGroupUpdate );
                //            }

                //            var attributeCache = AttributeCache.All().FirstOrDefault( ac => ac.ForeignKey == string.Format( "{0}^{1}", ImportInstanceFKPrefix, attributeValueImport.AttributeId ) );
                //            if ( group.AttributeValues[attributeCache.Key].Value != attributeValueImport.Value )
                //            {
                //                group.SetAttributeValue( attributeCache.Key, attributeValueImport.Value );
                //                groupAttributesUpdated = true;
                //            }
                //        }
                //    }
                //}

                // update Addresses
                //var addressesUpdated = false;
                //if ( groupImport.Addresses.Any() )
                //{
                //    var groupLocationService = new GroupLocationService( rockContextForGroupUpdate );
                //    var groupLocations = groupLocationService.Queryable().Where( a => a.GroupId == group.Id ).Include( a => a.Location ).AsNoTracking().ToList();
                //    foreach ( var groupAddressImport in groupImport.Addresses )
                //    {
                //        bool addressAlreadyExsistsExactMatch = groupLocations.Where( a =>
                //            a.GroupLocationTypeValueId == groupAddressImport.GroupLocationTypeValueId
                //            && (
                //                 a.Location.Street1 == groupAddressImport.Street1
                //                    && a.Location.Street2 == groupAddressImport.Street2
                //                    && a.Location.City == groupAddressImport.City
                //                    && a.Location.County == groupAddressImport.County
                //                    && a.Location.State == groupAddressImport.State
                //                    && a.Location.Country == groupAddressImport.Country
                //                    && a.Location.PostalCode == groupAddressImport.PostalCode
                //                 ) ).Any();

                //        if ( !addressAlreadyExsistsExactMatch )
                //        {
                //            var locationService = new LocationService( rockContextForGroupUpdate );

                //            Location location = locationService.Get( groupAddressImport.Street1, groupAddressImport.Street2, groupAddressImport.City, groupAddressImport.State, groupAddressImport.PostalCode, groupAddressImport.Country, false );

                //            if ( !groupLocations.Where( a => a.GroupLocationTypeValueId == groupAddressImport.GroupLocationTypeValueId && a.LocationId == location.Id ).Any() )
                //            {
                //                var groupLocation = new GroupLocation();
                //                groupLocation.GroupId = group.Id;
                //                groupLocation.GroupLocationTypeValueId = groupAddressImport.GroupLocationTypeValueId;
                //                groupLocation.IsMailingLocation = groupAddressImport.IsMailingLocation;
                //                groupLocation.IsMappedLocation = groupAddressImport.IsMappedLocation;

                //                if ( location.GeoPoint == null && groupAddressImport.Latitude.HasValue && groupAddressImport.Longitude.HasValue )
                //                {
                //                    location.SetLocationPointFromLatLong( groupAddressImport.Latitude.Value, groupAddressImport.Longitude.Value );
                //                }

                //                groupLocation.LocationId = location.Id;
                //                groupLocationService.Add( groupLocation );

                //                addressesUpdated = true;
                //            }
                //        }
                //    }
                //}

                // update schedule
                bool scheduleUpdated = false;
                DayOfWeek meetingDay;
                if ( groupImport.MeetingDay.IsNullOrWhiteSpace() && Enum.TryParse( groupImport.MeetingDay, out meetingDay ) )
                {
                    TimeSpan meetingTime;
                    TimeSpan.TryParse( groupImport.MeetingTime, out meetingTime );
                    if ( group.Schedule.WeeklyDayOfWeek != meetingDay || group.Schedule.WeeklyTimeOfDay != meetingTime )
                    {
                        group.Schedule = new Schedule()
                        {
                            Name = group.Name,
                            IsActive = group.IsActive,
                            WeeklyDayOfWeek = meetingDay,
                            WeeklyTimeOfDay = meetingTime,
                            ForeignId = groupImport.GroupForeignId,
                            ForeignKey = groupImport.GroupForeignKey,
                            CreatedDateTime = importDateTime,
                            ModifiedDateTime = importDateTime
                        };
                        scheduleUpdated = true;
                    }
                }

                //if ( groupAttributesUpdated )
                //{
                //    group.SaveAttributeValues();
                //}

                //Update Members

                //var groupMemberService = new GroupMemberService( rockContextForGroupUpdate );
                //var personIdLookup = new PersonService( rockContextForGroupUpdate ).Queryable().Where( a => !string.IsNullOrEmpty( a.ForeignKey ) && a.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                //     .Select( a => new { a.Id, ForeignKey = a.ForeignKey } ).ToDictionary( k => k.ForeignKey, v => v.Id );

                //var groupMemberList = group.Members.Where( x => x.Person.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) ).Select( a => new
                //{
                //    a.Id,
                //    a.Person.ForeignKey
                //} ).ToList();

                // populate/update GroupMembers
                //foreach ( var groupMemberImport in groupImport.GroupMembers )
                //{
                //    var personId = personIdLookup.GetValueOrNull( groupMemberImport.PersonForeignKey );
                //    if ( personId == null )
                //    {
                //        continue;
                //    }

                //    var groupTypeRoleLookup = GroupTypeCache.Get( groupImport.GroupTypeId ).Roles.ToDictionary( k => k.Name, v => v.Id );
                //    var groupRoleId = groupTypeRoleLookup.GetValueOrNull( groupMemberImport.RoleName );

                //    GroupMember groupMember = group.Members.Where( m => m.Person.ForeignKey == groupMemberImport.PersonForeignKey ).FirstOrDefault();

                //    if ( groupMember == null )
                //    {
                //        groupMember = new GroupMember();
                //        groupMember.GroupId = group.Id;
                //        groupMember.GroupRoleId = groupRoleId.Value;
                //        groupMember.GroupTypeId = groupImport.GroupTypeId;
                //        groupMember.PersonId = personId.Value;
                //        groupMember.CreatedDateTime = groupMemberImport.CreatedDate.HasValue ? groupMemberImport.CreatedDate.Value : importDateTime;
                //        groupMember.ModifiedDateTime = importDateTime;
                //        groupMember.ForeignKey = groupMemberImport.GroupMemberForeignKey;
                //        groupMember.GroupMemberStatus = groupMemberImport.GroupMemberStatus;
                //        groupMemberService.Add( groupMember );
                //    }
                //    else
                //    {
                //        groupMember.GroupRoleId = groupRoleId.Value;
                //        groupMember.ModifiedDateTime = importDateTime;
                //    }
                //}

                //foreach ( var member in groupMemberList.Where( gm => !groupImport.GroupMembers.Any( x => x.PersonForeignKey == gm.ForeignKey ) ) )
                //{
                //    var groupMember = groupMemberService.Get( member.Id );
                //    if ( groupMember != null )
                //    {
                //        groupMemberService.Delete( groupMember );
                //    }
                //}

                var updatedRecords = rockContextForGroupUpdate.SaveChanges( true );

                //return scheduleUpdated || addressesUpdated || groupAttributesUpdated || updatedRecords > 0;
                return scheduleUpdated || updatedRecords > 0;
            }
        }
    }
}
