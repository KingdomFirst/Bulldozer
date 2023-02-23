// <copyright>
// Copyright 2023 by Kingdom First Solutions
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
using System.Data.Entity;
using System.Linq;
using Bulldozer.Model;
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using static Bulldozer.CSV.CSVInstance;
using static Bulldozer.Utility.CachedTypes;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the People import methods
    /// </summary>
    partial class CSVComponent
    {
        /// <summary>
        /// Loads the Business data.
        /// </summary>
        private int ImportBusinesses()
        {
            this.ReportProgress( 0, "Preparing Business data for import..." );
            var businessImportList = GetBusinessImportList();


            this.ReportProgress( 0, string.Format( "Bulk Importing {0} Business Records...", businessImportList.Count ) );

            // Slice data into chunks and process
            var businessesRemainingToProcess = businessImportList.Count;
            var completed = 0;

            while ( businessesRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.PersonChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} Business records processed.", completed ) );
                }

                if ( completed % ( this.PersonChunkSize ) < 1 )
                {
                    var csvChunk = businessImportList.Take( Math.Min( this.PersonChunkSize, businessImportList.Count ) ).ToList();
                    completed += BulkBusinessImport( csvChunk );
                    businessesRemainingToProcess -= csvChunk.Count;
                    businessImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            return completed;
        }

        /// <summary>
        /// Gets the business import list.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception">personImport.PersonForeignId must be greater than 0
        /// or
        /// personImport.FamilyForeignId must be greater than 0 or null
        /// or</exception>
        private List<PersonImport> GetBusinessImportList()
        {
            var businessImportList = new List<PersonImport>();

            var familyRolesLookup = GroupTypeCache.GetFamilyGroupType().Roles.ToDictionary( k => k.Guid );

            int importCounter = 0;
            foreach ( var business in this.BusinessCsvList )
            {
                if ( business.Id.IsNullOrWhiteSpace() )
                {
                    LogException( "Business", string.Format( "Missing Id for Business '{0}'. Skipping.", business.Name ) );
                    continue;
                }
                importCounter++;
                var newBusiness = new PersonImport()
                {
                    RecordTypeValueId = this.PersonRecordTypeValuesDict[Rock.SystemGuid.DefinedValue.PERSON_RECORD_TYPE_BUSINESS.AsGuid()].Id,
                    PersonForeignKey = business.Id,
                    FamilyForeignKey = business.Id,
                    FamilyName = business.Name,
                    GroupRoleId = familyRolesLookup[Rock.SystemGuid.GroupRole.GROUPROLE_FAMILY_MEMBER_ADULT.AsGuid()].Id,
                    IsDeceased = false,
                    InactiveReasonNote = business.InactiveReasonNote.IsNullOrWhiteSpace() ? business.InactiveReason : business.InactiveReasonNote,
                    RecordStatusReasonValueId = this.RecordStatusReasonDVDict.Values.FirstOrDefault( v => v.Value.Equals( business.InactiveReason ) )?.Id,
                    LastName = business.Name,
                    Gender = Rock.Model.Gender.Unknown.ConvertToInt(),
                    Email = business.Email,
                    IsEmailActive = business.IsEmailActive.Value,
                    CreatedDateTime = business.CreatedDateTime.ToSQLSafeDate(),
                    ModifiedDateTime = business.ModifiedDateTime.ToSQLSafeDate(),
                    Note = business.Note,
                    GivingIndividually = false,
                    PhoneNumbers = new List<Model.PhoneNumberImport>(),
                    Addresses = new List<Model.PersonAddressImport>(),
                    AttributeValues = new List<Model.AttributeValueImport>()
                };

                if ( business.Campus != null )
                {
                    var campusIdInt = business.Campus.CampusId.AsIntegerOrNull();
                    if ( campusIdInt.HasValue && campusIdInt.Value > 0 )
                    {
                        newBusiness.CampusId = this.CampusList.FirstOrDefault( c => c.ForeignId == campusIdInt ).Id;
                    }
                    else if ( !campusIdInt.HasValue && business.Campus.CampusId.IsNotNullOrWhiteSpace() )
                    {
                        newBusiness.CampusId = this.CampusList.FirstOrDefault( c => c.ForeignKey == string.Format( "{0}_{1}", ImportInstanceFKPrefix, business.Campus.CampusId ) ).Id;
                    }
                    else if ( business.Campus.CampusName.IsNotNullOrWhiteSpace() )
                    {
                        newBusiness.CampusId = this.CampusList.FirstOrDefault( c => c.Name == business.Campus.CampusName ).Id;
                    }
                }

                switch ( business.RecordStatus )
                {
                    case CSVInstance.RecordStatus.Active:
                        newBusiness.RecordStatusValueId = this.RecordStatusDVDict[Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_ACTIVE.AsGuid()]?.Id;
                        break;

                    case CSVInstance.RecordStatus.Inactive:
                        newBusiness.RecordStatusValueId = this.RecordStatusDVDict[Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_INACTIVE.AsGuid()]?.Id;
                        break;

                    case CSVInstance.RecordStatus.Pending:
                        newBusiness.RecordStatusValueId = this.RecordStatusDVDict[Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_PENDING.AsGuid()]?.Id;
                        break;
                }

                switch ( business.EmailPreference )
                {
                    case EmailPreference.EmailAllowed:
                        newBusiness.EmailPreference = EmailPreference.EmailAllowed.ConvertToInt();
                        break;

                    case EmailPreference.DoNotEmail:
                        newBusiness.EmailPreference = EmailPreference.DoNotEmail.ConvertToInt();
                        break;

                    case EmailPreference.NoMassEmails:
                        newBusiness.EmailPreference = EmailPreference.NoMassEmails.ConvertToInt();
                        break;
                }

                // Phone Numbers
                foreach ( var businessPhone in business.PhoneNumbers )
                {
                    var newBusinessPhone = new PhoneNumberImport
                    {
                        NumberTypeValueId = this.PhoneNumberTypeDVDict[businessPhone.PhoneType].Id,
                        Number = businessPhone.PhoneNumber,
                        IsMessagingEnabled = businessPhone.IsMessagingEnabled ?? false,
                        IsUnlisted = businessPhone.IsUnlisted ?? false,
                        CountryCode = businessPhone.CountryCode
                    };

                    newBusiness.PhoneNumbers.Add( newBusinessPhone );
                }

                // Addresses
                foreach ( var businessAddress in business.Addresses )
                {
                    if ( string.IsNullOrEmpty( businessAddress.Street1 ) )
                    {
                        continue;
                    }
                    int? groupLocationTypeValueId = null;
                    switch ( businessAddress.AddressType )
                    {
                        case AddressType.Home:
                            groupLocationTypeValueId = this.GroupLocationTypeDVDict[Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_HOME.AsGuid()].Id;
                            break;

                        case AddressType.Previous:
                            groupLocationTypeValueId = this.GroupLocationTypeDVDict[Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_PREVIOUS.AsGuid()].Id;
                            break;

                        case AddressType.Work:
                            groupLocationTypeValueId = this.GroupLocationTypeDVDict[Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_WORK.AsGuid()].Id;
                            break;

                        case AddressType.Other:
                            groupLocationTypeValueId = this.GroupLocationTypeDVDict[Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_OTHER.AsGuid()].Id;
                            break;
                    }

                    if ( groupLocationTypeValueId.HasValue )
                    {
                        var newBusinessAddress = new PersonAddressImport
                        {
                            GroupLocationTypeValueId = groupLocationTypeValueId.Value,
                            IsMailingLocation = businessAddress.IsMailing,
                            IsMappedLocation = businessAddress.AddressType == AddressType.Home,
                            Street1 = businessAddress.Street1.Left( 100 ),
                            Street2 = businessAddress.Street2.Left( 100 ),
                            City = businessAddress.City.Left( 50 ),
                            State = businessAddress.State.Left( 50 ),
                            Country = businessAddress.Country.Left( 50 ),
                            PostalCode = businessAddress.PostalCode.Left( 50 ),
                            Latitude = businessAddress.Latitude.AsDoubleOrNull(),
                            Longitude = businessAddress.Longitude.AsDoubleOrNull()
                        };

                        newBusiness.Addresses.Add( newBusinessAddress );
                    }
                    else
                    {
                        LogException( "BusinessAddress", $"Unexpected Address Type ( {businessAddress.AddressType} ) for BusenessId { businessAddress.BusinessId }." );
                    }
                }

                // Attribute Values
                foreach ( var attributeValue in business.Attributes )
                {
                    int attributeId = this.PersonAttributeDict[attributeValue.AttributeKey].Id;
                    var newAttributeValue = new AttributeValueImport()
                    {
                        AttributeId = attributeId,
                        Value = attributeValue.AttributeValue,
                        AttributeValueForeignId = attributeValue.AttributeValueId.AsIntegerOrNull(),
                        AttributeValueForeignKey = string.Format( "{0}_{1}", this.ImportInstanceFKPrefix, attributeValue.AttributeValueId )
                    };
                    newBusiness.AttributeValues.Add( newAttributeValue );
                }

                businessImportList.Add( newBusiness );
            }

            return businessImportList;
        }

        /// <summary>
        /// Bulks the Business import.
        /// </summary>
        /// <param name="businessImports">The business imports.</param>
        /// <returns></returns>
        public int BulkBusinessImport( List<PersonImport> businessImports )
        {
            var rockContext = new RockContext();
            var qryAllPersons = new PersonService( rockContext ).Queryable( true, true );
            var groupService = new GroupService( rockContext );
            var groupMemberService = new GroupMemberService( rockContext );
            var locationService = new LocationService( rockContext );

            var recordTypeBusinessId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.PERSON_RECORD_TYPE_BUSINESS.AsGuid() ).Id;
            var familyGroupType = GroupTypeCache.GetFamilyGroupType();
            int familyGroupTypeId = familyGroupType.Id;

            var familiesLookup = groupService.Queryable().AsNoTracking().Where( a => a.GroupTypeId == familyGroupTypeId && a.ForeignId.HasValue && a.ForeignKey.IsNotNullOrWhiteSpace() && a.ForeignKey.Split( '_' )[0] == this.ImportInstanceFKPrefix )
                .ToList().ToDictionary( k => k.ForeignKey, v => v );

            var businessLookup = qryAllPersons.Include( a => a.PhoneNumbers ).AsNoTracking().Where( a => a.ForeignKey.IsNotNullOrWhiteSpace() && a.ForeignKey.Split( '_' )[0] == this.ImportInstanceFKPrefix && a.RecordTypeValueId == recordTypeBusinessId )
                .ToList().ToDictionary( k => k.ForeignKey, v => v );

            var defaultPhoneCountryCode = PhoneNumber.DefaultCountryCode();

            var importDateTime = RockDateTime.Now;

            int nextNewFamilyForeignId = familiesLookup.Any( a => a.Value.ForeignId.HasValue ) ? familiesLookup.Max( a => a.Value.ForeignId.Value ) : 0;
            if ( businessImports.Any() )
            {
                nextNewFamilyForeignId = Math.Max( nextNewFamilyForeignId, businessImports.Where( a => a.FamilyForeignId.HasValue ).Max( a => a.FamilyForeignId.Value ) );
            }

            EntityTypeAttributesCache.Clear();

            var entityTypeIdPerson = EntityTypeCache.Get<Person>().Id;
            var attributeValuesLookup = new AttributeValueService( rockContext ).Queryable().Where( a => a.Attribute.EntityTypeId == entityTypeIdPerson && a.EntityId.HasValue )
                .Select( a => new
                {
                    BusinessId = a.EntityId.Value,
                    a.AttributeId,
                    a.Value
                } )
                .GroupBy( a => a.BusinessId )
                .ToDictionary(
                    k => k.Key,
                    v => v.Select( x => new AttributeValueCache { AttributeId = x.AttributeId, EntityId = x.BusinessId, Value = x.Value } ).ToList() );

            int businessUpdatesCount = 0;
            int total = businessImports.Count();

            foreach ( var businessImport in businessImports )
            {
                Group family = null;

                if ( businessImport.FamilyForeignKey.IsNullOrWhiteSpace() )
                {
                    businessImport.FamilyForeignId = ++nextNewFamilyForeignId;
                    businessImport.FamilyForeignKey = string.Format( "{0}_{1}", this.ImportInstanceFKPrefix, nextNewFamilyForeignId );
                }

                if ( familiesLookup.ContainsKey( businessImport.FamilyForeignKey ) )
                {
                    family = familiesLookup[businessImport.FamilyForeignKey];
                }

                if ( family == null )
                {
                    family = new Group
                    {
                        GroupTypeId = familyGroupTypeId,
                        Name = string.IsNullOrEmpty( businessImport.FamilyName ) ? businessImport.LastName : businessImport.FamilyName,
                        CampusId = businessImport.CampusId,
                        ForeignId = businessImport.FamilyForeignId,
                        ForeignKey = businessImport.FamilyForeignKey,
                        CreatedDateTime = businessImport.CreatedDateTime.ToSQLSafeDate() ?? importDateTime,
                        ModifiedDateTime = businessImport.ModifiedDateTime.ToSQLSafeDate() ?? importDateTime
                    };

                    if ( string.IsNullOrWhiteSpace( family.Name ) )
                    {
                        family.Name = "Family";
                    }
                    familiesLookup.Add( businessImport.FamilyForeignKey, family );
                }

                Person business = null;
                if ( businessLookup.ContainsKey( businessImport.PersonForeignKey ) )
                {
                    business = businessLookup[businessImport.PersonForeignKey];
                }

                if ( business == null )
                {
                    business = new Person();
                    InitializeBusinessFromPersonImport( businessImport, business, recordTypeBusinessId );
                    businessLookup.Add( businessImport.PersonForeignKey, business );
                }
                else if ( this.ImportUpdateOption == ImportUpdateType.AlwaysUpdate )
                {
                    bool wasChanged = UpdatePersonFromPersonImport( business, businessImport, attributeValuesLookup, familiesLookup, importDateTime, recordTypeBusinessId );
                    if ( wasChanged )
                    {
                        businessUpdatesCount++;
                    }
                }
            }
            var insertedBusinessesForeignIds = new List<string>();

            // insert all the [Group] records
            var familiesToInsert = familiesLookup.Where( a => a.Value.Id == 0 ).Select( a => a.Value ).ToList();

            // insert all the [Person] records.
            var businessesToInsert = businessLookup.Where( a => a.Value.Id == 0 ).Select( a => a.Value ).ToList();

            rockContext.BulkInsert( familiesToInsert );

            // lookup GroupId from Group.ForeignId
            var familyIdLookup = groupService.Queryable().AsNoTracking().Where( a => a.GroupTypeId == familyGroupTypeId && a.ForeignKey.IsNotNullOrWhiteSpace() && a.ForeignKey.Split( '_' )[0] == this.ImportInstanceFKPrefix )
                .ToList().ToDictionary( k => k.ForeignKey, v => v.Id );

            var businessToInsertLookup = businessesToInsert.ToDictionary( k => k.ForeignKey, v => v );

            // now that we have GroupId for each family, set the GivingGroupId for personImport's that don't give individually
            foreach ( var businessImport in businessImports )
            {
                if ( !businessImport.GivingIndividually.HasValue )
                {
                    // If GivingIndividually is NULL, Set it to false
                    businessImport.GivingIndividually = false;
                }

                if ( !businessImport.GivingIndividually.Value && businessImport.FamilyForeignId.HasValue )
                {
                    var businessToInsert = businessToInsertLookup.GetValueOrNull( businessImport.PersonForeignKey );
                    if ( businessToInsert != null )
                    {
                        businessToInsert.GivingGroupId = familyIdLookup[businessImport.FamilyForeignKey];
                    }
                }
            }

            rockContext.BulkInsert( businessesToInsert );

            insertedBusinessesForeignIds = businessesToInsert.Select( a => a.ForeignKey ).ToList();

            // Make sure everybody has a PersonAlias
            PersonAliasService personAliasService = new PersonAliasService( rockContext );
            var personAliasServiceQry = personAliasService.Queryable();
            var businessAliasesToInsert = qryAllPersons.Where( p => p.ForeignKey.IsNotNullOrWhiteSpace() && p.ForeignKey.Split( '_' )[0] == this.ImportInstanceFKPrefix && !p.Aliases.Any() && !personAliasServiceQry.Any( pa => pa.AliasPersonId == p.Id ) )
                                                     .Select( x => new { x.Id, x.Guid, x.ForeignId, x.ForeignKey } )
                                                     .ToList()
                                                     .Select( person => new PersonAlias { AliasPersonId = person.Id, AliasPersonGuid = person.Guid, PersonId = person.Id, ForeignId = person.ForeignId, ForeignKey = person.ForeignKey } ).ToList();

            rockContext.BulkInsert( businessAliasesToInsert );

            var familyGroupMembersQry = new GroupMemberService( rockContext ).Queryable( true ).Where( a => a.Group.GroupTypeId == familyGroupTypeId );

            // get the person Ids along with the PersonImport and GroupMember record
            var businessIds = from p in qryAllPersons.AsNoTracking().Where( a => a.ForeignKey.IsNotNullOrWhiteSpace() && a.ForeignKey.Split( '_' )[0] == this.ImportInstanceFKPrefix && a.RecordTypeValueId == recordTypeBusinessId )
                                .Select( a => new { a.Id, a.ForeignKey } ).ToList()
                             join pi in businessImports on p.ForeignKey equals pi.PersonForeignKey
                             join f in groupService.Queryable().Where( a => a.ForeignKey.IsNotNullOrWhiteSpace() && a.ForeignKey.Split( '_' )[0] == this.ImportInstanceFKPrefix && a.GroupTypeId == familyGroupTypeId )
                             .Select( a => new { a.Id, a.ForeignKey } ).ToList() on pi.FamilyForeignKey equals f.ForeignKey
                             join gm in familyGroupMembersQry.Select( a => new { a.Id, a.PersonId } ) on p.Id equals gm.PersonId into gmj
                             from gm in gmj.DefaultIfEmpty()
                             select new
                             {
                                 BusinessId = p.Id,
                                 BusinessImport = pi,
                                 FamilyId = f.Id,
                                 HasGroupMemberRecord = gm != null
                             };

            // narrow it down to just person records that we inserted
            var businessIdsForBusinessImport = businessIds.Where( a => insertedBusinessesForeignIds.Contains( a.BusinessImport.PersonForeignKey ) );

            // Make the GroupMember records for all the imported person (unless they are already have a groupmember record for the family)
            var groupMemberRecordsToInsertQry = from ppi in businessIdsForBusinessImport
                                                where !ppi.HasGroupMemberRecord
                                                select new GroupMember
                                                {
                                                    PersonId = ppi.BusinessId,
                                                    GroupRoleId = ppi.BusinessImport.GroupRoleId,
                                                    GroupId = ppi.FamilyId,
                                                    GroupTypeId = GroupTypeCache.Get( Rock.SystemGuid.GroupType.GROUPTYPE_FAMILY.AsGuid() ).Id,
                                                    GroupMemberStatus = GroupMemberStatus.Active,
                                                    CreatedDateTime = ppi.BusinessImport.CreatedDateTime.ToSQLSafeDate() ?? importDateTime,
                                                    ModifiedDateTime = ppi.BusinessImport.ModifiedDateTime.ToSQLSafeDate() ?? importDateTime,
                                                };

            var groupMemberRecordsToInsertList = groupMemberRecordsToInsertQry.ToList();

            rockContext.BulkInsert( groupMemberRecordsToInsertList );

            var locationsToInsert = new List<Location>();
            var groupLocationsToInsert = new List<GroupLocation>();

            var locationCreatedDateTimeStart = RockDateTime.Now;

            foreach ( var familyRecord in businessIdsForBusinessImport.GroupBy( a => a.FamilyId ) )
            {
                // get the distinct addresses for each family in our import
                var familyAddresses = familyRecord.Where( a => a.BusinessImport?.Addresses != null ).SelectMany( a => a.BusinessImport.Addresses ).DistinctBy( a => new { a.GroupLocationTypeValueId, a.Street1, a.Street2, a.City, a.County, a.State } ).ToList();

                foreach ( var address in familyAddresses )
                {
                    Location location = new Location
                    {
                        Street1 = address.Street1.Left( 100 ),
                        Street2 = address.Street2.Left( 100 ),
                        City = address.City.Left( 50 ),
                        County = address.County.Left( 50 ),
                        State = address.State.Left( 50 ),
                        Country = address.Country.Left( 50 ),
                        PostalCode = address.PostalCode.Left( 50 ),
                        CreatedDateTime = locationCreatedDateTimeStart,
                        ModifiedDateTime = locationCreatedDateTimeStart,
                        Guid = Guid.NewGuid() // give the Location a Guid, and store a reference to which Location is associated with the GroupLocation record. Then we'll match them up later and do the bulk insert
                    };

                    if ( address.Latitude.HasValue && address.Longitude.HasValue )
                    {
                        location.SetLocationPointFromLatLong( address.Latitude.Value, address.Longitude.Value );
                    }

                    var newGroupLocation = new GroupLocation
                    {
                        GroupLocationTypeValueId = address.GroupLocationTypeValueId,
                        GroupId = familyRecord.Key,
                        IsMailingLocation = address.IsMailingLocation,
                        IsMappedLocation = address.IsMappedLocation,
                        CreatedDateTime = locationCreatedDateTimeStart,
                        ModifiedDateTime = locationCreatedDateTimeStart,
                        Location = location
                    };

                    groupLocationsToInsert.Add( newGroupLocation );
                    locationsToInsert.Add( newGroupLocation.Location );
                }
            }

            rockContext.BulkInsert( locationsToInsert );

            var locationIdLookup = locationService.Queryable().Select( a => new { a.Id, a.Guid } ).ToList().ToDictionary( k => k.Guid, v => v.Id );
            foreach ( var groupLocation in groupLocationsToInsert )
            {
                groupLocation.LocationId = locationIdLookup[groupLocation.Location.Guid];
            }

            rockContext.BulkInsert( groupLocationsToInsert );

            // PhoneNumbers
            var phoneNumbersToInsert = new List<PhoneNumber>();

            foreach ( var businessesIds in businessIdsForBusinessImport )
            {
                foreach ( var phoneNumberImport in businessesIds.BusinessImport.PhoneNumbers )
                {

                    var newPhoneNumber = new PhoneNumber();
                    newPhoneNumber.PersonId = businessesIds.BusinessId;
                    UpdatePhoneNumberFromPhoneNumberImport( phoneNumberImport, newPhoneNumber, importDateTime );
                    phoneNumbersToInsert.Add( newPhoneNumber );
                }
            }

            rockContext.BulkInsert( phoneNumbersToInsert );

            // Attribute Values
            var attributeValuesToInsert = new List<AttributeValue>();
            foreach ( var businessesIds in businessIdsForBusinessImport )
            {
                foreach ( var attributeValueImport in businessesIds.BusinessImport.AttributeValues )
                {
                    var newAttributeValue = new AttributeValue
                    {
                        EntityId = businessesIds.BusinessId,
                        AttributeId = attributeValueImport.AttributeId,
                        Value = attributeValueImport.Value,
                        CreatedDateTime = businessesIds.BusinessImport.CreatedDateTime.ToSQLSafeDate() ?? importDateTime,
                        ModifiedDateTime = businessesIds.BusinessImport.ModifiedDateTime.ToSQLSafeDate() ?? importDateTime,
                        ForeignId = attributeValueImport.AttributeValueForeignId,
                        ForeignKey = attributeValueImport.AttributeValueForeignKey
                    };
                    attributeValuesToInsert.Add( newAttributeValue );
                }
            }

            // WARNING:  Using BulkInsert on AttributeValues will circumvent tgrAttributeValue_InsertUpdate trigger, so
            // AttributeValueService.UpdateAllValueAsDateTimeFromTextValue() should be executed before we're done.
            rockContext.BulkInsert( attributeValuesToInsert );

            // since we bypassed Rock SaveChanges when Inserting Person records, sweep thru and ensure the AgeClassification, PrimaryFamily, and GivingLeaderId is set
            PersonService.UpdatePersonAgeClassificationAll( rockContext );
            PersonService.UpdatePrimaryFamilyAll( rockContext );
            PersonService.UpdateGivingLeaderIdAll( rockContext );

            return businessesToInsert.Count;
        }
    }
}