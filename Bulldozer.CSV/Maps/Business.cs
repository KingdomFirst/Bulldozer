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
using static Bulldozer.Utility.Extensions;
using static Bulldozer.Utility.CachedTypes;

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
            ReportProgress( 0, "Preparing Business data for import..." );

            // Let's deal with family groups first
            var familiesToCreate = new List<Group>();
            var rockContext = new RockContext();
            var importDateTime = RockDateTime.Now;

            var errors = string.Empty;
            var families = this.BusinessCsvList.Where( b => !this.FamilyDict.ContainsKey( string.Format( "{0}^B_{1}", ImportInstanceFKPrefix, b.Id ) ) )
                                                .GroupBy( b => b.Id )
                                                .Select( a => new
                                                {
                                                    BusinessId = a.Key,
                                                    Campus = a.Select( b => b.Campus ).FirstOrDefault(),
                                                    CreatedDate = a.Select( b => b.CreatedDateTime ).FirstOrDefault(),
                                                    ModifiedDate = a.Select( b => b.ModifiedDateTime ).FirstOrDefault(),
                                                    BusinessName = a.Select( b => b.Name ).FirstOrDefault(),
                                                } );

            this.ReportProgress( 0, string.Format( "Creating {0} new Business Family Group records...", families.Count() ) );

            foreach ( var family in families )
            {
                var newFamily = new Group
                {
                    GroupTypeId = FamilyGroupTypeId,
                    Name = family.BusinessName,
                    ForeignId = family.BusinessId.AsIntegerOrNull(),
                    ForeignKey = string.Format( "{0}^B_{1}", ImportInstanceFKPrefix, family.BusinessId ),
                    CreatedDateTime = family.CreatedDate.ToSQLSafeDate() ?? importDateTime,
                    ModifiedDateTime = family.ModifiedDate.ToSQLSafeDate() ?? importDateTime
                };

                if ( family.Campus != null )
                {
                    Campus campus = null;
                    if ( family.Campus.CampusId.IsNotNullOrWhiteSpace() )
                    {
                        if ( this.UseExistingCampusIds )
                        {
                            var campusIdInt = family.Campus.CampusId?.AsIntegerOrNull();
                            campus = this.CampusesDict.GetValueOrNull( campusIdInt.Value );
                        }
                        else
                        {
                            campus = this.CampusImportDict[$"{this.ImportInstanceFKPrefix}^{family.Campus.CampusId}"];
                        }
                        if ( campus == null )
                        {
                            errors += $"{DateTime.Now},Business,\"Invalid CampusId ({family.Campus.CampusId}) provided for BusinessId {family.BusinessId}. No campus was attached to its family.\"\r\n";
                        }
                    }
                    else if ( family.Campus.CampusName.IsNotNullOrWhiteSpace() )
                    {
                        if ( this.UseExistingCampusIds )
                        {
                            campus = this.CampusesDict.Values.FirstOrDefault( c => c.Name == family.Campus.CampusName );
                        }
                        else
                        {
                            campus = this.CampusImportDict.Values.FirstOrDefault( c => c.Name == family.Campus.CampusName );
                        }
                        if ( campus == null )
                        {
                            errors += $"{DateTime.Now},Business,\"Invalid CampusName ({family.Campus.CampusName}) provided for BusinessId {family.BusinessId}. No campus was attached to its family.\"\r\n";
                        }
                    }

                    newFamily.CampusId = campus?.Id;
                }

                if ( string.IsNullOrWhiteSpace( newFamily.Name ) )
                {
                    newFamily.Name = "Family";
                }

                familiesToCreate.Add( newFamily );
            }
            rockContext.BulkInsert( familiesToCreate );
            if ( errors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, errors, hasMultipleErrors: true );
            }

            ReportProgress( 0, string.Format( "Completed Creating {0} New Family Group records...", families.Count() ) );

            // Reload family lookup to include new families
            LoadFamilyDict();

            // Now we move on to Business records
            ReportProgress( 0, string.Format( "Begin processing {0} Business Records...", this.BusinessCsvList.Count ) );

            var familiesLookup = this.FamilyDict.ToDictionary( d => d.Key, d => d.Value );
            var personLookup = this.PersonDict.ToDictionary( p => p.Key, p => p.Value );
            var familyRolesLookup = GroupTypeCache.GetFamilyGroupType().Roles.ToDictionary( k => k.Guid, v => v );
            var familyAdultRole = familyRolesLookup[Rock.SystemGuid.GroupRole.GROUPROLE_FAMILY_MEMBER_ADULT.AsGuid()];

            // Slice data into chunks and process
            var businessesRemainingToProcess = this.BusinessCsvList.Count;
            var workingBusinessCsvList = this.BusinessCsvList.ToList();
            var completed = 0;

            while ( businessesRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.PersonChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} Business records processed." );
                }

                if ( completed % this.PersonChunkSize < 1 )
                {
                    var csvChunk = workingBusinessCsvList.Take( Math.Min( this.PersonChunkSize, workingBusinessCsvList.Count ) ).ToList();
                    completed += BulkBusinessImport( rockContext, csvChunk, personLookup, familiesLookup, familyAdultRole );
                    businessesRemainingToProcess -= csvChunk.Count;
                    workingBusinessCsvList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            LoadPersonDict();

            return completed;
        }

        /// <summary>
        /// Bulk import of BusinessImports.
        /// </summary>
        /// <param name="rockContext">The Rock context.</param>
        /// <param name="businessCsvs">The list of BusinessCsv objects.</param>
        /// <param name="personLookupDict">The dictionary of imported people.</param>
        /// <param name="familiesLookup">The dictionary of imported famlies.</param>
        /// <param name="familyAdultRole">The Adult role for family group type.</param>
        /// <returns></returns>
        public int BulkBusinessImport( RockContext rockContext, List<BusinessCsv> businessCsvs, Dictionary<string, Person> personLookupDict, Dictionary<string, Group> familiesLookup, GroupTypeRoleCache familyAdultRole )
        {
            var personLookup = personLookupDict.ToDictionary( p => p.Key, p => p.Value );
            var businessImportList = new List<PersonImport>();
            var errors = string.Empty;

            foreach ( var businessCsv in businessCsvs )
            {
                if ( businessCsv.Id.IsNullOrWhiteSpace() )
                {
                    errors += string.Format( "{0},{1},\"{2}\"\r\n", DateTime.Now.ToString(), "Business", string.Format( "Missing Id for Business '{0}'. Skipping.", businessCsv.Name ) );
                    continue;
                }

                var newBusiness = new PersonImport
                {
                    RecordTypeValueId = BusinessRecordTypeId,
                    PersonForeignId = businessCsv.Id.AsIntegerOrNull(),
                    PersonForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, businessCsv.Id ),
                    FamilyForeignId = businessCsv.Id.AsIntegerOrNull(),
                    FamilyForeignKey = string.Format( "{0}^B_{1}", this.ImportInstanceFKPrefix, businessCsv.Id ),
                    InactiveReasonNote = businessCsv.InactiveReasonNote.IsNullOrWhiteSpace() ? businessCsv.InactiveReason : businessCsv.InactiveReasonNote,
                    RecordStatusReasonValueId = this.RecordStatusReasonDVDict.Values.FirstOrDefault( v => v.Value.Equals( businessCsv.InactiveReason ) )?.Id,
                    LastName = businessCsv.Name,
                    Gender = Rock.Model.Gender.Unknown,
                    Email = businessCsv.Email,
                    IsEmailActive = businessCsv.IsEmailActive.HasValue ? businessCsv.IsEmailActive.Value : true,
                    EmailPreference = businessCsv.EmailPreference.HasValue ? businessCsv.EmailPreference.Value : EmailPreference.EmailAllowed,
                    CreatedDateTime = businessCsv.CreatedDateTime.ToSQLSafeDate(),
                    ModifiedDateTime = businessCsv.ModifiedDateTime.ToSQLSafeDate(),
                    Note = businessCsv.Note,
                    GivingIndividually = false,
                    GroupRoleId = familyAdultRole.Id
                };

                switch ( businessCsv.RecordStatus )
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

                businessImportList.Add( newBusiness );
            }

            var importDateTime = RockDateTime.Now;
            var personAliasesToInsert = new List<PersonAlias>();

            foreach ( var personImport in businessImportList )
            {
                Person person = null;
                if ( personLookup.ContainsKey( personImport.PersonForeignKey ) )
                {
                    person = personLookup[personImport.PersonForeignKey];
                }

                if ( person == null )
                {
                    person = new Person();
                    errors += InitializeBusinessFromPersonImport( personImport, person );
                    personLookup.Add( personImport.PersonForeignKey, person );
                }
            }

            // lookup GroupId from Group.ForeignId
            var groupService = new GroupService( rockContext );
            var familyIdLookup = familiesLookup.ToDictionary( k => k.Key, v => v.Value.Id );

            var businessesToInsert = personLookup.Where( a => a.Value.Id == 0 ).Select( a => a.Value ).ToList();
            rockContext.BulkInsert( businessesToInsert );

            // Make sure everybody has a PersonAlias
            var personAliasService = new PersonAliasService( rockContext );
            var personAliasServiceQry = personAliasService.Queryable();
            var qryAllPersons = new PersonService( rockContext ).Queryable( true, true );
            personAliasesToInsert.AddRange( qryAllPersons.Where( p => !string.IsNullOrEmpty( p.ForeignKey ) && p.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) && !p.Aliases.Any() && !personAliasServiceQry.Any( pa => pa.AliasPersonId == p.Id ) )
                                                     .Select( x => new { x.Id, x.Guid, x.ForeignId, x.ForeignKey } )
                                                     .ToList()
                                                     .Select( person => new PersonAlias { AliasPersonId = person.Id, AliasPersonGuid = person.Guid, PersonId = person.Id, ForeignId = person.ForeignId, ForeignKey = person.ForeignKey } ).ToList() );
            rockContext.BulkInsert( personAliasesToInsert );

            var familyGroupMembersQry = new GroupMemberService( rockContext ).Queryable( true ).Where( a => a.Group.GroupTypeId == FamilyGroupTypeId );

            // get the person Ids along with the PersonImport and GroupMember record

            var personsIdsForPersonImport = from p in qryAllPersons.AsNoTracking().Where( a => !string.IsNullOrEmpty( a.ForeignKey ) && a.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                             .Select( a => new { a.Id, a.ForeignKey } ).ToList()
                             join pi in businessImportList on p.ForeignKey equals pi.PersonForeignKey
                             join f in groupService.Queryable().Where( a => !string.IsNullOrEmpty( a.ForeignKey ) && a.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) && a.GroupTypeId == FamilyGroupTypeId )
                             .Select( a => new { a.Id, a.ForeignKey } ).ToList() on pi.FamilyForeignKey equals f.ForeignKey
                             join gm in familyGroupMembersQry.Select( a => new { a.Id, a.PersonId } ) on p.Id equals gm.PersonId into gmj
                             from gm in gmj.DefaultIfEmpty()
                             select new
                             {
                                 PersonId = p.Id,
                                 PersonImport = pi,
                                 FamilyId = f.Id,
                                 HasGroupMemberRecord = gm != null
                             };

            // Make the GroupMember records for all the imported businesses (unless they are already have a groupmember record for the family)
            var groupMemberRecordsToInsertQry = from ppi in personsIdsForPersonImport
                                                where !ppi.HasGroupMemberRecord
                                                select new GroupMember
                                                {
                                                    PersonId = ppi.PersonId,
                                                    GroupRoleId = ppi.PersonImport.GroupRoleId,
                                                    GroupId = ppi.FamilyId,
                                                    GroupTypeId = FamilyGroupTypeId,
                                                    GroupMemberStatus = GroupMemberStatus.Active,
                                                    CreatedDateTime = ppi.PersonImport.CreatedDateTime.ToSQLSafeDate() ?? importDateTime,
                                                    ModifiedDateTime = ppi.PersonImport.ModifiedDateTime.ToSQLSafeDate() ?? importDateTime,
                                                };

            var groupMemberRecordsToInsertList = groupMemberRecordsToInsertQry.ToList();

            rockContext.BulkInsert( groupMemberRecordsToInsertList );

            if ( errors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, errors, hasMultipleErrors: true );
            }

            // since we bypassed Rock SaveChanges when Inserting Person records, sweep thru and ensure the AgeClassification, PrimaryFamily, and GivingLeaderId is set
            PersonService.UpdatePersonAgeClassificationAll( rockContext );
            PersonService.UpdatePrimaryFamilyAll( rockContext );
            PersonService.UpdateGivingLeaderIdAll( rockContext );

            return businessesToInsert.Count;
        }

        /// <summary>
        /// Processes the BusinessAddress list.
        /// </summary>
        private int ImportBusinessAddresses()
        {
            this.ReportProgress( 0, "Preparing Business Address data for import..." );

            var rockContext = new RockContext();
            if ( this.FamilyDict == null )
            {
                LoadFamilyDict( rockContext );
            }

            var familyAddressImports = new List<GroupAddressImport>();
            var familyAddressErrors = string.Empty;
            var addressCsvObjects = this.BusinessAddressCsvList.Select( a => new
            {
                Family = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, a.BusinessId ) )?.PrimaryFamily,
                FamilyForeignKey = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, a.BusinessId ) )?.PrimaryFamily.ForeignKey,
                BusinessAddressCsv = a
            } );
            var groupLocationService = new GroupLocationService( rockContext );
            var groupLocationLookup = groupLocationService.Queryable()
                                                            .AsNoTracking()
                                                            .Where( l => !string.IsNullOrEmpty( l.ForeignKey ) && l.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                                                            .Select( a => new
                                                            {
                                                                GroupLocation = a,
                                                                a.ForeignKey
                                                            } )
                                                            .ToDictionary( k => k.ForeignKey, v => v.GroupLocation );
            var addressesNoFamilyMatch = addressCsvObjects.Where( a => a.Family == null || a.Family.Id <= 0 ).ToList();
            if ( addressesNoFamilyMatch.Count > 0 )
            {
                var errorMsg = $"{addressesNoFamilyMatch.Count} Addresses found with invalid or missing Business or Family records and will be skipped. Affected BusinessIds are:\r\n";
                errorMsg += string.Join( ", ", addressesNoFamilyMatch.Select( a => a.BusinessAddressCsv.BusinessId ) );
                LogException( "BusinessAddress", errorMsg );
            }
            var addressCsvObjectsToProcess = addressCsvObjects.Where( a => a.Family != null && a.Family.Id > 0 && !groupLocationLookup.ContainsKey( string.Format( "{0}^{1}", ImportInstanceFKPrefix, a.BusinessAddressCsv.AddressId.IsNotNullOrWhiteSpace() ? a.BusinessAddressCsv.AddressId : string.Format( "{0}_{1}", a.Family.Id, a.BusinessAddressCsv.AddressType.ToString() ) ) ) ).ToList();
            this.ReportProgress( 0, $"{this.BusinessAddressCsvList.Count - addressesNoFamilyMatch.Count - addressCsvObjectsToProcess.Count} Addresses already exist. Preparing {addressCsvObjectsToProcess.Count} Business Address records for processing." );

            foreach ( var addressCsv in addressCsvObjectsToProcess )
            {
                if ( string.IsNullOrEmpty( addressCsv.BusinessAddressCsv.Street1 ) )
                {
                    familyAddressErrors += $"{DateTime.Now}, BusinessAddress, Blank Street Address for BusinessId {addressCsv.BusinessAddressCsv.BusinessId}, Address Type {addressCsv.BusinessAddressCsv.AddressType}. Business Address was skipped.\r\n";
                    continue;
                }
                if ( addressCsv.Family == null )
                {
                    familyAddressErrors += $"{DateTime.Now}, BusinessAddress, Family for BusinessId {addressCsv.BusinessAddressCsv.BusinessId} not found. Business Address was skipped.\r\n";
                    continue;
                }

                var groupLocationTypeValueId = GetGroupLocationTypeDVId( addressCsv.BusinessAddressCsv.AddressType );

                if ( groupLocationTypeValueId.HasValue )
                {
                    var newGroupAddress = new GroupAddressImport()
                    {
                        GroupId = addressCsv.Family.Id,
                        GroupLocationTypeValueId = groupLocationTypeValueId.Value,
                        IsMailingLocation = addressCsv.BusinessAddressCsv.IsMailing,
                        IsMappedLocation = addressCsv.BusinessAddressCsv.AddressType == AddressType.Home,
                        Street1 = addressCsv.BusinessAddressCsv.Street1.Left( 100 ),
                        Street2 = addressCsv.BusinessAddressCsv.Street2.Left( 100 ),
                        City = addressCsv.BusinessAddressCsv.City.Left( 50 ),
                        State = addressCsv.BusinessAddressCsv.State.Left( 50 ),
                        Country = addressCsv.BusinessAddressCsv.Country.Left( 50 ),
                        PostalCode = addressCsv.BusinessAddressCsv.PostalCode.Left( 50 ),
                        Latitude = addressCsv.BusinessAddressCsv.Latitude.AsDoubleOrNull(),
                        Longitude = addressCsv.BusinessAddressCsv.Longitude.AsDoubleOrNull(),
                        AddressForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, addressCsv.BusinessAddressCsv.AddressId.IsNotNullOrWhiteSpace() ? addressCsv.BusinessAddressCsv.AddressId : string.Format( "{0}_{1}", addressCsv.Family.Id, addressCsv.BusinessAddressCsv.AddressType.ToString() ) )
                    };

                    familyAddressImports.Add( newGroupAddress );
                }
                else
                {
                    familyAddressErrors += $"{DateTime.Now}, BusinessAddress, Unexpected Address Type ({addressCsv.BusinessAddressCsv.AddressType}) encountered for BusinessId \"{addressCsv.BusinessAddressCsv.BusinessId}\". Business Address was skipped.\r\n";
                }
            }

            this.ReportProgress( 0, string.Format( "Begin processing {0} Business Address Records...", familyAddressImports.Count ) );

            // Slice data into chunks and process
            var groupLocationsToInsert = new List<GroupLocation>();
            var groupAddressesRemainingToProcess = familyAddressImports.Count;
            var workingGroupAddressImportList = familyAddressImports.ToList();
            var completedGroupAddresses = 0;

            while ( groupAddressesRemainingToProcess > 0 )
            {
                if ( completedGroupAddresses > 0 && completedGroupAddresses % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completedGroupAddresses} GroupAddress - Locations processed." );
                }

                if ( completedGroupAddresses % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingGroupAddressImportList.Take( Math.Min( this.DefaultChunkSize, workingGroupAddressImportList.Count ) ).ToList();
                    var imported = BulkGroupAddressImport( rockContext, csvChunk, groupLocationLookup, groupLocationsToInsert );
                    completedGroupAddresses += imported;
                    groupAddressesRemainingToProcess -= csvChunk.Count;
                    workingGroupAddressImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }
            if ( familyAddressErrors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, familyAddressErrors, hasMultipleErrors: true );
            }

            // Slice data into chunks and process
            var groupLocationRemainingToProcess = groupLocationsToInsert.Count;
            var workingGroupLocationList = groupLocationsToInsert.ToList();
            var completedGroupLocations = 0;
            var locationDict = new LocationService( rockContext ).Queryable().Select( a => new { a.Id, a.Guid } ).ToList().ToDictionary( k => k.Guid, v => v.Id );

            while ( groupLocationRemainingToProcess > 0 )
            {
                if ( completedGroupLocations > 0 && completedGroupLocations % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completedGroupLocations} GroupAddress - GroupLocations processed." );
                }

                if ( completedGroupLocations % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingGroupLocationList.Take( Math.Min( this.DefaultChunkSize, workingGroupLocationList.Count ) ).ToList();
                    var imported = BulkInsertGroupLocation( rockContext, csvChunk, locationDict );
                    completedGroupLocations += imported;
                    groupLocationRemainingToProcess -= csvChunk.Count;
                    workingGroupLocationList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            return completedGroupAddresses;
        }

        /// <summary>
        /// Processes the Business AttributeValue list.
        /// </summary>
        private int ImportBusinessAttributeValues()
        {
            this.ReportProgress( 0, "Preparing Business Attribute Value data for import..." );
            
            var rockContext = new RockContext();
            var businessAVImports = new List<AttributeValueImport>();
            var errors = string.Empty;
            var businessAttributeValues = this.BusinessAttributeValueCsvList.DistinctBy( av => new { av.AttributeKey, av.BusinessId } ).ToList();  // Protect against duplicates in import data

            var attributeDefinedValuesDict = GetAttributeDefinedValuesDictionary( rockContext, PersonEntityTypeId );
            var attributeValueLookup = GetAttributeValueLookup( rockContext, PersonEntityTypeId );

            foreach ( var attributeValueCsv in businessAttributeValues )
            {
                var business = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, attributeValueCsv.BusinessId ) );
                if ( business == null )
                {
                    errors += $"{DateTime.Now}, BusinessAttributeValue, BusinessId {attributeValueCsv.BusinessId} not found. Business AttributeValue for {attributeValueCsv.AttributeKey} attribute was skipped.\r\n";
                    continue;
                }

                var attribute = this.PersonAttributeDict.GetValueOrNull( attributeValueCsv.AttributeKey );
                if ( attribute == null )
                {
                    errors += $"{DateTime.Now}, BusinessAttributeValue, AttributeKey {attributeValueCsv.AttributeKey} not found. AttributeValue for BusinessId {attributeValueCsv.BusinessId} was skipped.\r\n";
                    continue;
                }

                if ( attributeValueLookup.Any( l => l.Item1 == attribute.Id && l.Item2 == business.Id ) )
                {
                    errors += $"{DateTime.Now}, BusinessAttributeValue, AttributeValue for AttributeKey {attributeValueCsv.AttributeKey} and BusinessId {attributeValueCsv.BusinessId} already exists. AttributeValueId {attributeValueCsv.AttributeValueId} was skipped.\r\n";
                    continue;
                }

                var newAttributeValue = new AttributeValueImport()
                {
                    AttributeId = attribute.Id,
                    AttributeValueForeignId = attributeValueCsv.AttributeValueId.AsIntegerOrNull(),
                    EntityId = business.Id,
                    AttributeValueForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, attributeValueCsv.AttributeValueId.IsNotNullOrWhiteSpace() ? attributeValueCsv.AttributeValueId : string.Format( "{0}_{1}", attributeValueCsv.BusinessId, attributeValueCsv.AttributeKey ) )
                };

                newAttributeValue.Value = GetAttributeValueStringByAttributeType( rockContext, attributeValueCsv.AttributeValue, attribute, attributeDefinedValuesDict );
                businessAVImports.Add( newAttributeValue );
            }

            this.ReportProgress( 0, string.Format( "Begin processing {0} Business Attribute Value Records...", businessAVImports.Count ) );
            if ( errors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, errors, hasMultipleErrors: true );
            }
            return ImportAttributeValues( businessAVImports );
        }

        private int ImportBusinessPhones()
        {
            this.ReportProgress( 0, "Preparing Business Phone data for import..." );

            var businessPhoneImports = new List<PhoneNumberImport>();
            var errors = string.Empty;

            foreach ( var phoneCsv in this.BusinessPhoneCsvList )
            {
                var business = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, phoneCsv.BusinessId ) );
                if ( business == null )
                {
                    errors += $"{DateTime.Now}, BusinessPhone, BusinessId {phoneCsv.BusinessId} not found. {phoneCsv.PhoneType} Phone number was skipped.\r\n";
                    continue;
                }
                var newBusinessPhone = new PhoneNumberImport
                {
                    NumberTypeValueId = this.PhoneNumberTypeDVDict[phoneCsv.PhoneType].Id,
                    Number = phoneCsv.PhoneNumber,
                    IsMessagingEnabled = phoneCsv.IsMessagingEnabled ?? false,
                    IsUnlisted = phoneCsv.IsUnlisted ?? false,
                    CountryCode = phoneCsv.CountryCode,
                    Extension = phoneCsv.Extension,
                    PersonId = business.Id,
                    PhoneId = phoneCsv.PhoneId
                };
                businessPhoneImports.Add( newBusinessPhone );
            }

            this.ReportProgress( 0, string.Format( "Begin processing {0} Business Phone Records...", businessPhoneImports.Count ) );

            // Slice data into chunks and process
            var businessPhonesRemainingToProcess = businessPhoneImports.Count;
            var workingBusinessPhoneImportList = businessPhoneImports.ToList();
            var completed = 0;

            while ( businessPhonesRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} Phone Numbers processed." );
                }

                if ( completed % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingBusinessPhoneImportList.Take( Math.Min( this.DefaultChunkSize, workingBusinessPhoneImportList.Count ) ).ToList();
                    var imported = BulkPersonPhoneImport( csvChunk );
                    completed += imported;
                    businessPhonesRemainingToProcess -= csvChunk.Count;
                    workingBusinessPhoneImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }
            if ( errors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, errors, hasMultipleErrors: true );
            }
            return completed;
        }

        private int ImportBusinessContacts()
        {
            var rockContext = new RockContext();
            var groupMemberService = new GroupMemberService( rockContext );
            var errors = string.Empty;
            this.ReportProgress( 0, "Preparing Business Contact data for import..." );

            var knownRelationshipGroupType = GroupTypeCache.Get( Rock.SystemGuid.GroupType.GROUPTYPE_KNOWN_RELATIONSHIPS.AsGuid() );
            int ownerRoleId = knownRelationshipGroupType.Roles.FirstOrDefault( r => r.Guid.Equals( Rock.SystemGuid.GroupRole.GROUPROLE_KNOWN_RELATIONSHIPS_OWNER.AsGuid() ) ).Id;

            // Create lookup of existing KnownRelationship type Groups
            var knownRelationshipGroupLookup = groupMemberService.Queryable()
                                                            .Include( gm => gm.Person )
                                                            .Include( gm => gm.Group )
                                                            .Where( gm => gm.Group.GroupTypeId == knownRelationshipGroupType.Id
                                                                            && gm.GroupRoleId == ownerRoleId
                                                                            && gm.Person.ForeignKey != null
                                                                            && gm.Person.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                                                            .GroupBy( gm => gm.PersonId )
                                                            .Select( a => new
                                                            {
                                                                OwnerPersonId = a.Key,
                                                                OwnerPersonForeignKey = a.Select( gm => gm.Person.ForeignKey ).FirstOrDefault(),
                                                                Group = a.Select( gm => gm.Group ).FirstOrDefault()
                                                            } )
                                                            .ToDictionary( k => k.OwnerPersonForeignKey, v => v.Group );


            var businessContactCsvObjects = this.BusinessContactCsvList.Select( a => new BusinessContactObject
            {
                Business = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, a.BusinessId ) ),
                Contact = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, a.PersonId ) ),
                BusinessKnownRelationshipGroup = knownRelationshipGroupLookup.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, a.BusinessId ) ),
                ContactKnownRelationshipGroup = knownRelationshipGroupLookup.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, a.PersonId ) ),
                BusinessRelationshipForeignKey = string.Format( "{0}^{1}_{2}_Business", ImportInstanceFKPrefix, a.BusinessId, a.PersonId ),
                ContactRelationshipForeignKey = string.Format( "{0}^{1}_{2}_Contact", ImportInstanceFKPrefix, a.PersonId, a.BusinessId ),
                BusinessContactCsv = a
            } );

            var invalidBusinessIdCsvs = businessContactCsvObjects.Where( a => a.Business == null );
            var invalidPersonIdCsvs = businessContactCsvObjects.Where( a => a.Business != null && a.Contact == null );
            var businessContactObjsToProcess = businessContactCsvObjects.Where( a => a.Business != null && a.Contact != null ).ToList();

            this.ReportProgress( 0, string.Format( "Begin processing {0} Business Contact Records...", this.BusinessContactCsvList.Count ) );

            foreach ( var invalidBusinessIdCsv in invalidBusinessIdCsvs )
            {
                errors += $"{DateTime.Now}, BusinessContact, Invalid BusinessId {invalidBusinessIdCsv.BusinessContactCsv.BusinessId}. Business contact for PersonId {invalidBusinessIdCsv.BusinessContactCsv.PersonId} was skipped.\r\n";
            }
            foreach ( var invalidPersonIdCsv in invalidPersonIdCsvs )
            {
                errors += $"{DateTime.Now}, BusinessContact, Invalid PersonId {invalidPersonIdCsv.BusinessContactCsv.PersonId}. Contact for BusinessId {invalidPersonIdCsv.BusinessContactCsv.BusinessId} was skipped.\r\n";
            }

            if ( errors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, errors, hasMultipleErrors: true );
            }

            // Create Know Relationship groups for any businesses or contacts that do not yet have one.
            this.ReportProgress( 0, "Creating Known Relationship Groups for businesses and contacts..." );

            var groupService = new GroupService( rockContext );
            var knownRelationshipGroupsToAdd = new List<Group>();
            var knownRelationshipGroupMembersToAdd = new List<GroupMember>();
            var businessesNoKRGroups = businessContactObjsToProcess.Where( c => c.BusinessKnownRelationshipGroup == null ).OrderBy( bc => bc.Business.Id );
            var contactsNoKRGroups = businessContactObjsToProcess.Where( c => c.ContactKnownRelationshipGroup == null ).OrderBy( bc => bc.Contact.Id );

            var newPersonGroupDict = new Dictionary<int, Group>();
            int businessId = 0;
            Group knownRelationshipGroup = null;
            foreach ( var contactImport in businessesNoKRGroups )
            {
                // Make sure we only create one new Known Relationship group per business
                if ( contactImport.Business.Id != businessId )
                {
                    var newKnownRelationshipGroup = new Group
                    {
                        Name = "Known Relationship",
                        GroupTypeId = knownRelationshipGroupType.Id,
                        Guid = Guid.NewGuid(),
                        ForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, contactImport.BusinessContactCsv.BusinessId )
                    };
                    knownRelationshipGroupsToAdd.Add( newKnownRelationshipGroup );
                    knownRelationshipGroup = newKnownRelationshipGroup;

                    var ownerGroupMember = new GroupMember
                    {
                        PersonId = contactImport.Business.Id,
                        Group = newKnownRelationshipGroup,
                        GroupTypeId = newKnownRelationshipGroup.GroupTypeId,
                        GroupRoleId = ownerRoleId
                    };
                    knownRelationshipGroupMembersToAdd.Add( ownerGroupMember );
                    businessId = contactImport.Business.Id;
                    newPersonGroupDict.Add( businessId, newKnownRelationshipGroup );
                }
                contactImport.BusinessKnownRelationshipGroupGuid = knownRelationshipGroup.Guid;
            }

            int contactId = 0;
            foreach ( var contactImport in contactsNoKRGroups )
            {
                // Check to see if we already created a new Known Relationship Group in the previous loop.
                if ( newPersonGroupDict.ContainsKey( contactImport.Contact.Id ) )
                {
                    contactImport.ContactKnownRelationshipGroupGuid = newPersonGroupDict[contactImport.Contact.Id].Guid;
                    continue;
                }

                // Make sure we only create one new Known Relationship group per contact
                if ( contactImport.Contact.Id != contactId )
                {
                    var newKnownRelationshipGroup = new Group
                    {
                        Name = "Known Relationship",
                        GroupTypeId = knownRelationshipGroupType.Id,
                        Guid = Guid.NewGuid(),
                        ForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, contactImport.BusinessContactCsv.PersonId )
                    };
                    knownRelationshipGroupsToAdd.Add( newKnownRelationshipGroup );
                    knownRelationshipGroup = newKnownRelationshipGroup;

                    var ownerGroupMember = new GroupMember
                    {
                        PersonId = contactImport.Contact.Id,
                        Group = newKnownRelationshipGroup,
                        GroupTypeId = newKnownRelationshipGroup.GroupTypeId,
                        GroupRoleId = ownerRoleId
                    };
                    knownRelationshipGroupMembersToAdd.Add( ownerGroupMember );
                    contactId = contactImport.Contact.Id;
                }
                contactImport.ContactKnownRelationshipGroupGuid = knownRelationshipGroup.Guid;
            }
            rockContext.BulkInsert( knownRelationshipGroupsToAdd );

            var knownRelationshipGroupDict = new GroupService( rockContext ).Queryable()
                                                                            .AsNoTracking()
                                                                            .Where( g => g.GroupTypeId == knownRelationshipGroupType.Id )
                                                                            .ToList()
                                                                            .ToDictionary( k => k.Guid, v => v );

            foreach ( var groupMember in knownRelationshipGroupMembersToAdd )
            {
                groupMember.GroupId = knownRelationshipGroupDict[groupMember.Group.Guid].Id;
            }
            rockContext.BulkInsert( knownRelationshipGroupMembersToAdd );

            this.ReportProgress( 0, $"Created {knownRelationshipGroupsToAdd.Count} Known Relationship Groups." );

            foreach ( var businessContactImport in businessContactObjsToProcess.Where( c => c.BusinessKnownRelationshipGroup == null || c.ContactKnownRelationshipGroup == null ) )
            {
                if ( businessContactImport.BusinessKnownRelationshipGroup == null )
                {
                    businessContactImport.BusinessKnownRelationshipGroup = knownRelationshipGroupDict[businessContactImport.BusinessKnownRelationshipGroupGuid.Value];
                }
                if ( businessContactImport.ContactKnownRelationshipGroup == null )
                {
                    businessContactImport.ContactKnownRelationshipGroup = knownRelationshipGroupDict[businessContactImport.ContactKnownRelationshipGroupGuid.Value];
                }
            }

            this.ReportProgress( 0, $"Processing {businessContactObjsToProcess.Count()} Business Contact relationships." );

            var knownRelationshipLookup = groupService.Queryable()
                                                            .Include( g => g.Members )
                                                            .Where( g => g.GroupTypeId == knownRelationshipGroupType.Id )
                                                            .Select( a => new
                                                            {
                                                                OwnerPersonId = a.Members.FirstOrDefault( m => m.GroupRoleId == ownerRoleId ).PersonId,   // Assumption that each person record can only be owner of 1 Known Relationship group.
                                                                RelatedPersonIds = a.Members.Where( m => m.GroupRoleId != ownerRoleId ).Select( m => m.PersonId ).ToList()
                                                            } )
                                                            .ToDictionary( k => k.OwnerPersonId, v => v.RelatedPersonIds );

            // Slice data into chunks and process
            var businessContactsRemainingToProcess = businessContactObjsToProcess.Count();
            var workingBusinessContactImportList = businessContactObjsToProcess.ToList();
            var completed = 0;

            while ( businessContactsRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.PersonChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} Business Contacts processed." );
                }

                if ( completed % this.PersonChunkSize < 1 )
                {
                    var csvChunk = workingBusinessContactImportList.Take( Math.Min( this.PersonChunkSize, workingBusinessContactImportList.Count ) ).ToList();
                    var imported = CreateBusinessContacts( rockContext, csvChunk, knownRelationshipGroupType, knownRelationshipLookup );
                    completed += imported;
                    businessContactsRemainingToProcess -= csvChunk.Count;
                    workingBusinessContactImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }
            return completed;
        }

        private int CreateBusinessContacts( RockContext rockContext, List<BusinessContactObject> businessContactImports, GroupTypeCache knownRelationshipGroupType, Dictionary<int, List<int>> knownRelationshipLookup )
        {
            var errors = string.Empty;
            int businessContactRoleId = knownRelationshipGroupType.Roles.FirstOrDefault( r => r.Guid.Equals( Rock.SystemGuid.GroupRole.GROUPROLE_KNOWN_RELATIONSHIPS_BUSINESS_CONTACT.AsGuid() ) ).Id;
            int businessRoleId = knownRelationshipGroupType.Roles.FirstOrDefault( r => r.Guid.Equals( Rock.SystemGuid.GroupRole.GROUPROLE_KNOWN_RELATIONSHIPS_BUSINESS.AsGuid() ) ).Id;
            var knownRelationshipGroupMembersToAdd = new List<GroupMember>();
            foreach ( var businessContactImport in businessContactImports )
            {
                if ( knownRelationshipLookup.Any( l => businessContactImport.Business.Id == l.Key && l.Value.Any( v => v == businessContactImport.Contact.Id ) ) )
                {
                    errors += $"{DateTime.Now}, BusinessContact, Relationship already exists between Business: {businessContactImport.Business.FullName} and Person: {businessContactImport.Contact.FullName}. Business Contact skipped.\r\n";
                    continue;
                }

                // Add Business to Known Relationship group of Contact.

                var businessGroupMember = new GroupMember
                {
                    PersonId = businessContactImport.Business.Id,
                    GroupRoleId = businessRoleId,
                    GroupId = businessContactImport.ContactKnownRelationshipGroup.Id,
                    GroupTypeId = businessContactImport.ContactKnownRelationshipGroup.GroupTypeId,
                };
                knownRelationshipGroupMembersToAdd.Add( businessGroupMember );

                // Add Contact to Known Relationship group of Business.

                var contactGroupMember = new GroupMember
                {
                    PersonId = businessContactImport.Contact.Id,
                    GroupRoleId = businessContactRoleId,
                    GroupId = businessContactImport.BusinessKnownRelationshipGroup.Id,
                    GroupTypeId = businessContactImport.BusinessKnownRelationshipGroup.GroupTypeId,
                };
                knownRelationshipGroupMembersToAdd.Add( contactGroupMember );
            }
            rockContext.BulkInsert( knownRelationshipGroupMembersToAdd );

            return businessContactImports.Count;
        }

    }
}