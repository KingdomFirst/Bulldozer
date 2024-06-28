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
        /// Processes the person list.
        /// </summary>
        /// <returns>Count of processed PersonCsv records</returns>
        private int ImportPersonList()
        {
            this.ReportProgress( 0, "Preparing Person data for import..." );

            // Let's deal with family groups first
            var familiesToCreate = new List<Group>();
            var rockContext = new RockContext();
            var importDateTime = RockDateTime.Now;

            var errors = string.Empty;
            var families = this.PersonCsvList.Where( p => !this.FamilyDict.ContainsKey( string.Format( "{0}^{1}", ImportInstanceFKPrefix, p.FamilyId ) ) )
                                                .GroupBy( p => new { p.FamilyId, p.FamilyName } )
                                                .Select( a => new
                                                {
                                                    FamilyId = a.Key.FamilyId,
                                                    PersonId = a.Select( p => p.Id ).FirstOrDefault(),
                                                    FamilyName = a.Key.FamilyName,
                                                    Campus = a.Select( p => p.Campus ).FirstOrDefault(),
                                                    CreatedDate = a.Select( p => p.CreatedDateTime ).FirstOrDefault(),
                                                    ModifiedDate = a.Select( p => p.ModifiedDateTime ).FirstOrDefault(),
                                                    LastName = a.Select( p => p.LastName ).FirstOrDefault(),

                                                } );

            int nextNewFamilyForeignId = this.FamilyDict.Any( a => a.Value.ForeignId.HasValue ) ? this.FamilyDict.Max( a => a.Value.ForeignId.GetValueOrDefault() ) : 0;
            if ( families.Any() )
            {
                var importsWithNumericIds = families.Where( a => a.FamilyId.ToIntSafe( 0 ) > 0 );
                if ( importsWithNumericIds.Any() )
                {
                    nextNewFamilyForeignId = Math.Max( nextNewFamilyForeignId, importsWithNumericIds.Max( a => a.FamilyId.ToIntSafe( 0 ) ) );
                }
            }

            this.ReportProgress( 0, string.Format( "Creating {0} New Family Group records...", families.Count() ) );

            foreach ( var family in families )
            {
                var newFamily = new Group
                {
                    GroupTypeId = FamilyGroupTypeId,
                    Name = family.FamilyName.IsNullOrWhiteSpace() ? family.LastName : family.FamilyName,
                    ForeignId = family.FamilyId.AsIntegerOrNull(),
                    ForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, family.FamilyId ),
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
                            campus = this.CampusImportDict.GetValueOrNull($"{this.ImportInstanceFKPrefix}^{family.Campus.CampusId}");
                        }
                        if ( campus == null )
                        {
                            errors += $"{DateTime.Now},Person,\"Invalid CampusId ({family.Campus.CampusId}) provided for PersonId {family.PersonId}. No campus was attached to its family.\"\r\n";
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
                            errors += $"{DateTime.Now},Person,\"Invalid CampusName ({family.Campus.CampusName}) provided for PersonId {family.PersonId}. No campus was attached to its family.\"\r\n";
                        }
                    }

                    newFamily.CampusId = campus?.Id;
                }
                if ( string.IsNullOrWhiteSpace( newFamily.Name ) )
                {
                    newFamily.Name = "Family";
                }

                if ( family.FamilyId.IsNullOrWhiteSpace() )
                {
                    newFamily.ForeignId = ++nextNewFamilyForeignId;
                    newFamily.ForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, nextNewFamilyForeignId );
                }

                familiesToCreate.Add( newFamily );
            }
            rockContext.BulkInsert( familiesToCreate );
            if ( errors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, errors, hasMultipleErrors: true );
            }

            this.ReportProgress( 0, string.Format( "Completed Creating {0} New Family Group records...", families.Count() ) );

            // Reload family lookup to include new families
            LoadFamilyDict();

            // Now we move on to person records
            this.ReportProgress( 0, string.Format( "Begin processing {0} Person Records...", this.PersonCsvList.Count ) );

            var familyRolesLookup = GroupTypeCache.GetFamilyGroupType().Roles.ToDictionary( k => k.Guid, v => v );
            var familiesLookup = this.FamilyDict.ToDictionary( d => d.Key, d => d.Value );
            var personLookup = this.PersonDict.ToDictionary( p => p.Key, p => p.Value );
            var gradeOffsetLookupFromDescription = DefinedTypeCache.Get( Rock.SystemGuid.DefinedType.SCHOOL_GRADES.AsGuid() )?.DefinedValues
                .ToDictionary( k => k.Description, v => v.Value.AsInteger(), StringComparer.OrdinalIgnoreCase );

            var gradeOffsetLookupFromAbbreviation = DefinedTypeCache.Get( Rock.SystemGuid.DefinedType.SCHOOL_GRADES.AsGuid() )?.DefinedValues
                .Select( a => new { Value = a.Value.AsInteger(), Abbreviation = a.AttributeValues["Abbreviation"]?.Value } )
                .Where( a => !string.IsNullOrWhiteSpace( a.Abbreviation ) )
                .ToDictionary( k => k.Abbreviation, v => v.Value, StringComparer.OrdinalIgnoreCase );

            // Slice data into chunks and process
            var peopleRemainingToProcess = this.PersonCsvList.Count;
            var workingPersonCsvList = this.PersonCsvList.ToList();
            var completed = 0;

            while ( peopleRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.PersonChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} Person records processed." );
                }

                if ( completed % this.PersonChunkSize < 1 )
                {
                    var csvChunk = workingPersonCsvList.Take( Math.Min( this.PersonChunkSize, workingPersonCsvList.Count ) ).ToList();
                    completed += BulkPersonImport( rockContext, csvChunk, personLookup, familyRolesLookup, gradeOffsetLookupFromDescription, gradeOffsetLookupFromAbbreviation, familiesLookup );
                    peopleRemainingToProcess -= csvChunk.Count;
                    workingPersonCsvList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }
            LoadPersonDict();

            return completed;
        }

        /// <summary>
        /// Bulk import of PersonImports.
        /// </summary>
        /// <param name="rockContext">The RockContext.</param>
        /// <param name="personCsvs">The list of PersonCsvs.</param>
        /// <param name="personLookupDict">A foreignKey keyed dictionary of Person records.</param>
        /// <param name="familyRolesLookup">A guid keyed dictionary of family GroupTypeRoleCaches.</param>
        /// <param name="gradeOffsetLookupFromDescription">A description keyed dictionary of gradeoffset DefinedValue values.</param>
        /// <param name="gradeOffsetLookupFromAbbreviation">An abbreviation keyed dictionary of gradeoffset DefinedValue values.</param>
        /// <param name="familiesLookup">A foreignKey keyed dictionary of family groups.</param>
        /// <returns>Count of Person records created</returns>
        public int BulkPersonImport( RockContext rockContext, List<PersonCsv> personCsvs, Dictionary<string, Person> personLookupDict, Dictionary<Guid, GroupTypeRoleCache> familyRolesLookup, Dictionary<string, int> gradeOffsetLookupFromDescription, Dictionary<string, int> gradeOffsetLookupFromAbbreviation, Dictionary<string, Group> familiesLookup )
        {
            var personLookup = personLookupDict.ToDictionary( p => p.Key, p => p.Value );
            var personImportList = new List<PersonImport>();
            var errors = string.Empty;

            foreach ( var personCsv in personCsvs )
            {
                if ( personCsv.Id.IsNullOrWhiteSpace() )
                {
                    errors += string.Format( "{0},{1},\"{2}\"\r\n", DateTime.Now.ToString(), "Person", string.Format( "Missing Id for Person '{0} {1}'. Skipping.", personCsv.FirstName, personCsv.LastName ) );
                    continue;
                }

                if ( personCsv.FamilyId.IsNullOrWhiteSpace() )
                {
                    errors += string.Format( "{0},{1},\"{2}\"\r\n", DateTime.Now.ToString(), "Person", string.Format( "Missing FamilyId for Person '{0}'. Skipping.", personCsv.Id ) );
                    continue;
                }
                var personGender = Rock.Model.Gender.Unknown;
                Enum.TryParse( personCsv.Gender, out personGender );

                var emailPreference = EmailPreference.EmailAllowed;
                Enum.TryParse( personCsv.EmailPreference, out emailPreference );

                var newPerson = new PersonImport
                {
                    RecordTypeValueId = PersonRecordTypeId,
                    PersonForeignId = personCsv.Id.AsIntegerOrNull(),
                    PersonForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, personCsv.Id ),
                    FamilyForeignId = personCsv.FamilyId.AsIntegerOrNull(),
                    FamilyForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, personCsv.FamilyId ),
                    InactiveReasonNote = personCsv.InactiveReasonNote.IsNullOrWhiteSpace() ? personCsv.InactiveReason : personCsv.InactiveReasonNote,
                    RecordStatusReasonValueId = this.RecordStatusReasonDVDict.Values.FirstOrDefault( v => v.Value.Equals( personCsv.InactiveReason ) )?.Id,
                    IsDeceased = personCsv.IsDeceased.HasValue ? personCsv.IsDeceased.Value : false,
                    FirstName = personCsv.FirstName,
                    NickName = personCsv.NickName,
                    MiddleName = personCsv.MiddleName,
                    LastName = personCsv.LastName,
                    Gender = personGender,
                    AnniversaryDate = personCsv.AnniversaryDate.ToSQLSafeDate(),
                    Grade = personCsv.Grade,
                    Email = personCsv.Email,
                    IsEmailActive = personCsv.IsEmailActive.HasValue ? personCsv.IsEmailActive.Value : true,
                    EmailPreference = emailPreference,
                    CreatedDateTime = personCsv.CreatedDateTime.ToSQLSafeDate(),
                    ModifiedDateTime = personCsv.ModifiedDateTime.ToSQLSafeDate(),
                    Note = personCsv.Note,
                    GivingIndividually = personCsv.GiveIndividually.HasValue ? personCsv.GiveIndividually.Value : false
                };
                if ( !string.IsNullOrWhiteSpace( personCsv.PreviousPersonIds ) )
                {
                    newPerson.PreviousPersonIds = personCsv.PreviousPersonIds.StringToIntList().ToList();
                }

                switch ( personCsv.FamilyRole )
                {
                    case "Adult":
                        newPerson.GroupRoleId = familyRolesLookup[Rock.SystemGuid.GroupRole.GROUPROLE_FAMILY_MEMBER_ADULT.AsGuid()].Id;
                        break;

                    case "Child":
                        newPerson.GroupRoleId = familyRolesLookup[Rock.SystemGuid.GroupRole.GROUPROLE_FAMILY_MEMBER_CHILD.AsGuid()].Id;
                        break;
                }

                switch ( personCsv.RecordStatus )
                {
                    case "Active":
                        newPerson.RecordStatusValueId = this.RecordStatusDVDict[Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_ACTIVE.AsGuid()]?.Id;
                        break;

                    case "Inactive":
                        newPerson.RecordStatusValueId = this.RecordStatusDVDict[Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_INACTIVE.AsGuid()]?.Id;
                        break;

                    case "Pending":
                        newPerson.RecordStatusValueId = this.RecordStatusDVDict[Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_PENDING.AsGuid()]?.Id;
                        break;
                }

                if ( !string.IsNullOrEmpty( personCsv.ConnectionStatus ) )
                {
                    newPerson.ConnectionStatusValueId = ConnectionStatusDVDict[personCsv.ConnectionStatus]?.Id;
                }

                if ( !string.IsNullOrEmpty( personCsv.Salutation ) )
                {
                    var prefix = personCsv.Salutation.RemoveSpecialCharacters();
                    newPerson.TitleValueId = TitleDVDict[prefix]?.Id;
                }

                if ( !string.IsNullOrEmpty( personCsv.Suffix ) )
                {
                    var suffix = personCsv.Suffix.RemoveSpecialCharacters();
                    newPerson.SuffixValueId = SuffixDVDict[suffix]?.Id;
                }

                if ( personCsv.Birthdate.HasValue )
                {
                    newPerson.BirthMonth = personCsv.Birthdate.Value.Month;
                    newPerson.BirthDay = personCsv.Birthdate.Value.Day;
                    newPerson.BirthYear = personCsv.Birthdate.Value.Year == personCsv.DefaultBirthdateYear ? ( int? ) null : personCsv.Birthdate.Value.Year;
                }

                if ( !string.IsNullOrEmpty( personCsv.MaritalStatus ) )
                {
                    newPerson.MaritalStatusValueId = MaritalStatusDVDict[personCsv.MaritalStatus]?.Id;
                }

                if ( personCsv.GraduationYear.HasValue )
                {
                    newPerson.GraduationYear = personCsv.GraduationYear.Value;
                }
                else if ( !string.IsNullOrWhiteSpace( newPerson.Grade ) )
                {
                    // do a case-insensitive lookup GradeOffset from either the Description ("Kindergarten", "1st Grade", etc) or Abbreviation ("K", "1st", etc)
                    int? gradeOffset = null;
                    gradeOffset = gradeOffsetLookupFromDescription.GetValueOrNull( newPerson.Grade );
                    if ( gradeOffset == null )
                    {
                        gradeOffset = gradeOffsetLookupFromAbbreviation.GetValueOrNull( newPerson.Grade );
                    }
                    if ( gradeOffset.HasValue )
                    {
                        newPerson.GraduationYear = Person.GraduationYearFromGradeOffset( gradeOffset );
                    }
                }

                personImportList.Add( newPerson );
            }

            var importDateTime = RockDateTime.Now;


            int familyChildRoleId = familyRolesLookup[Rock.SystemGuid.GroupRole.GROUPROLE_FAMILY_MEMBER_CHILD.AsGuid()].Id;
            var personAliasesToInsert = new List<PersonAlias>();

            foreach ( var personImport in personImportList )
            {
                Person person = null;
                if ( personLookup.ContainsKey( personImport.PersonForeignKey ) )
                {
                    person = personLookup[personImport.PersonForeignKey];
                }
                else if ( personImport.PreviousPersonIds.Count > 0 )
                {
                    var previousForeignIds = personImport.PreviousPersonIds.Select( i => string.Format( "{0}^{1}", ImportInstanceFKPrefix, i ) );
                    person = personLookup.FirstOrDefault( d => previousForeignIds.Any( i => i == d.Value.ForeignKey ) ).Value;
                    if ( person != null )
                    {
                        foreach ( var personId in personImport.PreviousPersonIds )
                        {
                            personAliasesToInsert.Add( new PersonAlias
                            {
                                AliasPersonId = personId,
                                AliasPersonGuid = person.Guid,
                                PersonId = person.Id,
                                ForeignId = personImport.PersonForeignId,
                                ForeignKey = personImport.PersonForeignKey
                            } );
                        }
                    }
                }

                if ( person == null )
                {
                    person = new Person();
                    errors += InitializePersonFromPersonImport( personImport, person );
                    personLookup.Add( personImport.PersonForeignKey, person );
                }
            }

            // lookup GroupId from Group.ForeignId
            var groupService = new GroupService( rockContext );
            var familyIdLookup = familiesLookup.ToDictionary( k => k.Key, v => v.Value.Id );

            var personsToInsert = personLookup.Where( a => a.Value.Id == 0 ).Select( a => a.Value ).ToList();
            var personToInsertLookup = personsToInsert.ToDictionary( k => k.ForeignKey, v => v );
            foreach ( var personImport in personImportList )
            {
                if ( !personImport.GivingIndividually.HasValue )
                {
                    // If GivingIndividually is NULL, base it on GroupRole (Adults give with Family, Kids give as individuals)
                    personImport.GivingIndividually = personImport.GroupRoleId == familyChildRoleId;
                }

                if ( !personImport.GivingIndividually.Value && personImport.FamilyForeignKey.IsNotNullOrWhiteSpace() )
                {
                    var personToInsert = personToInsertLookup.GetValueOrNull( personImport.PersonForeignKey );
                    if ( personToInsert != null )
                    {
                        personToInsert.GivingGroupId = familyIdLookup[personImport.FamilyForeignKey];
                    }
                }
            }
            rockContext.BulkInsert( personsToInsert );

            // Make sure everybody has a PersonAlias
            var insertedPersonForeignKeys = personsToInsert.Select( a => a.ForeignKey ).ToList();
            var personAliasService = new PersonAliasService( rockContext );
            var personAliasServiceQry = personAliasService.Queryable();
            var qryAllPersons = new PersonService( rockContext ).Queryable( true, true );
            personAliasesToInsert.AddRange( qryAllPersons.Where( p => !string.IsNullOrEmpty( p.ForeignKey ) && p.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) && !p.Aliases.Any() && !personAliasServiceQry.Any( pa => pa.AliasPersonId == p.Id ) )
                                                     .Select( x => new { x.Id, x.Guid, x.ForeignId, x.ForeignKey } )
                                                     .ToList()
                                                     .Select( person => new PersonAlias { AliasPersonId = person.Id, AliasPersonGuid = person.Guid, PersonId = person.Id, ForeignId = person.ForeignId, ForeignKey = person.ForeignKey } ).ToList() );
            rockContext.BulkInsert( personAliasesToInsert );

            var familyGroupMembersQry = new GroupMemberService( rockContext ).Queryable( true ).Where( a => a.Group.GroupTypeId == FamilyGroupTypeId );

            // get the person Ids along with the PersonImport and GroupMember record

            var personsIds = from p in qryAllPersons.AsNoTracking().Where( a => !string.IsNullOrEmpty( a.ForeignKey ) && a.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                                .Select( a => new { a.Id, a.ForeignKey } ).ToList()
                             join pi in personImportList on p.ForeignKey equals pi.PersonForeignKey
                             join f in groupService.Queryable().Where( a => !string.IsNullOrEmpty( a.ForeignKey ) && a.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) && a.GroupTypeId == FamilyGroupTypeId )
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

            // narrow it down to just person records that we inserted
            var personsIdsForPersonImport = personsIds.Where( a => insertedPersonForeignKeys.Contains( a.PersonImport.PersonForeignKey ) );

            // Make the GroupMember records for all the imported person (unless they are already have a groupmember record for the family)
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

            return personsToInsert.Count;
        }

        /// <summary>
        /// Processes the PersonAddress list.
        /// </summary>
        private int ImportPersonAddresses()
        {
            this.ReportProgress( 0, "Preparing Person Address data for import..." );

            var rockContext = new RockContext();
            if ( this.FamilyDict == null )
            {
                LoadFamilyDict( rockContext );
            }

            var familyAddressImports = new List<GroupAddressImport>();
            var familyAddressErrors = string.Empty;
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
            var addressCsvObjects = this.PersonAddressCsvList
                .Select( a => new
                    {
                        Family = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, a.PersonId ) )?.PrimaryFamily,
                        FamilyForeignKey = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, a.PersonId ) )?.PrimaryFamily.ForeignKey,
                        AddressType = a.AddressType,
                        Street1 = a.Street1,
                        Street2 = a.Street2,
                        PersonAddressCsv = a
                    } )
                .GroupBy( ao => new { FamilyForeignKey = ao.FamilyForeignKey, AddressType = ao.AddressType, Street1 = ao.Street1, Street2 = ao.Street2 } )
                .Select( ao => new { FamilyForeignKey = ao.Key.FamilyForeignKey, Family = ao.FirstOrDefault().Family, PersonAddressCsv = ao.FirstOrDefault().PersonAddressCsv } );

            var addressesNoFamilyMatch = addressCsvObjects.Where( a => a.Family == null || a.Family.Id <= 0 ).ToList();
            if ( addressesNoFamilyMatch.Count > 0 )
            {
                var errorMsg = $"{addressesNoFamilyMatch.Count} Addresses found with invalid or missing Person or Family records and will be skipped. Affected PersonIds are:\r\n";
                errorMsg += string.Join( ", ", addressesNoFamilyMatch.Select( a => a.PersonAddressCsv.PersonId ) );
                LogException( "Address", errorMsg );
            }
            var addressCsvObjectsToProcess = addressCsvObjects.Where( a => a.Family != null && a.Family.Id > 0 && !groupLocationLookup.ContainsKey( string.Format( "{0}^{1}", ImportInstanceFKPrefix, a.PersonAddressCsv.AddressId.IsNotNullOrWhiteSpace() ? a.PersonAddressCsv.AddressId : string.Format( "{0}_{1}", a.Family.Id, a.PersonAddressCsv.AddressType.ToString() ) ) ) ).ToList();
            this.ReportProgress( 0, $"{this.PersonAddressCsvList.Count - addressCsvObjectsToProcess.Count} Addresses already exist. Preparing {addressCsvObjectsToProcess.Count} Person Address records for processing." );

            foreach ( var addressCsv in addressCsvObjectsToProcess )
            {
                if ( string.IsNullOrEmpty( addressCsv.PersonAddressCsv.Street1 ) )
                {
                    familyAddressErrors += $"{DateTime.Now}, PersonAddress, Blank Street Address for PersonId {addressCsv.PersonAddressCsv.PersonId}, Address Type {addressCsv.PersonAddressCsv.AddressType}. Person Address was skipped.\r\n";
                    continue;
                }
                if ( addressCsv.Family == null )
                {
                    familyAddressErrors += $"{DateTime.Now}, PersonAddress, Family for PersonId {addressCsv.PersonAddressCsv.PersonId} not found. Person Address was skipped.\r\n";
                    continue;
                }

                var groupLocationTypeValueId = GetGroupLocationTypeDVId( addressCsv.PersonAddressCsv.AddressType );

                if ( groupLocationTypeValueId.HasValue )
                {
                    var newGroupAddress = new GroupAddressImport()
                    {
                        GroupId = addressCsv.Family.Id,
                        GroupLocationTypeValueId = groupLocationTypeValueId.Value,
                        IsMailingLocation = addressCsv.PersonAddressCsv.IsMailing,
                        IsMappedLocation = addressCsv.PersonAddressCsv.AddressType == "Home",
                        Street1 = addressCsv.PersonAddressCsv.Street1.Left( 100 ),
                        Street2 = addressCsv.PersonAddressCsv.Street2.Left( 100 ),
                        City = addressCsv.PersonAddressCsv.City.Left( 50 ),
                        State = addressCsv.PersonAddressCsv.State.Left( 50 ),
                        Country = addressCsv.PersonAddressCsv.Country.Left( 50 ),
                        PostalCode = addressCsv.PersonAddressCsv.PostalCode.Left( 50 ),
                        Latitude = addressCsv.PersonAddressCsv.Latitude.AsDoubleOrNull(),
                        Longitude = addressCsv.PersonAddressCsv.Longitude.AsDoubleOrNull(),
                        AddressForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, addressCsv.PersonAddressCsv.AddressId.IsNotNullOrWhiteSpace() ? addressCsv.PersonAddressCsv.AddressId : string.Format( "{0}_{1}", addressCsv.Family.Id, addressCsv.PersonAddressCsv.AddressType.ToString() ) )
                    };

                    familyAddressImports.Add( newGroupAddress );
                }
                else
                {
                    familyAddressErrors += $"{DateTime.Now}, PersonAddress, Unexpected Address Type ({addressCsv.PersonAddressCsv.AddressType}) encountered for Person \"{addressCsv.PersonAddressCsv.PersonId}\". Person Address was skipped.\r\n";
                }
            }

            this.ReportProgress( 0, string.Format( "Begin processing {0} Group Address Records...", familyAddressImports.Count ) );

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
        /// Processes the Person AttributeValue list.
        /// </summary>
        private int ImportPersonAttributeValues()
        {
            ReportProgress( 0, "Preparing Person Attribute Value data for import..." );
            
            var rockContext = new RockContext();
            var personAVImports = new List<AttributeValueImport>();
            var personAVErrors = string.Empty;
            var personAttributeValues = this.PersonAttributeValueCsvList.DistinctBy( av => new { av.AttributeKey, av.PersonId } ).ToList();  // Protect against duplicates in import data

            var attributeDefinedValuesDict = GetAttributeDefinedValuesDictionary( rockContext, PersonEntityTypeId );
            var attributeValueLookup = GetAttributeValueLookup( rockContext, PersonEntityTypeId );

            foreach ( var attributeValueCsv in this.PersonAttributeValueCsvList )
            {
                var person = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, attributeValueCsv.PersonId ) );
                if ( person == null )
                {
                    personAVErrors += $"{DateTime.Now}, PersonAttributeValue, PersonId {attributeValueCsv.PersonId} not found. AttributeValue for {attributeValueCsv.AttributeKey} attribute was skipped.\r\n";
                    continue;
                }

                var attribute = this.PersonAttributeDict.GetValueOrNull( attributeValueCsv.AttributeKey );
                if ( attribute == null )
                {
                    personAVErrors += $"{DateTime.Now}, PersonAttributeValue, AttributeKey {attributeValueCsv.AttributeKey} not found. AttributeValue for PersonId {attributeValueCsv.PersonId} was skipped.\r\n";
                    continue;
                }

                if ( attributeValueLookup.Any( l => l.Item1 == attribute.Id && l.Item2 == person.Id ) )
                {
                    personAVErrors += $"{DateTime.Now}, PersonAttributeValue, AttributeValue for AttributeKey {attributeValueCsv.AttributeKey} and PersonId {attributeValueCsv.PersonId} already exists. AttributeValueId {attributeValueCsv.AttributeValueId} was skipped.\r\n";
                    continue;
                }

                var newAttributeValue = new AttributeValueImport()
                {
                    AttributeId = attribute.Id,
                    AttributeValueForeignId = attributeValueCsv.AttributeValueId.AsIntegerOrNull(),
                    EntityId = person.Id,
                    AttributeValueForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, attributeValueCsv.AttributeValueId.IsNotNullOrWhiteSpace() ? attributeValueCsv.AttributeValueId : string.Format( "{0}_{1}", attributeValueCsv.PersonId, attributeValueCsv.AttributeKey ) )
                };

                newAttributeValue.Value = GetAttributeValueStringByAttributeType( rockContext, attributeValueCsv.AttributeValue, attribute, attributeDefinedValuesDict );

                personAVImports.Add( newAttributeValue );
            }

            this.ReportProgress( 0, string.Format( "Begin processing {0} Person Attribute Value Records...", personAVImports.Count ) );
            if ( personAVErrors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, personAVErrors, hasMultipleErrors: true );
            }
            return ImportAttributeValues( personAVImports );
        }

        private int ImportPersonPhones()
        {
            this.ReportProgress( 0, "Preparing Person Phone data for import..." );

            var personPhoneImports = new List<PhoneNumberImport>();
            var errors = string.Empty;

            foreach ( var phoneCsv in this.PersonPhoneCsvList )
            {
                var person = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, phoneCsv.PersonId ) );
                if ( person == null )
                {
                    errors += $"{DateTime.Now}, PersonPhone, PersonId {phoneCsv.PersonId} not found. {phoneCsv.PhoneType} Phone number was skipped.\r\n";
                    continue;
                }
                var newPersonPhone = new PhoneNumberImport
                {
                    NumberTypeValueId = this.PhoneNumberTypeDVDict[phoneCsv.PhoneType].Id,
                    Number = phoneCsv.PhoneNumber,
                    IsMessagingEnabled = phoneCsv.IsMessagingEnabled ?? false,
                    IsUnlisted = phoneCsv.IsUnlisted ?? false,
                    CountryCode = phoneCsv.CountryCode,
                    Extension = phoneCsv.Extension,
                    PersonId = person.Id,
                    PhoneId = phoneCsv.PhoneId
                };
                personPhoneImports.Add( newPersonPhone );
            }

            this.ReportProgress( 0, string.Format( "Begin processing {0} Person Phone Records...", personPhoneImports.Count ) );

            // Slice data into chunks and process
            var personPhonesRemainingToProcess = personPhoneImports.Count;
            var workingPersonPhoneImportList = personPhoneImports.ToList();
            var completed = 0;

            while ( personPhonesRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} Phone Numbers processed." );
                }

                if ( completed % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingPersonPhoneImportList.Take( Math.Min( this.DefaultChunkSize, workingPersonPhoneImportList.Count ) ).ToList();
                    var imported = BulkPersonPhoneImport( csvChunk );
                    completed += imported;
                    personPhonesRemainingToProcess -= csvChunk.Count;
                    workingPersonPhoneImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }
            if ( errors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, errors, hasMultipleErrors: true );
            }
            return completed;
        }

        private int BulkPersonPhoneImport( List<PhoneNumberImport> phoneImports, RockContext rockContext = null )
        {
            if ( rockContext == null )
            {
                rockContext = new RockContext();
            }
            var phoneNumbersToInsert = new List<PhoneNumber>();
            foreach ( var phoneNumberImport in phoneImports )
            {
                var newPhoneNumber = new PhoneNumber();
                newPhoneNumber.PersonId = phoneNumberImport.PersonId;
                UpdatePhoneNumberFromPhoneNumberImport( phoneNumberImport, newPhoneNumber, RockDateTime.Now );
                phoneNumbersToInsert.Add( newPhoneNumber );
            }

            rockContext.BulkInsert( phoneNumbersToInsert );
            return phoneImports.Count;
        }

        private int ImportPersonSearchKeys()
        {
            if ( this.PersonSearchKeyDict == null )
            {
                LoadPersonSearchKeyDict();
            }
            var rockContext = new RockContext();
            this.ReportProgress( 0, "Preparing Person Search Key data for import..." );

            var searchKeyTypeDVEmailId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.PERSON_SEARCH_KEYS_EMAIL ).Id;
            var searchKeyTypeDVAltId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.PERSON_SEARCH_KEYS_ALTERNATE_ID ).Id;
            var personSearchImports = new List<PersonSearchKeyImport>();
            var errors = string.Empty;
            var personAliasIdLookupFromPersonId = new PersonAliasService( rockContext ).Queryable().Where( a => !string.IsNullOrEmpty( a.ForeignKey ) && a.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) && a.PersonId == a.AliasPersonId )
                .Select( a => new { PersonAliasId = a.Id, PersonId = a.PersonId } ).ToDictionary( k => k.PersonId, v => v.PersonAliasId );

            foreach ( var searchKeyCsv in this.PersonSearchKeyCsvList )
            {
                var person = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, searchKeyCsv.PersonId ) );
                if ( person == null )
                {
                    errors += $"{DateTime.Now}, SearchKey, PersonId {searchKeyCsv.PersonId} not found. {searchKeyCsv.SearchValue} Search Key was skipped.\r\n";
                    continue;
                }
                var personAliasId = personAliasIdLookupFromPersonId.GetValueOrNull( person.Id );
                if ( !personAliasId.HasValue )
                {
                    errors += $"{DateTime.Now}, SearchKey, PersonId {searchKeyCsv.PersonId} does not have a valid PersonAlias record. {searchKeyCsv.SearchValue} Search Key was skipped.\r\n";
                    continue;
                }

                var searchType = PersonSearchKeyType.Email;
                Enum.TryParse( searchKeyCsv.SearchType, out searchType );

                var newPersonSearchKeyImport = new PersonSearchKeyImport
                {
                    PersonId = person.Id,
                    PersonAliasId = personAliasId.Value,
                    SearchValue = searchKeyCsv.SearchValue,
                    SearchTypeDefinedValueId = searchType == PersonSearchKeyType.Email ? searchKeyTypeDVEmailId : searchKeyTypeDVAltId,
                    ForeignKey = string.Format( "{0}^{1}_{2}_{3}", ImportInstanceFKPrefix, searchKeyCsv.PersonId, ( int ) searchType, searchKeyCsv.SearchValue )
                };
                personSearchImports.Add( newPersonSearchKeyImport );
            }

            var searchKeyLookup = this.PersonSearchKeyDict.ToDictionary( k => k.Key, v => v.Value );

            var searchKeyImportsToProcess = personSearchImports.Where( k => !searchKeyLookup.ContainsKey( k.ForeignKey ) ).ToList();
            if ( personSearchImports.Count != searchKeyImportsToProcess.Count )
            {
                this.ReportProgress( 0, string.Format( "{0} PersonSearchKeys already exist and will be skipped.", personSearchImports.Count - searchKeyImportsToProcess.Count ) );
            }
            this.ReportProgress( 0, string.Format( "Begin processing {0} Person Search Key records...", searchKeyImportsToProcess.Count ) );

            // Slice data into chunks and process
            var searchKeysRemainingToProcess = searchKeyImportsToProcess.Count;
            var workingSearchKeyImportList = searchKeyImportsToProcess.ToList();
            var completed = 0;

            while ( searchKeysRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} Search Keys processed." );
                }

                if ( completed % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingSearchKeyImportList.Take( Math.Min( this.DefaultChunkSize, workingSearchKeyImportList.Count ) ).ToList();
                    var imported = BulkSearchKeyImport( csvChunk, searchKeyLookup );
                    completed += imported;
                    searchKeysRemainingToProcess -= csvChunk.Count;
                    workingSearchKeyImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }
            if ( errors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, errors, hasMultipleErrors: true );
            }

            LoadPersonSearchKeyDict( rockContext );
            return completed;
        }

        private int BulkSearchKeyImport( List<PersonSearchKeyImport> searchKeyImports, Dictionary<string, PersonSearchKey> searchKeyLookup, RockContext rockContext = null )
        {
            if ( rockContext == null )
            {
                rockContext = new RockContext();
            }
            var searchKeysToInsert = new List<PersonSearchKey>();
            foreach ( var searchKeyImport in searchKeyImports )
            {
                var newPersonSearchKey = new PersonSearchKey
                {
                    PersonAliasId = searchKeyImport.PersonAliasId,
                    SearchValue = searchKeyImport.SearchValue.Left( 255 ),
                    SearchTypeValueId = searchKeyImport.SearchTypeDefinedValueId,
                    ForeignKey = searchKeyImport.ForeignKey
                };
                searchKeysToInsert.Add( newPersonSearchKey );
            }

            rockContext.BulkInsert( searchKeysToInsert );
            return searchKeyImports.Count;
        }

        #region Previous Last Names

        /// <summary>
        /// Loads the Person Previous Name data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadPersonPreviousName( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var importedPersonPreviousNames = new PersonPreviousNameService( lookupContext ).Queryable().Count( p => p.ForeignKey != null && p.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) );

            var personPreviousNames = new List<PersonPreviousName>();

            var completedItems = 0;
            ReportProgress( 0, string.Format( "Verifying person previous name import ({0:N0} already imported).", importedPersonPreviousNames ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var previousPersonNameId = row[PreviousLastNameId] as string;
                var previousPersonName = row[PreviousLastName] as string;
                var previousPersonId = row[PreviousLastNamePersonId] as string;

                var personAliasKey = previousPersonId;
                var previousNamePersonKeys = GetPersonKeys( personAliasKey );
                if ( previousNamePersonKeys != null )
                {
                    var previousPersonAliasId = previousNamePersonKeys.PersonAliasId;

                    var previousName = AddPersonPreviousName( lookupContext, previousPersonName, previousPersonAliasId, $"{this.ImportInstanceFKPrefix}^{previousPersonNameId}", false );

                    if ( previousName.Id == 0 )
                    {
                        personPreviousNames.Add( previousName );
                    }
                }

                completedItems++;
                if ( completedItems % ( DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} person previous names processed.", completedItems ) );
                }

                if ( completedItems % DefaultChunkSize < 1 )
                {
                    SavePersonPreviousNames( personPreviousNames );
                    ReportPartialProgress();
                    personPreviousNames.Clear();
                }
            }

            if ( personPreviousNames.Any() )
            {
                SavePersonPreviousNames( personPreviousNames );
            }

            ReportProgress( 100, string.Format( "Finished person previous name import: {0:N0} previous names processed.", completedItems ) );
            return completedItems;
        }

        /// <summary>
        /// Saves the person previous names.
        /// </summary>
        /// <param name="personPreviousNames">The previous last names list.</param>
        private static void SavePersonPreviousNames( List<PersonPreviousName> personPreviousNames )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.PersonPreviousNames.AddRange( personPreviousNames );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }

        #endregion Previous Last Names
    }
}