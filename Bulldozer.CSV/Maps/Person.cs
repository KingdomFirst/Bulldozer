using Bulldozer.Model;
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using static Bulldozer.CSV.CSVInstance;

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
        /// <param name="bwWorker">The bw worker.</param>
        private int ImportPersonList()
        {
            this.ReportProgress( 0, "Preparing Person data for import..." );
            var personImportList = GetPersonImportList();

            this.ReportProgress( 0, string.Format( "Bulk Importing {0} Person Records...", personImportList.Count ) );

            // Slice data into chunks and process
            var peopleRemainingToProcess = personImportList.Count;
            var completed = 0;

            while ( peopleRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.PersonChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} Person records processed.", completed ) );
                }

                if ( completed % ( this.PersonChunkSize ) < 1 )
                {
                    var csvChunk = personImportList.Take( Math.Min( this.PersonChunkSize, personImportList.Count ) ).ToList();
                    completed += BulkPersonImport( csvChunk );
                    peopleRemainingToProcess -= csvChunk.Count;
                    personImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            return completed;
        }

        /// <summary>
        /// Gets the PersonImport list.
        /// <returns></returns>
        protected List<PersonImport> GetPersonImportList()
        {
            var personImportList = new List<PersonImport>();

            var familyRolesLookup = GroupTypeCache.GetFamilyGroupType().Roles.ToDictionary( k => k.Guid );

            var gradeOffsetLookupFromDescription = DefinedTypeCache.Get( Rock.SystemGuid.DefinedType.SCHOOL_GRADES.AsGuid() )?.DefinedValues
                .ToDictionary( k => k.Description, v => v.Value.AsInteger(), StringComparer.OrdinalIgnoreCase );

            var gradeOffsetLookupFromAbbreviation = DefinedTypeCache.Get( Rock.SystemGuid.DefinedType.SCHOOL_GRADES.AsGuid() )?.DefinedValues
                .Select( a => new { Value = a.Value.AsInteger(), Abbreviation = a.AttributeValues["Abbreviation"]?.Value } )
                .Where( a => !string.IsNullOrWhiteSpace( a.Abbreviation ) )
                .ToDictionary( k => k.Abbreviation, v => v.Value, StringComparer.OrdinalIgnoreCase );

            foreach ( var personCsv in this.PersonCsvList )
            {
                if ( personCsv.Id.IsNullOrWhiteSpace() )
                {
                    LogException( "Person", string.Format( "Missing Id for Person '{0} {1}'. Skipping.", personCsv.FirstName, personCsv.LastName ) );
                    continue;
                }

                if ( personCsv.FamilyId.IsNullOrWhiteSpace() )
                {
                    LogException( "Person", string.Format( "Missing FamilyId for Person {0}. Skipping.", personCsv.Id ) );
                    continue;
                }

                var newPerson = new PersonImport
                {
                    RecordTypeValueId = 1,
                    PersonForeignId = personCsv.Id.AsIntegerOrNull(),
                    PersonForeignKey = string.Format( "{0}_{1}", this.ImportInstanceFKPrefix, personCsv.Id ),
                    FamilyForeignId = personCsv.FamilyId.AsIntegerOrNull(),
                    FamilyForeignKey = string.Format( "{0}_{1}", this.ImportInstanceFKPrefix, personCsv.FamilyId ),
                    FamilyName = personCsv.FamilyName,
                    InactiveReasonNote = personCsv.InactiveReasonNote.IsNullOrWhiteSpace() ? personCsv.InactiveReason : personCsv.InactiveReasonNote,
                    RecordStatusReasonValueId = this.RecordStatusReasonDVDict.Values.FirstOrDefault( v => v.Value.Equals( personCsv.InactiveReason ) )?.Id,
                    IsDeceased = personCsv.IsDeceased.HasValue ? personCsv.IsDeceased.Value : false,
                    FirstName = personCsv.FirstName,
                    NickName = personCsv.NickName,
                    MiddleName = personCsv.MiddleName,
                    LastName = personCsv.LastName,
                    AnniversaryDate = personCsv.AnniversaryDate.ToSQLSafeDate(),
                    Grade = personCsv.Grade,
                    Email = personCsv.Email,
                    IsEmailActive = personCsv.IsEmailActive.HasValue ? personCsv.IsEmailActive.Value : true,
                    CreatedDateTime = personCsv.CreatedDateTime.ToSQLSafeDate(),
                    ModifiedDateTime = personCsv.ModifiedDateTime.ToSQLSafeDate(),
                    Note = personCsv.Note,
                    GivingIndividually = personCsv.GiveIndividually.HasValue ? personCsv.GiveIndividually.Value : false,
                    PhoneNumbers = new List<PhoneNumberImport>(),
                    Addresses = new List<PersonAddressImport>(),
                    AttributeValues = new List<AttributeValueImport>()
                };

                switch ( personCsv.FamilyRole )
                {
                    case CSVInstance.FamilyRole.Adult:
                        newPerson.GroupRoleId = familyRolesLookup[Rock.SystemGuid.GroupRole.GROUPROLE_FAMILY_MEMBER_ADULT.AsGuid()].Id;
                        break;

                    case CSVInstance.FamilyRole.Child:
                        newPerson.GroupRoleId = familyRolesLookup[Rock.SystemGuid.GroupRole.GROUPROLE_FAMILY_MEMBER_CHILD.AsGuid()].Id;
                        break;
                }

                if ( personCsv.Campus != null )
                {
                    var campusIdInt = personCsv.Campus.CampusId.AsIntegerOrNull();
                    if ( campusIdInt.HasValue && campusIdInt.Value > 0 )
                    {
                        newPerson.CampusId = this.CampusList.FirstOrDefault( c => c.ForeignId == campusIdInt ).Id;
                    }
                    else if ( !campusIdInt.HasValue && personCsv.Campus.CampusId.IsNotNullOrWhiteSpace() )
                    {
                        newPerson.CampusId = this.CampusList.FirstOrDefault( c => c.ForeignKey == string.Format( "{0}_{1}", ImportInstanceFKPrefix, personCsv.Campus.CampusId ) ).Id;
                    }
                    else if ( personCsv.Campus.CampusName.IsNotNullOrWhiteSpace() )
                    {
                        newPerson.CampusId = this.CampusList.FirstOrDefault( c => c.Name == personCsv.Campus.CampusName ).Id;
                    }
                }

                switch ( personCsv.RecordStatus )
                {
                    case CSVInstance.RecordStatus.Active:
                        newPerson.RecordStatusValueId = this.RecordStatusDVDict[Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_ACTIVE.AsGuid()]?.Id;
                        break;

                    case CSVInstance.RecordStatus.Inactive:
                        newPerson.RecordStatusValueId = this.RecordStatusDVDict[Rock.SystemGuid.DefinedValue.PERSON_RECORD_STATUS_INACTIVE.AsGuid()]?.Id;
                        break;

                    case CSVInstance.RecordStatus.Pending:
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

                switch ( personCsv.Gender )
                {
                    case Rock.Model.Gender.Male:
                        newPerson.Gender = Rock.Model.Gender.Male.ConvertToInt();
                        break;

                    case Rock.Model.Gender.Female:
                        newPerson.Gender = Rock.Model.Gender.Female.ConvertToInt();
                        break;

                    case Rock.Model.Gender.Unknown:
                        newPerson.Gender = Rock.Model.Gender.Unknown.ConvertToInt();
                        break;
                }

                if ( !string.IsNullOrEmpty( personCsv.MaritalStatus ) )
                {
                    newPerson.MaritalStatusValueId = MaritalStatusDVDict[personCsv.MaritalStatus]?.Id;
                }

                // do a case-insensitive lookup GradeOffset from either the Description ("Kindergarten", "1st Grade", etc) or Abbreviation ("K", "1st", etc)
                int? gradeOffset = null;
                if ( !string.IsNullOrWhiteSpace( newPerson.Grade ) )
                {
                    gradeOffset = gradeOffsetLookupFromDescription.GetValueOrNull( newPerson.Grade );
                    if ( gradeOffset == null )
                    {
                        gradeOffset = gradeOffsetLookupFromAbbreviation.GetValueOrNull( newPerson.Grade );
                    }
                }

                if ( gradeOffset.HasValue )
                {
                    newPerson.GraduationYear = Person.GraduationYearFromGradeOffset( gradeOffset );
                }

                switch ( personCsv.EmailPreference )
                {
                    case EmailPreference.EmailAllowed:
                        newPerson.EmailPreference = EmailPreference.EmailAllowed.ConvertToInt();
                        break;

                    case EmailPreference.DoNotEmail:
                        newPerson.EmailPreference = EmailPreference.DoNotEmail.ConvertToInt();
                        break;

                    case EmailPreference.NoMassEmails:
                        newPerson.EmailPreference = EmailPreference.NoMassEmails.ConvertToInt();
                        break;
                }

                // Person Search Keys
                newPerson.PersonSearchKeys = new List<PersonSearchKeyImport>();
                foreach ( var searchKey in personCsv.PersonSearchKeys )
                {
                    var newPersonSearchKeyImport = new PersonSearchKeyImport
                    {
                        PersonId = personCsv.Id,
                        SearchValue = searchKey.SearchValue,
                        SearchType = searchKey.SearchType.ConvertToInt()
                    };
                    newPerson.PersonSearchKeys.Add( newPersonSearchKeyImport );
                }
                if ( personCsv.AlternateEmails.IsNotNullOrWhiteSpace() )
                {
                    foreach ( var email in personCsv.AlternateEmails.Split( new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries ).Distinct() )
                    {
                        var newPersonSearchKeyImport = new PersonSearchKeyImport
                        {
                            PersonId = personCsv.Id,
                            SearchValue = email,
                            SearchType = CSVInstance.PersonSearchKeyType.Email.ConvertToInt()
                        };
                        newPerson.PersonSearchKeys.Add( newPersonSearchKeyImport );
                    }
                }

                // Phone Numbers
                foreach ( var phoneNumber in personCsv.PhoneNumbers )
                {
                    var newPersonPhone = new PhoneNumberImport
                    {
                        NumberTypeValueId = this.PhoneNumberTypeDVDict[phoneNumber.PhoneType].Id,
                        Number = phoneNumber.PhoneNumber,
                        IsMessagingEnabled = phoneNumber.IsMessagingEnabled ?? false,
                        IsUnlisted = phoneNumber.IsUnlisted ?? false,
                        CountryCode = phoneNumber.CountryCode
                    };
                    newPerson.PhoneNumbers.Add( newPersonPhone );
                }

                // Addresses
                foreach ( var address in personCsv.Addresses )
                {
                    if ( string.IsNullOrEmpty( address.Street1 ) )
                    {
                        continue;
                    }
                    int? groupLocationTypeValueId = null;
                    switch ( address.AddressType )
                    {
                        case AddressType.Home:
                            groupLocationTypeValueId = GroupLocationTypeDVDict[Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_HOME.AsGuid()].Id;
                            break;

                        case AddressType.Previous:
                            groupLocationTypeValueId = GroupLocationTypeDVDict[Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_PREVIOUS.AsGuid()].Id;
                            break;

                        case AddressType.Work:
                            groupLocationTypeValueId = GroupLocationTypeDVDict[Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_WORK.AsGuid()].Id;
                            break;

                        case AddressType.Other:
                            groupLocationTypeValueId = GroupLocationTypeDVDict[Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_OTHER.AsGuid()].Id;
                            break;
                    }

                    if ( groupLocationTypeValueId.HasValue )
                    {
                        var newPersonAddress = new PersonAddressImport()
                        {
                            GroupLocationTypeValueId = groupLocationTypeValueId.Value,
                            IsMailingLocation = address.IsMailing,
                            IsMappedLocation = address.AddressType == AddressType.Home,
                            Street1 = address.Street1.Left( 100 ),
                            Street2 = address.Street2.Left( 100 ),
                            City = address.City.Left( 50 ),
                            State = address.State.Left( 50 ),
                            Country = address.Country.Left( 50 ),
                            PostalCode = address.PostalCode.Left( 50 ),
                            Latitude = address.Latitude.AsDoubleOrNull(),
                            Longitude = address.Longitude.AsDoubleOrNull()
                        };

                        newPerson.Addresses.Add( newPersonAddress );
                    }
                    else
                    {
                        LogException( "PersonAddress", $"Unexpected Address Type ( {address.AddressType} ) for PersonId {address.PersonId}." );
                    }
                }

                // Attribute Values
                foreach ( var attributeValue in personCsv.Attributes )
                {
                    int attributeId = this.PersonAttributeDict[attributeValue.AttributeKey].Id;
                    var newAttributeValue = new AttributeValueImport()
                    {
                        AttributeId = attributeId,
                        Value = attributeValue.AttributeValue,
                        AttributeValueForeignId = attributeValue.AttributeValueId.AsIntegerOrNull(),
                        AttributeValueForeignKey = string.Format( "{0}_{1}", this.ImportInstanceFKPrefix, attributeValue.AttributeValueId )
                    };
                    newPerson.AttributeValues.Add( newAttributeValue );
                }

                personImportList.Add( newPerson );
            }

            return personImportList;
        }

        /// <summary>
        /// Bulk import of PersonImports.
        /// </summary>
        /// <param name="personImports">The person imports.</param>
        /// <returns></returns>
        public int BulkPersonImport( List<PersonImport> personImports )
        {
            var rockContext = new RockContext();
            var qryAllPersons = new PersonService( rockContext ).Queryable( true, true );
            var groupService = new GroupService( rockContext );
            var groupMemberService = new GroupMemberService( rockContext );
            var locationService = new LocationService( rockContext );

            var familyGroupType = GroupTypeCache.GetFamilyGroupType();
            int familyGroupTypeId = familyGroupType.Id;
            int familyChildRoleId = familyGroupType.Roles.First( a => a.Guid == Rock.SystemGuid.GroupRole.GROUPROLE_FAMILY_MEMBER_CHILD.AsGuid() ).Id;
            var recordTypePersonId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.PERSON_RECORD_TYPE_PERSON.AsGuid() ).Id;
            int personSeachKeyTypeAlternateId = DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.PERSON_SEARCH_KEYS_ALTERNATE_ID.AsGuid() ).Id;

            var familiesLookup = groupService.Queryable().AsNoTracking().Where( a => a.GroupTypeId == familyGroupTypeId && a.ForeignKey.IsNotNullOrWhiteSpace() && a.ForeignKey.Split( '_' )[0] == this.ImportInstanceFKPrefix )
                .ToList().ToDictionary( k => k.ForeignKey, v => v );

            var personLookup = qryAllPersons.Include( a => a.PhoneNumbers ).AsNoTracking().Where( a => a.ForeignKey.IsNotNullOrWhiteSpace() && a.ForeignKey.Split( '_' )[0] == this.ImportInstanceFKPrefix )
                .ToList().ToDictionary( k => k.ForeignKey, v => v );

            var defaultPhoneCountryCode = PhoneNumber.DefaultCountryCode();

            var importDateTime = RockDateTime.Now;

            int nextNewFamilyForeignId = familiesLookup.Any( a => a.Value.ForeignId.HasValue ) ? familiesLookup.Max( a => a.Value.ForeignId.Value ) : 0;
            if ( personImports.Any() )
            {
                nextNewFamilyForeignId = Math.Max( nextNewFamilyForeignId, personImports.Where( a => a.FamilyForeignId.HasValue ).Max( a => a.FamilyForeignId.Value ) );
            }

            EntityTypeAttributesCache.Clear();

            var entityTypeIdPerson = EntityTypeCache.Get<Person>().Id;
            var attributeValuesLookup = new AttributeValueService( rockContext ).Queryable().Where( a => a.Attribute.EntityTypeId == entityTypeIdPerson && a.EntityId.HasValue )
                .Select( a => new
                {
                    PersonId = a.EntityId.Value,
                    a.AttributeId,
                    a.Value
                } )
                .GroupBy( a => a.PersonId )
                .ToDictionary(
                    k => k.Key,
                    v => v.Select( x => new AttributeValueCache { AttributeId = x.AttributeId, EntityId = x.PersonId, Value = x.Value } ).ToList() );

            int personUpdatesCount = 0;
            int total = personImports.Count();

            foreach ( var personImport in personImports )
            {
                Group family = null;

                if ( personImport.FamilyForeignKey.IsNullOrWhiteSpace() )
                {
                    personImport.FamilyForeignId = ++nextNewFamilyForeignId;
                    personImport.FamilyForeignKey = string.Format( "{0}_{1}", this.ImportInstanceFKPrefix, nextNewFamilyForeignId );
                }

                if ( familiesLookup.ContainsKey( personImport.FamilyForeignKey ) )
                {
                    family = familiesLookup[personImport.FamilyForeignKey];
                }

                if ( family == null )
                {
                    family = new Group
                    {
                        GroupTypeId = familyGroupTypeId,
                        Name = string.IsNullOrEmpty( personImport.FamilyName ) ? personImport.LastName : personImport.FamilyName,
                        CampusId = personImport.CampusId,
                        ForeignId = personImport.FamilyForeignId,
                        ForeignKey = personImport.FamilyForeignKey,
                        CreatedDateTime = personImport.CreatedDateTime.ToSQLSafeDate() ?? importDateTime,
                        ModifiedDateTime = personImport.ModifiedDateTime.ToSQLSafeDate() ?? importDateTime
                    };

                    if ( string.IsNullOrWhiteSpace( family.Name ) )
                    {
                        family.Name = "Family";
                    }
                    familiesLookup.Add( personImport.FamilyForeignKey, family );
                }

                Person person = null;
                if ( personLookup.ContainsKey( personImport.PersonForeignKey ) )
                {
                    person = personLookup[personImport.PersonForeignKey];
                }

                if ( person == null )
                {
                    person = new Person();
                    InitializePersonFromPersonImport( personImport, person, recordTypePersonId );
                    personLookup.Add( personImport.PersonForeignKey, person );
                }
                else if ( this.ImportUpdateOption == ImportUpdateType.AlwaysUpdate )
                {
                    bool wasChanged = UpdatePersonFromPersonImport( person, personImport, attributeValuesLookup, familiesLookup, importDateTime, recordTypePersonId );
                    if ( wasChanged )
                    {
                        personUpdatesCount++;
                    }
                }
            }
            var insertedPersonForeignIds = new List<string>();

            // insert all the [Group] records
            var familiesToInsert = familiesLookup.Where( a => a.Value.Id == 0 ).Select( a => a.Value ).ToList();

            // insert all the [Person] records.
            var personsToInsert = personLookup.Where( a => a.Value.Id == 0 ).Select( a => a.Value ).ToList();

            rockContext.BulkInsert( familiesToInsert );

            // lookup GroupId from Group.ForeignId
            var familyIdLookup = groupService.Queryable().AsNoTracking().Where( a => a.GroupTypeId == familyGroupTypeId && a.ForeignKey.IsNotNullOrWhiteSpace() && a.ForeignKey.Split( '_' )[0] == this.ImportInstanceFKPrefix )
                .ToList().ToDictionary( k => k.ForeignKey, v => v.Id );

            var personToInsertLookup = personsToInsert.ToDictionary( k => k.ForeignKey, v => v );

            // now that we have GroupId for each family, set the GivingGroupId for personImport's that don't give individually
            foreach ( var personImport in personImports )
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

            insertedPersonForeignIds = personsToInsert.Select( a => a.ForeignKey ).ToList();

            // Make sure everybody has a PersonAlias
            var personAliasService = new PersonAliasService( rockContext );
            var personAliasServiceQry = personAliasService.Queryable();
            var personAliasesToInsert = qryAllPersons.Where( p => p.ForeignKey.IsNotNullOrWhiteSpace() && p.ForeignKey.Split( '_' )[0] == this.ImportInstanceFKPrefix && !p.Aliases.Any() && !personAliasServiceQry.Any( pa => pa.AliasPersonId == p.Id ) )
                                                     .Select( x => new { x.Id, x.Guid, x.ForeignId, x.ForeignKey } )
                                                     .ToList()
                                                     .Select( person => new PersonAlias { AliasPersonId = person.Id, AliasPersonGuid = person.Guid, PersonId = person.Id, ForeignId = person.ForeignId, ForeignKey = person.ForeignKey } ).ToList();

            rockContext.BulkInsert( personAliasesToInsert );

            var familyGroupMembersQry = new GroupMemberService( rockContext ).Queryable( true ).Where( a => a.Group.GroupTypeId == familyGroupTypeId );

            // get the person Ids along with the PersonImport and GroupMember record
            var personsIds = from p in qryAllPersons.AsNoTracking().Where( a => a.ForeignKey.IsNotNullOrWhiteSpace() && a.ForeignKey.Split( '_' )[0] == this.ImportInstanceFKPrefix )
                                .Select( a => new { a.Id, a.ForeignKey } ).ToList()
                                join pi in personImports on p.ForeignKey equals pi.PersonForeignKey
                                join f in groupService.Queryable().Where( a => a.ForeignKey.IsNotNullOrWhiteSpace() && a.ForeignKey.Split( '_' )[0] == this.ImportInstanceFKPrefix && a.GroupTypeId == familyGroupTypeId )
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
            var personsIdsForPersonImport = personsIds.Where( a => insertedPersonForeignIds.Contains( a.PersonImport.PersonForeignKey ) );

            // Make the GroupMember records for all the imported person (unless they are already have a groupmember record for the family)
            var groupMemberRecordsToInsertQry = from ppi in personsIdsForPersonImport
                                                where !ppi.HasGroupMemberRecord
                                                select new GroupMember
                                                {
                                                    PersonId = ppi.PersonId,
                                                    GroupRoleId = ppi.PersonImport.GroupRoleId,
                                                    GroupId = ppi.FamilyId,
                                                    GroupTypeId = GroupTypeCache.Get( Rock.SystemGuid.GroupType.GROUPTYPE_FAMILY.AsGuid() ).Id,
                                                    GroupMemberStatus = GroupMemberStatus.Active,
                                                    CreatedDateTime = ppi.PersonImport.CreatedDateTime.ToSQLSafeDate() ?? importDateTime,
                                                    ModifiedDateTime = ppi.PersonImport.ModifiedDateTime.ToSQLSafeDate() ?? importDateTime,
                                                };

            var groupMemberRecordsToInsertList = groupMemberRecordsToInsertQry.ToList();

            rockContext.BulkInsert( groupMemberRecordsToInsertList );

            var locationsToInsert = new List<Location>();
            var groupLocationsToInsert = new List<GroupLocation>();

            var locationCreatedDateTimeStart = RockDateTime.Now;

            foreach ( var familyRecord in personsIdsForPersonImport.GroupBy( a => a.FamilyId ) )
            {
                // get the distinct addresses for each family in our import
                var familyAddresses = familyRecord.Where( a => a.PersonImport?.Addresses != null ).SelectMany( a => a.PersonImport.Addresses ).DistinctBy( a => new { a.GroupLocationTypeValueId, a.Street1, a.Street2, a.City, a.County, a.State } ).ToList();

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

            var personAliasIdLookupFromPersonId = new PersonAliasService( rockContext ).Queryable().Where( a => a.ForeignKey.IsNotNullOrWhiteSpace() && a.ForeignKey.Split( '_' )[0] == this.ImportInstanceFKPrefix && a.PersonId == a.AliasPersonId )
                .Select( a => new { PersonAliasId = a.Id, PersonId = a.PersonId } ).ToDictionary( k => k.PersonId, v => v.PersonAliasId );

            // PersonSearchKeys
            List<PersonSearchKey> personSearchKeysToInsert = new List<PersonSearchKey>();

            foreach ( var personsId in personsIdsForPersonImport )
            {
                var personAliasId = personAliasIdLookupFromPersonId.GetValueOrNull( personsId.PersonId );
                if ( personAliasId.HasValue )
                {
                    foreach ( var personSearchKeyImport in personsId.PersonImport.PersonSearchKeys )
                    {
                        var newPersonSearchKey = new PersonSearchKey
                        {
                            PersonAliasId = personAliasId.Value,
                            SearchValue = personSearchKeyImport.SearchValue.Left( 255 ),
                            SearchTypeValueId = personSearchKeyImport.SearchType
                        };

                        personSearchKeysToInsert.Add( newPersonSearchKey );
                    }
                }
            }

            rockContext.BulkInsert( personSearchKeysToInsert );

            // PhoneNumbers
            var phoneNumbersToInsert = new List<PhoneNumber>();

            foreach ( var personsId in personsIdsForPersonImport )
            {
                foreach ( var phoneNumberImport in personsId.PersonImport.PhoneNumbers )
                {

                    var newPhoneNumber = new PhoneNumber();
                    newPhoneNumber.PersonId = personsId.PersonId;
                    UpdatePhoneNumberFromPhoneNumberImport( phoneNumberImport, newPhoneNumber, importDateTime );
                    phoneNumbersToInsert.Add( newPhoneNumber );
                }
            }

            rockContext.BulkInsert( phoneNumbersToInsert );

            // Attribute Values
            var attributeValuesToInsert = new List<AttributeValue>();
            foreach ( var personsId in personsIdsForPersonImport )
            {
                foreach ( var attributeValueImport in personsId.PersonImport.AttributeValues )
                {
                    var newAttributeValue = new AttributeValue
                    {
                        EntityId = personsId.PersonId,
                        AttributeId = attributeValueImport.AttributeId,
                        Value = attributeValueImport.Value,
                        CreatedDateTime = personsId.PersonImport.CreatedDateTime.ToSQLSafeDate() ?? importDateTime,
                        ModifiedDateTime = personsId.PersonImport.ModifiedDateTime.ToSQLSafeDate() ?? importDateTime,
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

            return personsToInsert.Count;
        }
    }
}
