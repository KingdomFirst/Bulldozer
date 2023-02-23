// <copyright>
// Copyright 2022 by Kingdom First Solutions
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
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using static Bulldozer.Utility.CachedTypes;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the People import methods
    /// </summary>
    partial class CSVComponent
    {
        #region Main Methods

        /// <summary>
        /// Loads the individual data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadIndividuals( CSVInstance csvData )
        {
            var lookupContext = new RockContext();

            // Marital statuses: Married, Single, Separated, etc
            var maritalStatusTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.PERSON_MARITAL_STATUS ), lookupContext ).DefinedValues;

            // Connection statuses: Member, Visitor, Attendee, etc
            var connectionStatusTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.PERSON_CONNECTION_STATUS ), lookupContext ).DefinedValues;

            // Suffix types: Dr., Jr., II, etc
            var suffixTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.PERSON_SUFFIX ), lookupContext ).DefinedValues;

            // Title types: Mr., Mrs. Dr., etc
            var titleTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.PERSON_TITLE ), lookupContext ).DefinedValues;

            // Group roles: Owner, Adult, Child, others
            var familyRoles = GroupTypeCache.Get( new Guid( Rock.SystemGuid.GroupType.GROUPTYPE_FAMILY ), lookupContext ).Roles;

            // Phone types: Home, Work, Mobile
            var numberTypeValues = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.PERSON_PHONE_TYPE ), lookupContext ).DefinedValues;

            // School Person attribute
            var schoolAttribute = FindEntityAttribute( lookupContext, "Education", "School", PersonEntityTypeId );

            // Visit info category
            var visitInfoCategory = new CategoryService( lookupContext ).GetByEntityTypeId( PersonAttributeCategoryEntityTypeId )
                .FirstOrDefault( c => c.Name == "Visit Information" );

            // Look for custom attributes in the Individual file
            var allFields = csvData.TableNodes.FirstOrDefault().Children.Select( ( node, index ) => new { node = node, index = index } ).ToList();
            var customAttributes = allFields
                .Where( f => f.index > AlternateEmails )
                .ToDictionary( f => f.index, f => f.node.Name );

            var personAttributes = new List<Rock.Model.Attribute>();

            // Add any attributes if they don't already exist
            if ( customAttributes.Any() )
            {
                foreach ( var newAttributePair in customAttributes )
                {
                    var pairs = newAttributePair.Value.Split( '^' );
                    var categoryName = string.Empty;
                    var attributeName = string.Empty;
                    var attributeTypeString = string.Empty;
                    var attributeForeignKey = string.Empty;
                    var definedValueForeignKey = string.Empty;
                    var fieldTypeId = TextFieldTypeId;

                    if ( pairs.Length == 1 )
                    {
                        attributeName = pairs[0];
                    }
                    else if ( pairs.Length == 2 )
                    {
                        attributeName = pairs[0];
                        attributeTypeString = pairs[1];
                    }
                    else if ( pairs.Length >= 3 )
                    {
                        categoryName = pairs[1];
                        attributeName = pairs[2];
                        if ( pairs.Length >= 4 )
                        {
                            attributeTypeString = pairs[3];
                        }
                        if ( pairs.Length >= 5 )
                        {
                            attributeForeignKey = pairs[4];
                        }
                        if ( pairs.Length >= 6 )
                        {
                            definedValueForeignKey = pairs[5];
                        }
                    }

                    var definedValueForeignId = definedValueForeignKey.AsType<int?>();

                    //
                    // Translate the provided attribute type into one we know about.
                    //
                    fieldTypeId = GetAttributeFieldType( attributeTypeString );

                    if ( string.IsNullOrEmpty( attributeName ) )
                    {
                        LogException( "Individual", string.Format( "Individual Attribute Name cannot be blank '{0}'.", newAttributePair.Value ) );
                    }
                    else
                    {
                        //
                        // First try to find the existing attribute, if not found then add a new one.
                        //
                        var attribute = FindEntityAttribute( lookupContext, categoryName, attributeName, PersonEntityTypeId, attributeForeignKey );
                        if ( attribute == null )
                        {
                            var fk = string.Empty;
                            if ( string.IsNullOrWhiteSpace( attributeForeignKey ) )
                            {
                                fk = string.Format( "Bulldozer_{0}_{1}", categoryName.RemoveWhitespace(), attributeName.RemoveWhitespace() ).Left( 100 );
                            }
                            else
                            {
                                fk = attributeForeignKey;
                            }

                            attribute = AddEntityAttribute( lookupContext, PersonEntityTypeId, string.Empty, string.Empty, fk, categoryName, attributeName,
                                string.Empty, fieldTypeId, true, definedValueForeignId, definedValueForeignKey, attributeTypeString: attributeTypeString );
                        }
                        personAttributes.Add( attribute );
                    }
                }
            }

            var currentFamilyGroup = new Group();
            var newFamilyList = new List<Group>();
            var newFamilyMembers = new List<GroupMember>();
            var newVisitorList = new List<Group>();
            var newNoteList = new List<Note>();
            var alternateEmails = new List<PersonSearchKey>();

            var completed = 0;
            var newFamilies = 0;
            var newPeople = 0;
            ReportProgress( 0, string.Format( "Starting Individual import ({0:N0} already exist).", ImportedPeopleKeys.Count ) );

            string[] row;
            row = csvData.Database.FirstOrDefault();
            while ( row != null )
            {
                int? groupRoleId = null;
                var isFamilyRelationship = true;

                var rowFamilyName = row[FamilyName];
                var rowFamilyKey = row[FamilyId];
                var rowPersonKey = row[PersonId];
                var rowPreviousPersonKeys = row[PreviousPersonIds];
                var rowAlternateEmails = row[AlternateEmails];
                var rowFamilyId = rowFamilyKey.AsType<int?>();
                var rowPersonId = rowPersonKey.AsType<int?>();

                // Check that this person isn't already in our data

                var personKeys = GetPersonKeys( rowPersonKey );

                // If they aren't already in our data, check if past Ids are provided and look for them too.
                if ( personKeys == null && !string.IsNullOrWhiteSpace( rowPreviousPersonKeys ) )
                {
                    foreach ( var key in rowPreviousPersonKeys.Split( new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries ) )
                    {
                        personKeys = GetPersonKeys( key.Trim() );
                        if ( personKeys != null )
                        {
                            using ( RockContext context = new RockContext() )
                            {
                                var person = new PersonService( context ).Get( personKeys.PersonId );
                                var pa = new PersonAlias
                                {
                                    PersonId = personKeys.PersonId,
                                    AliasPersonId = -rowPersonId,
                                    ForeignKey = rowPersonKey,
                                    ForeignId = rowPersonId,
                                    Guid = Guid.NewGuid()
                                };
                                person.Aliases.Add( pa );
                                context.SaveChanges();
                                ImportedPeopleKeys.Add( new PersonKeys
                                {
                                    PersonAliasId = pa.Id,
                                    GroupForeignId = rowFamilyId,
                                    PersonId = pa.PersonId,
                                    PersonForeignId = pa.ForeignId,
                                    PersonForeignKey = pa.ForeignKey
                                } );
                            }
                            break;
                        }
                    }
                }

                if ( personKeys == null )
                {
                    #region person create

                    var person = new Person
                    {
                        ForeignKey = rowPersonKey,
                        ForeignId = rowPersonId,
                        SystemNote = string.Format( "Imported via Bulldozer on {0}", ImportDateTime ),
                        RecordTypeValueId = PersonRecordTypeId,
                        CreatedByPersonAliasId = ImportPersonAliasId
                    };
                    var firstName = row[FirstName].Left( 50 );
                    var nickName = row[NickName].Left( 50 );
                    person.FirstName = firstName;
                    person.NickName = string.IsNullOrWhiteSpace( nickName ) ? firstName : nickName;
                    person.MiddleName = row[MiddleName].Left( 50 );
                    person.LastName = row[LastName].Left( 50 );

                    var createdDateValue = ParseDateOrDefault( row[CreatedDate], null );
                    if ( createdDateValue.HasValue )
                    {
                        person.CreatedDateTime = createdDateValue;
                        person.ModifiedDateTime = ImportDateTime;
                    }
                    else
                    {
                        person.CreatedDateTime = ImportDateTime;
                        person.ModifiedDateTime = ImportDateTime;
                    }

                    var birthDate = ParseDateOrDefault( row[DateOfBirth], null );
                    if ( birthDate.HasValue )
                    {
                        person.BirthDay = ( ( DateTime ) birthDate ).Day;
                        person.BirthMonth = ( ( DateTime ) birthDate ).Month;
                        person.BirthYear = ( ( DateTime ) birthDate ).Year;
                    }

                    var graduationDate = ParseDateOrDefault( row[GraduationDate], null );
                    if ( graduationDate.HasValue )
                    {
                        person.GraduationYear = ( ( DateTime ) graduationDate ).Year;
                    }

                    var anniversary = ParseDateOrDefault( row[Anniversary], null );
                    if ( anniversary.HasValue )
                    {
                        person.AnniversaryDate = anniversary;
                    }

                    var gender = row[Gender];
                    if ( gender != null )
                    {
                        switch ( gender.Trim().ToLower() )
                        {
                            case "m":
                            case "male":
                                person.Gender = Rock.Model.Gender.Male;
                                break;

                            case "f":
                            case "female":
                                person.Gender = Rock.Model.Gender.Female;
                                break;

                            default:
                                person.Gender = Rock.Model.Gender.Unknown;
                                break;
                        }
                    }

                    var prefix = row[Prefix];
                    if ( !string.IsNullOrWhiteSpace( prefix ) )
                    {
                        prefix = prefix.RemoveSpecialCharacters();
                        person.TitleValueId = titleTypes.Where( s => prefix.Equals( s.Value.RemoveSpecialCharacters(), StringComparison.OrdinalIgnoreCase ) )
                            .Select( s => ( int? ) s.Id ).FirstOrDefault();

                        if ( !person.TitleValueId.HasValue )
                        {
                            var newTitle = AddDefinedValue( lookupContext, Rock.SystemGuid.DefinedType.PERSON_TITLE, prefix );
                            if ( newTitle != null )
                            {
                                titleTypes.Add( newTitle );
                                person.TitleValueId = newTitle.Id;
                            }
                        }
                    }

                    var suffix = row[Suffix];
                    if ( !string.IsNullOrWhiteSpace( suffix ) )
                    {
                        suffix = suffix.RemoveSpecialCharacters();
                        person.SuffixValueId = suffixTypes.Where( s => suffix.Equals( s.Value.RemoveSpecialCharacters(), StringComparison.OrdinalIgnoreCase ) )
                            .Select( s => ( int? ) s.Id ).FirstOrDefault();

                        if ( !person.SuffixValueId.HasValue )
                        {
                            var newSuffix = AddDefinedValue( lookupContext, Rock.SystemGuid.DefinedType.PERSON_SUFFIX, suffix );
                            if ( newSuffix != null )
                            {
                                suffixTypes.Add( newSuffix );
                                person.SuffixValueId = newSuffix.Id;
                            }
                        }
                    }

                    var maritalStatus = row[MaritalStatus];
                    if ( !string.IsNullOrWhiteSpace( maritalStatus ) )
                    {
                        maritalStatus = maritalStatus.RemoveSpecialCharacters();
                        person.MaritalStatusValueId = maritalStatusTypes.Where( s => maritalStatus.Equals( s.Value.RemoveSpecialCharacters(), StringComparison.OrdinalIgnoreCase ) )
                            .Select( dv => ( int? ) dv.Id ).FirstOrDefault();

                        if ( !person.MaritalStatusValueId.HasValue )
                        {
                            var newMaritalStatus = AddDefinedValue( lookupContext, Rock.SystemGuid.DefinedType.PERSON_MARITAL_STATUS, maritalStatus );
                            if ( newMaritalStatus != null )
                            {
                                maritalStatusTypes.Add( newMaritalStatus );
                                person.MaritalStatusValueId = newMaritalStatus.Id;
                            }
                        }
                    }

                    if ( person.MaritalStatusValueId == null )
                    {
                        person.MaritalStatusValueId = maritalStatusTypes.Where( dv => dv.Value.Equals( "Unknown", StringComparison.OrdinalIgnoreCase ) )
                            .Select( dv => ( int? ) dv.Id ).FirstOrDefault();
                    }

                    var familyRole = row[FamilyRole];
                    if ( !string.IsNullOrWhiteSpace( familyRole ) )
                    {
                        familyRole = familyRole.RemoveSpecialCharacters().Trim();
                        groupRoleId = familyRoles.Where( dv => string.Equals( dv.Name, familyRole, StringComparison.OrdinalIgnoreCase ) )
                            .Select( dv => ( int? ) dv.Id ).FirstOrDefault();

                        if ( !groupRoleId.HasValue )
                        {
                            AddGroupRole( lookupContext, Rock.SystemGuid.GroupType.GROUPTYPE_FAMILY, familyRole );
                            familyRoles = GroupTypeCache.Get( Rock.SystemGuid.GroupType.GROUPTYPE_FAMILY ).Roles;
                            groupRoleId = familyRoles.Where( dv => dv.Name == familyRole )
                                .Select( dv => ( int? ) dv.Id ).FirstOrDefault();
                        }

                        if ( familyRole.Equals( "Visitor", StringComparison.OrdinalIgnoreCase ) )
                        {
                            isFamilyRelationship = false;
                        }
                    }

                    if ( groupRoleId == null )
                    {
                        groupRoleId = FamilyAdultRoleId;
                    }

                    var recordStatus = row[RecordStatus];
                    if ( !string.IsNullOrWhiteSpace( recordStatus ) )
                    {
                        switch ( recordStatus.Trim().ToLower() )
                        {
                            case "active":
                                person.RecordStatusValueId = ActivePersonRecordStatusId;
                                break;

                            case "inactive":
                                person.RecordStatusValueId = InactivePersonRecordStatusId;
                                break;

                            default:
                                person.RecordStatusValueId = PendingPersonRecordStatusId;
                                break;
                        }
                    }
                    else
                    {
                        person.RecordStatusValueId = ActivePersonRecordStatusId;
                    }

                    var connectionStatus = row[ConnectionStatus];
                    if ( !string.IsNullOrWhiteSpace( connectionStatus ) )
                    {
                        if ( connectionStatus.Equals( "Member", StringComparison.OrdinalIgnoreCase ) )
                        {
                            person.ConnectionStatusValueId = MemberConnectionStatusId;
                        }
                        else if ( connectionStatus.Equals( "Visitor", StringComparison.OrdinalIgnoreCase ) )
                        {
                            person.ConnectionStatusValueId = VisitorConnectionStatusId;
                        }
                        else if ( connectionStatus.Equals( "Business", StringComparison.OrdinalIgnoreCase ) )
                        {
                            person.RecordTypeValueId = BusinessRecordTypeId;
                        }
                        else if ( connectionStatus.Equals( "Inactive", StringComparison.OrdinalIgnoreCase ) )
                        {
                            person.RecordStatusValueId = InactivePersonRecordStatusId;
                        }
                        else
                        {
                            // create user-defined connection type if it doesn't exist
                            person.ConnectionStatusValueId = connectionStatusTypes.Where( dv => dv.Value.Equals( connectionStatus, StringComparison.OrdinalIgnoreCase ) )
                                .Select( dv => ( int? ) dv.Id ).FirstOrDefault();

                            if ( !person.ConnectionStatusValueId.HasValue )
                            {
                                var newConnectionStatus = AddDefinedValue( lookupContext, Rock.SystemGuid.DefinedType.PERSON_CONNECTION_STATUS, connectionStatus );
                                if ( newConnectionStatus != null )
                                {
                                    connectionStatusTypes.Add( newConnectionStatus );
                                    person.ConnectionStatusValueId = newConnectionStatus.Id;
                                }
                            }
                        }
                    }
                    else
                    {
                        person.ConnectionStatusValueId = VisitorConnectionStatusId;
                    }

                    var isDeceasedValue = row[IsDeceased];
                    if ( !string.IsNullOrWhiteSpace( isDeceasedValue ) )
                    {
                        switch ( isDeceasedValue.Trim().ToLower() )
                        {
                            case "y":
                            case "yes":
                            case "true":
                                person.IsDeceased = true;
                                person.RecordStatusReasonValueId = DeceasedPersonRecordReasonId;
                                person.RecordStatusValueId = InactivePersonRecordStatusId;
                                break;

                            default:
                                person.IsDeceased = false;
                                break;
                        }
                    }

                    var personNumbers = new Dictionary<string, string>();
                    personNumbers.Add( "Home", row[HomePhone] );
                    personNumbers.Add( "Mobile", row[MobilePhone] );
                    personNumbers.Add( "Work", row[WorkPhone] );
                    var smsAllowed = row[AllowSMS];

                    foreach ( var numberPair in personNumbers.Where( n => !string.IsNullOrWhiteSpace( n.Value ) && n.Value.AsNumeric().AsType<Int64>() > 0 ) )
                    {
                        var extension = string.Empty;
                        var countryCode = PhoneNumber.DefaultCountryCode();
                        var normalizedNumber = string.Empty;
                        var countryIndex = numberPair.Value.IndexOf( '+' );
                        var extensionIndex = numberPair.Value.LastIndexOf( 'x' ) > 0 ? numberPair.Value.LastIndexOf( 'x' ) : numberPair.Value.Length;
                        if ( countryIndex >= 0 && numberPair.Value.Length > ( countryIndex + 3 ) )
                        {
                            countryCode = numberPair.Value.Substring( countryIndex, countryIndex + 3 ).AsNumeric();
                            normalizedNumber = numberPair.Value.Substring( countryIndex + 3, extensionIndex - 3 ).AsNumeric().TrimStart( new Char[] { '0' } );
                            extension = numberPair.Value.Substring( extensionIndex );
                        }
                        else if ( extensionIndex > 0 )
                        {
                            normalizedNumber = numberPair.Value.Substring( 0, extensionIndex ).AsNumeric();
                            extension = numberPair.Value.Substring( extensionIndex ).AsNumeric();
                        }
                        else
                        {
                            normalizedNumber = numberPair.Value.AsNumeric();
                        }

                        if ( !string.IsNullOrWhiteSpace( normalizedNumber ) )
                        {
                            var currentNumber = new PhoneNumber();
                            currentNumber.CountryCode = countryCode;
                            currentNumber.CreatedByPersonAliasId = ImportPersonAliasId;
                            currentNumber.Extension = extension.Left( 20 );
                            currentNumber.Number = normalizedNumber.TrimStart( new char[] { '0' } ).Left( 20 );
                            currentNumber.NumberFormatted = PhoneNumber.FormattedNumber( currentNumber.CountryCode, currentNumber.Number );
                            currentNumber.NumberTypeValueId = numberTypeValues.Where( v => v.Value.Equals( numberPair.Key, StringComparison.OrdinalIgnoreCase ) )
                                .Select( v => ( int? ) v.Id ).FirstOrDefault();
                            if ( numberPair.Key == "Mobile" )
                            {
                                switch ( smsAllowed.Trim().ToLower() )
                                {
                                    case "y":
                                    case "yes":
                                    case "active":
                                    case "true":
                                        currentNumber.IsMessagingEnabled = true;
                                        break;

                                    default:
                                        currentNumber.IsMessagingEnabled = false;
                                        break;
                                }
                            }

                            person.PhoneNumbers.Add( currentNumber );
                        }
                    }

                    // Map Person attributes
                    person.Attributes = new Dictionary<string, AttributeCache>();
                    person.AttributeValues = new Dictionary<string, AttributeValueCache>();

                    bool isEmailActive;
                    switch ( row[IsEmailActive].Trim().ToLower() )
                    {
                        case "n":
                        case "no":
                        case "inactive":
                        case "false":
                            isEmailActive = false;
                            break;

                        default:
                            isEmailActive = true;
                            break;
                    }

                    EmailPreference emailPreference;
                    switch ( row[AllowBulkEmail].Trim().ToLower() )
                    {
                        case "n":
                        case "no":
                        case "inactive":
                        case "false":
                            emailPreference = EmailPreference.NoMassEmails;
                            break;

                        default:
                            emailPreference = EmailPreference.EmailAllowed;
                            break;
                    }

                    person.EmailPreference = emailPreference;
                    var primaryEmail = row[Email].Trim().Left( 75 );
                    if ( !string.IsNullOrWhiteSpace( primaryEmail ) )
                    {
                        if ( person.Email.IsEmail() )
                        {
                            person.Email = primaryEmail;
                            person.IsEmailActive = isEmailActive;
                        }
                        else
                        {
                            LogException( "InvalidPrimaryEmail", string.Format( "PersonId: {0} - Email: {1}", rowPersonKey, primaryEmail ) );
                        }
                    }

                    var schoolName = row[School];
                    if ( !string.IsNullOrWhiteSpace( schoolName ) )
                    {
                        AddEntityAttributeValue( lookupContext, schoolAttribute, person, schoolName, null, true );
                    }

                    //
                    // Add any Individual attribute values
                    //
                    foreach ( var attributePair in customAttributes )
                    {
                        var newValue = row[attributePair.Key];

                        if ( !string.IsNullOrWhiteSpace( newValue ) )
                        {
                            var pairs = attributePair.Value.Split( '^' );
                            var categoryName = string.Empty;
                            var attributeName = string.Empty;
                            var attributeTypeString = string.Empty;
                            var attributeForeignKey = string.Empty;
                            var definedValueForeignKey = string.Empty;

                            if ( pairs.Length == 1 )
                            {
                                attributeName = pairs[0];
                            }
                            else if ( pairs.Length == 2 )
                            {
                                attributeName = pairs[0];
                                attributeTypeString = pairs[1];
                            }
                            else if ( pairs.Length >= 3 )
                            {
                                categoryName = pairs[1];
                                attributeName = pairs[2];
                                if ( pairs.Length >= 4 )
                                {
                                    attributeTypeString = pairs[3];
                                }
                                if ( pairs.Length >= 5 )
                                {
                                    attributeForeignKey = pairs[4];
                                }
                                if ( pairs.Length >= 6 )
                                {
                                    definedValueForeignKey = pairs[5];
                                }
                            }

                            // look up the local copy of this attribute
                            if ( !string.IsNullOrEmpty( attributeName ) )
                            {
                                var attributeQueryable = personAttributes.AsQueryable()
                                    .Where( a => a.EntityTypeId == PersonEntityTypeId && a.Name.Equals( attributeName, StringComparison.OrdinalIgnoreCase ) );

                                if ( !string.IsNullOrEmpty( attributeForeignKey ) )
                                {
                                    attributeQueryable = attributeQueryable.Where( a => a.ForeignKey.Equals( attributeForeignKey, StringComparison.OrdinalIgnoreCase ) );
                                }

                                if ( !string.IsNullOrEmpty( categoryName ) )
                                {
                                    attributeQueryable = attributeQueryable.Where( a => a.Categories.Any( c => c.Name.Equals( categoryName, StringComparison.OrdinalIgnoreCase ) ) );
                                }
                                else
                                {
                                    attributeQueryable = attributeQueryable.Where( a => !a.Categories.Any() );
                                }

                                var attribute = attributeQueryable.FirstOrDefault();
                                if ( attribute == null )
                                {
                                    attribute = FindEntityAttribute( lookupContext, categoryName, attributeName, PersonEntityTypeId, attributeForeignKey );
                                    personAttributes.Add( attribute );
                                }

                                AddEntityAttributeValue( lookupContext, attribute, person, newValue, null, true );
                            }
                        }
                    }

                    // Add notes to timeline
                    var notePairs = new Dictionary<string, string>();
                    notePairs.Add( "General", row[GeneralNote] );
                    notePairs.Add( "Medical", row[MedicalNote] );
                    notePairs.Add( "Security", row[SecurityNote] );

                    foreach ( var notePair in notePairs.Where( n => !string.IsNullOrWhiteSpace( n.Value ) ) )
                    {
                        var splitNotePair = notePair.Value.Split( '^' );
                        foreach ( string noteValue in splitNotePair )
                        {
                            var newNote = new Note
                            {
                                NoteTypeId = PersonalNoteTypeId,
                                CreatedByPersonAliasId = ImportPersonAliasId,
                                CreatedDateTime = ImportDateTime,
                                Text = noteValue,
                                ForeignKey = rowPersonKey,
                                ForeignId = rowPersonId,
                                Caption = string.Format( "{0} Note", notePair.Key )
                            };

                            if ( noteValue.StartsWith( "[ALERT]", StringComparison.OrdinalIgnoreCase ) )
                            {
                                newNote.IsAlert = true;
                            }

                            if ( notePair.Key.Equals( "Security" ) )
                            {
                                // Pastoral note type id
                                var securityNoteType = new NoteTypeService( lookupContext ).Get( PersonEntityTypeId, "Secure Note", true );
                                if ( securityNoteType != null )
                                {
                                    newNote.NoteTypeId = securityNoteType.Id;
                                }
                            }

                            if ( notePair.Key.Equals( "Medical" ) )
                            {
                                newNote.IsAlert = true;
                            }

                            newNoteList.Add( newNote );
                        }
                    }

                    // Add any additional emails as search keys
                    if ( !string.IsNullOrWhiteSpace( rowAlternateEmails ) )
                    {
                        var emailSearcKeyDVId = DefinedValueCache.Get( new Guid( Rock.SystemGuid.DefinedValue.PERSON_SEARCH_KEYS_EMAIL ) ).Id; 
                        foreach ( var email in rowAlternateEmails.Split( new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries ).Distinct() )
                        {
                            var emailSearchKey = new PersonSearchKey
                            {
                                SearchTypeValueId = emailSearcKeyDVId,
                                ForeignKey = rowPersonKey,
                                ForeignId = rowPersonId,
                                SearchValue = email.ToLower().Trim()
                            };
                            alternateEmails.Add( emailSearchKey );
                        }
                    }

                    #endregion person create

                    var groupMember = new GroupMember
                    {
                        Person = person,
                        GroupRoleId = ( int ) groupRoleId,
                        CreatedDateTime = ImportDateTime,
                        ModifiedDateTime = ImportDateTime,
                        CreatedByPersonAliasId = ImportPersonAliasId,
                        GroupMemberStatus = GroupMemberStatus.Active,
                        GroupTypeId = FamilyGroupTypeId
                    };

                    if ( rowFamilyKey != currentFamilyGroup.ForeignKey )
                    {
                        // person not part of the previous family, see if that family exists or create a new one
                        currentFamilyGroup = ImportedFamilies.FirstOrDefault( g => g.ForeignKey == rowFamilyKey );
                        if ( currentFamilyGroup == null )
                        {
                            currentFamilyGroup = CreateFamilyGroup( row[FamilyName], rowFamilyKey );
                            newFamilyList.Add( currentFamilyGroup );
                            newFamilies++;
                            currentFamilyGroup.Members.Add( groupMember );
                        }
                        else
                        {
                            groupMember.GroupId = currentFamilyGroup.Id;
                            lookupContext.Groups.Attach( currentFamilyGroup );
                            newFamilyMembers.Add( groupMember );
                        }

                    }
                    else
                    {
                        // person is part of this family group, check if they're a visitor
                        if ( isFamilyRelationship || currentFamilyGroup.Members.Count() < 1 )
                        {
                            currentFamilyGroup.Members.Add( groupMember );
                        }
                        else
                        {
                            var visitorFamily = CreateFamilyGroup( person.LastName + " Family", rowFamilyKey );
                            visitorFamily.Members.Add( groupMember );
                            newFamilyList.Add( visitorFamily );
                            newVisitorList.Add( visitorFamily );
                            newFamilies++;
                        }
                    }

                    // look ahead 1 row
                    var rowNextFamilyKey = "-1";
                    if ( ( row = csvData.Database.FirstOrDefault() ) != null )
                    {
                        rowNextFamilyKey = row[FamilyId];
                    }

                    newPeople++;
                    completed++;
                    if ( completed % ( ReportingNumber * 10 ) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} people processed.", completed ) );
                    }

                    if ( newPeople >= ReportingNumber && rowNextFamilyKey != currentFamilyGroup.ForeignKey )
                    {
                        SaveIndividuals( lookupContext, newFamilyList, newVisitorList, newNoteList, alternateEmails, newFamilyMembers );
                        lookupContext.SaveChanges();
                        ReportPartialProgress();

                        // Clear out variables
                        currentFamilyGroup = new Group();
                        newFamilyList.Clear();
                        newFamilyMembers.Clear();
                        newVisitorList.Clear();
                        newNoteList.Clear();
                        alternateEmails.Clear();
                        newPeople = 0;
                    }
                }
                else
                {
                    row = csvData.Database.FirstOrDefault();
                }
            }

            // Save any changes to new families or new family members
            if ( newFamilyList.Any() || newFamilyMembers.Any() )
            {
                SaveIndividuals( lookupContext, newFamilyList, newVisitorList, newNoteList, alternateEmails, newFamilyMembers );
            }

            // Save any changes to existing families
            lookupContext.SaveChanges();
            DetachAllInContext( lookupContext );
            lookupContext.Dispose();

            ReportProgress( 0, string.Format( "Finished individual import: {0:N0} families and {1:N0} people added.", newFamilies, completed ) );
            return completed;
        }

        /// <summary>
        /// Creates the family group.
        /// </summary>
        /// <param name="rowFamilyName">Name of the row family.</param>
        /// <param name="rowFamilyKey">The row family identifier.</param>
        /// <returns></returns>
        private static Group CreateFamilyGroup( string rowFamilyName, string rowFamilyKey )
        {
            var familyGroup = new Group();
            if ( !string.IsNullOrWhiteSpace( rowFamilyName ) )
            {
                familyGroup.Name = rowFamilyName;
            }
            else
            {
                familyGroup.Name = string.Format( "Family Group {0}", rowFamilyKey );
            }

            familyGroup.CreatedByPersonAliasId = ImportPersonAliasId;
            familyGroup.GroupTypeId = FamilyGroupTypeId;
            familyGroup.ForeignKey = rowFamilyKey;
            familyGroup.ForeignId = rowFamilyKey.AsType<int?>();
            return familyGroup;
        }

        /// <summary>
        /// Saves the individuals.
        /// </summary>
        /// <param name="mainRockContext">The Rock context.</param>
        /// <param name="newFamilyList">The family list.</param>
        /// <param name="visitorList">The optional visitor list.</param>
        /// <param name="newNoteList">The new note list.</param>
        /// <param name="alternateEmailKeys">The alternate email key list.</param>
        /// <param name="newFamilyMembers">The new family member list.</param>
        private void SaveIndividuals( RockContext mainRockContext, List<Group> newFamilyList, List<Group> visitorList = null, List<Note> newNoteList = null, List<PersonSearchKey> alternateEmailKeys = null, List<GroupMember> newFamilyMembers = null )
        {
            if ( newFamilyMembers == null )
            {
                newFamilyMembers = new List<GroupMember>();
            }
            if ( newFamilyList.Any() || newFamilyMembers.Any() )
            {
                var rockContext = new RockContext();
                rockContext.WrapTransaction( () =>
                {
                    if ( newFamilyList.Any() )
                    {
                        rockContext.Groups.AddRange( newFamilyList );
                    }
                    if ( newFamilyMembers.Any() )
                    {
                        rockContext.GroupMembers.AddRange( newFamilyMembers );
                    }
                    rockContext.SaveChanges( DisableAuditing );

                    // #TODO find out how to track family groups without context locks
                    ImportedFamilies.AddRange( newFamilyList );

                    var newPersonForeignIds = new List<int>();

                    foreach ( var familyGroups in newFamilyList.GroupBy( g => g.ForeignKey ) )
                    {
                        var visitorsExist = visitorList.Any() && familyGroups.Any();
                        foreach ( var newFamilyGroup in familyGroups )
                        {
                            foreach ( var person in newFamilyGroup.Members.Select( m => m.Person ) )
                            {
                                BuildNewPerson( newNoteList, alternateEmailKeys, rockContext, newFamilyGroup.Id, person );
                                newPersonForeignIds.Add( person.ForeignId.Value );
                                if ( visitorsExist )
                                {
                                    // Retrieve or create the group this person is an owner of
                                    var ownerGroup = new GroupMemberService( rockContext ).Queryable()
                                        .Where( m => m.PersonId == person.Id && m.GroupRoleId == KnownRelationshipOwnerRoleId )
                                        .Select( m => m.Group ).FirstOrDefault();
                                    if ( ownerGroup == null )
                                    {
                                        var ownerGroupMember = new GroupMember
                                        {
                                            PersonId = person.Id,
                                            GroupRoleId = KnownRelationshipOwnerRoleId
                                        };

                                        ownerGroup = new Group
                                        {
                                            Name = KnownRelationshipGroupType.Name,
                                            GroupTypeId = KnownRelationshipGroupType.Id
                                        };
                                        ownerGroup.Members.Add( ownerGroupMember );
                                        rockContext.Groups.Add( ownerGroup );
                                    }

                                    // Visitor, add relationships to the family members
                                    if ( visitorList.Where( v => v.ForeignKey == newFamilyGroup.ForeignKey )
                                            .Any( v => v.Members.Any( m => m.Person.ForeignKey.Equals( person.ForeignKey ) ) ) )
                                    {
                                        var familyMembers = familyGroups.Except( visitorList ).SelectMany( g => g.Members );
                                        foreach ( var familyMember in familyMembers )
                                        {
                                            // Add visitor invitedBy relationship
                                            var invitedByMember = new GroupMember
                                            {
                                                PersonId = familyMember.Person.Id,
                                                GroupRoleId = InvitedByKnownRelationshipId
                                            };

                                            ownerGroup.Members.Add( invitedByMember );

                                            if ( person.Age < 18 && familyMember.Person.Age > 15 )
                                            {
                                                // Add visitor allowCheckInBy relationship
                                                var allowCheckinMember = new GroupMember
                                                {
                                                    PersonId = familyMember.Person.Id,
                                                    GroupRoleId = AllowCheckInByKnownRelationshipId
                                                };

                                                ownerGroup.Members.Add( allowCheckinMember );
                                            }
                                        }
                                    }
                                    else
                                    {   // Family member, add relationships to the visitor(s)
                                        var familyVisitors = visitorList.Where( v => v.ForeignKey == newFamilyGroup.ForeignKey ).SelectMany( g => g.Members ).ToList();
                                        foreach ( var visitor in familyVisitors )
                                        {
                                            // Add invited visitor relationship
                                            var inviteeMember = new GroupMember
                                            {
                                                PersonId = visitor.Person.Id,
                                                GroupRoleId = InviteeKnownRelationshipId
                                            };

                                            ownerGroup.Members.Add( inviteeMember );

                                            if ( visitor.Person.Age < 18 && person.Age > 15 )
                                            {
                                                // Add canCheckIn visitor relationship
                                                var canCheckInMember = new GroupMember
                                                {
                                                    PersonId = visitor.Person.Id,
                                                    GroupRoleId = CanCheckInKnownRelationshipId
                                                };

                                                ownerGroup.Members.Add( canCheckInMember );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    foreach ( GroupMember famMember in newFamilyMembers )
                    {
                        BuildNewPerson( newNoteList, alternateEmailKeys, rockContext, famMember.GroupId, famMember.Person );
                        newPersonForeignIds.Add( famMember.Person.ForeignId.Value );
                    }

                    // Save notes and all changes
                    rockContext.Notes.AddRange( newNoteList );
                    rockContext.SaveChanges( DisableAuditing );

                    // Set email person search keys
                    if ( alternateEmailKeys.Count > 0 )
                    {
                        foreach ( var foreignId in newPersonForeignIds )
                        {
                            Person importedPerson = null;
                            if ( newFamilyList.Any() )
                            {
                                var importedPersonObj = newFamilyList
                                                    .SelectMany( m => m.Members )
                                                    .Select( m => m.Person )
                                                    .FirstOrDefault( p => p.ForeignId == foreignId );
                                if ( importedPersonObj != null )
                                {
                                    importedPerson = new PersonService( rockContext ).Get( importedPersonObj.Guid );
                                }
                            }
                            if ( importedPerson == null && newFamilyMembers.Any() )
                            {
                                importedPerson = new PersonService( rockContext ).Get( newFamilyMembers
                                                    .Select( m => m.Person )
                                                    .FirstOrDefault( p => p.ForeignId == foreignId ).Guid );
                            }
                            if ( importedPerson != null )
                            {
                                var emailPersonSearchKeys = alternateEmailKeys.Where( e => e.ForeignId == importedPerson.ForeignId ).ToList();
                                if ( emailPersonSearchKeys.Any() )
                                {
                                    emailPersonSearchKeys.ForEach( k => k.PersonAliasId = importedPerson.PrimaryAliasId );
                                }
                            }
                        }
                        mainRockContext.PersonSearchKeys.AddRange( alternateEmailKeys );
                    }

                    if ( refreshIndividualListEachCycle )
                    {
                        // add reference to imported people now that we have ID's
                        ImportedPeopleKeys.AddRange(
                            newFamilyList.Where( m => m.ForeignKey != null )
                            .SelectMany( m => m.Members )
                            .Select( p => new PersonKeys
                            {
                                PersonAliasId = ( int ) p.Person.PrimaryAliasId,
                                GroupForeignId = p.Group.ForeignId,
                                PersonId = p.Person.Id,
                                PersonForeignId = p.Person.ForeignId,
                                PersonForeignKey = p.Person.ForeignKey
                            } )
                        );
                        ImportedPeopleKeys.AddRange(
                            newFamilyMembers.Where( m => m.ForeignKey != null )
                            .Select( p => new PersonKeys
                            {
                                PersonAliasId = ( int ) p.Person.PrimaryAliasId,
                                GroupForeignId = p.Group.ForeignId,
                                PersonId = p.Person.Id,
                                PersonForeignId = p.Person.ForeignId,
                                PersonForeignKey = p.Person.ForeignKey
                            } )
                        );
                        ImportedPeopleKeys = ImportedPeopleKeys.OrderBy( k => k.PersonForeignId ).ThenBy( k => k.PersonForeignKey ).ToList();
                    }
                } );
            }
        }

        private static void BuildNewPerson( List<Note> newNoteList, List<PersonSearchKey> alternateEmails, RockContext rockContext, int familyGroupId, Person newPerson )
        {
            // Set notes on this person
            var personNotes = newNoteList.Where( n => n.ForeignKey == newPerson.ForeignKey ).ToList();
            if ( personNotes.Any() )
            {
                personNotes.ForEach( n => n.EntityId = newPerson.Id );
            }

            // Set attributes on this person
            var personAttributeValues = newPerson.Attributes.Select( a => a.Value )
            .Select( a => new AttributeValue
            {
                AttributeId = a.Id,
                EntityId = newPerson.Id,
                Value = newPerson.AttributeValues[a.Key].Value
            } ).ToList();

            rockContext.AttributeValues.AddRange( personAttributeValues );

            // Set aliases on this person
            if ( !newPerson.Aliases.Any( a => a.PersonId == newPerson.Id ) )
            {
                newPerson.Aliases.Add( new PersonAlias
                {
                    AliasPersonId = newPerson.Id,
                    AliasPersonGuid = newPerson.Guid,
                    ForeignKey = newPerson.ForeignKey,
                    ForeignId = newPerson.ForeignId,
                    PersonId = newPerson.Id
                } );
            }

            newPerson.GivingGroupId = familyGroupId;
        }

        #endregion Main Methods

        #region Previous Last Names

        /// <summary>
        /// Loads the Person Previous Name data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadPersonPreviousName( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var importedPersonPreviousNames = new PersonPreviousNameService( lookupContext ).Queryable().Count( p => p.ForeignKey != null );

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

                    var previousName = AddPersonPreviousName( lookupContext, previousPersonName, previousPersonAliasId, previousPersonNameId, false );

                    if ( previousName.Id == 0 )
                    {
                        personPreviousNames.Add( previousName );
                    }
                }

                completedItems++;
                if ( completedItems % ( ReportingNumber * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} person previous names processed.", completedItems ) );
                }

                if ( completedItems % ReportingNumber < 1 )
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