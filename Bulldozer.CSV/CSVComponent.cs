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
using System.ComponentModel.Composition;
using System.Data;
using System.Data.Entity;
using System.IO;
using System.Linq;
using LumenWorks.Framework.IO.Csv;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using static Bulldozer.Utility.CachedTypes;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// This example extends the base Bulldozer class to consume a CSV model.
    /// </summary>
    /// <seealso cref="Bulldozer.BulldozerComponent" />
    [Export( typeof( BulldozerComponent ) )]
    partial class CSVComponent : BulldozerComponent
    {
        #region Fields

        /// <summary>
        /// Gets the full name of the bulldozer type.
        /// </summary>
        /// <value>
        /// The name of the database being imported.
        /// </value>
        public override string FullName => "CSV File";

        /// <summary>
        /// Gets the supported file extension type(s).
        /// </summary>
        /// <value>
        /// The supported extension type.
        /// </value>
        public override string ExtensionType => ".csv";

        /// <summary>
        /// The local data store, contains Database and TableNode list
        /// because multiple files can be uploaded
        /// </summary>
        private List<CSVInstance> CsvDataToImport { get; set; }

        /// <summary>
        /// The person assigned to do the import
        /// </summary>
        protected static int? ImportPersonAliasId;

        /// <summary>
        /// The anonymous giver record
        /// </summary>
        protected static int? AnonymousGiverAliasId;

        /// <summary>
        /// The person attribute category entity type identifier
        /// </summary>
        private int PersonAttributeCategoryEntityTypeId;

        /// <summary>
        /// All the family groups who've been imported
        /// </summary>
        private List<Group> ImportedFamilies;

        /// <summary>
        /// All the group types that have been imported
        /// </summary>
        private List<Location> ImportedLocations;

        /// <summary>
        /// All the group types that have been imported
        /// </summary>
        private List<GroupType> ImportedGroupTypes;

        /// <summary>
        /// All the general groups that have been imported
        /// </summary>
        private List<Group> ImportedGroups;

        /// <summary>
        /// The list of current campuses
        /// </summary>
        private List<Campus> CampusList;

        /// <summary>
        /// All imported accounts. Used in Accounts
        /// </summary>
        protected static Dictionary<int, int?> ImportedAccounts;

        /// <summary>
        /// All imported batches. Used in Batches & Contributions
        /// </summary>
        protected static Dictionary<string, int?> ImportedBatches;

        /// <summary>
        /// All the people keys who've been imported
        /// </summary>
        protected static List<PersonKeys> ImportedPeopleKeys;

        /// <summary>
        /// All imported person history. Used in PersonHistory
        /// </summary>
        protected static List<History> ImportedPersonHistory;

        // Custom attribute types

        protected static AttributeCache IndividualIdAttribute;
        protected static AttributeCache HouseholdIdAttribute;

        #endregion Fields

        #region Methods

        /// <summary>
        /// Loads the database for this instance.
        /// may be called multiple times, if uploading multiple CSV files.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public override bool LoadSchema( string fileName )
        {
            //enforce that the filename must be a known configuration.
            if ( !FileIsKnown( fileName ) )
            {
                return false;
            }

            using ( var dbPreview = new CsvReader( new StreamReader( fileName ), true ) )
            {
                if ( CsvDataToImport == null )
                {
                    CsvDataToImport = new List<CSVInstance>();
                    DataNodes = new List<DataNode>();
                }

                //a local tableNode object, which will track this one of multiple CSV files that may be imported
                var tableNodes = new List<DataNode>();
                CsvDataToImport.Add( new CSVInstance( fileName ) { TableNodes = tableNodes, RecordType = GetRecordTypeFromFilename( fileName ) } );

                var currentIndex = 0;
                var tableItem = new DataNode
                {
                    Name = Path.GetFileNameWithoutExtension( fileName )
                };

                var firstRow = dbPreview.ElementAtOrDefault( 0 );
                if ( firstRow != null )
                {
                    foreach ( var columnName in dbPreview.GetFieldHeaders() )
                    {
                        var childItem = new DataNode
                        {
                            Name = columnName,
                            NodeType = typeof( string ),
                            Value = firstRow[currentIndex] ?? string.Empty
                        };
                        childItem.Parent.Add( tableItem );
                        tableItem.Children.Add( childItem );
                        currentIndex++;
                    }

                    tableNodes.Add( tableItem );
                    DataNodes.Add( tableItem ); //this is to maintain compatibility with the base Bulldozer object.
                }

                return tableNodes.Count() > 0 ? true : false;
            }
        }

        /// <summary>
        /// Previews the data. Overrides base class because we have potential for more than one imported file
        /// </summary>
        /// <param name="settings">todo: describe settings parameter on TransformData</param>
        /// <returns></returns>
        public override int TransformData( Dictionary<string, string> settings )
        {
            var importUser = settings["ImportUser"];

            var completed = 0;
            ReportProgress( 0, "Starting health checks..." );
            if ( !LoadExistingData( importUser ) )
            {
                return -1;
            }

            // only import things that the user checked
            var selectedCsvData = CsvDataToImport.Where( c => c.TableNodes.Any( n => n.Checked != false ) ).ToList();

            ReportProgress( 0, "Starting data import..." );

            // Person data is important, so load it first or make sure some is already there
            if ( selectedCsvData.Any( d => d.RecordType == CSVInstance.RockDataType.INDIVIDUAL ) )
            {
                selectedCsvData = selectedCsvData.OrderByDescending( d => d.RecordType == CSVInstance.RockDataType.INDIVIDUAL ).ToList();
            }
            else if ( !ImportedPeopleKeys.Any() )
            {
                LogException( "Individual Data", "No imported people were found and your data may not be matched correctly." );
            }

            SqlServerTypes.Utilities.LoadNativeAssemblies( AppDomain.CurrentDomain.BaseDirectory );
            foreach ( var csvData in selectedCsvData )
            {
                if ( csvData.RecordType == CSVInstance.RockDataType.INDIVIDUAL )
                {
                    completed += LoadIndividuals( csvData );

                    //
                    // Refresh the list of imported Individuals for other record types to use.
                    //
                    LoadPersonKeys( new RockContext() );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.FAMILY )
                {
                    completed += LoadFamily( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.METRICS )
                {
                    completed += LoadMetrics( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.BANKACCOUNT )
                {
                    completed += MapBankAccount( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.ACCOUNT )
                {
                    completed += MapAccount( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.PLEDGE )
                {
                    completed += MapPledge( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.BATCH )
                {
                    completed += MapBatch( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.CONTRIBUTION )
                {
                    completed += MapContribution( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.NAMEDLOCATION )
                {
                    completed += LoadNamedLocation( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.GROUPTYPE )
                {
                    completed += LoadGroupType( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.GROUP )
                {
                    completed += LoadGroup( csvData );

                    //
                    // Refresh the list of imported groups for other record types to use.
                    //
                    LoadImportedGroups( new RockContext() );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.GROUPPOLYGON )
                {
                    completed += LoadGroupPolygon( csvData );

                    //
                    // Refresh the list of imported groups for other record types to use.
                    //
                    LoadImportedGroups( new RockContext() );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.GROUPMEMBER )
                {
                    completed += LoadGroupMember( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.RELATIONSHIP )
                {
                    completed += LoadRelationshipGroupMember( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.ATTENDANCE )
                {
                    completed += LoadAttendance( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.SCHEDULEDTRANSACTION )
                {
                    completed += LoadScheduledTransaction( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.USERLOGIN )
                {
                    completed += LoadUserLogin( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.ENTITYATTRIBUTE )
                {
                    completed += LoadEntityAttributes( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.ENTITYATTRIBUTEVALUE )
                {
                    completed += LoadEntityAttributeValues( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.CONTENTCHANNEL )
                {
                    completed += LoadContentChannel( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.CONTENTCHANNELITEM )
                {
                    completed += LoadContentChannelItem( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.NOTE )
                {
                    completed += LoadNote( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.PRAYERREQUEST )
                {
                    completed += LoadPrayerRequest( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.PREVIOUSLASTNAME )
                {
                    completed += LoadPersonPreviousName( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.PHONENUMBER )
                {
                    completed += LoadPhoneNumber( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.CONNECTIONREQUEST )
                {
                    completed += LoadConnectionRequest( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.BENEVOLENCEREQUEST )
                {
                    completed += LoadBenevolenceRequest( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.BENEVOLENCERESULT )
                {
                    completed += LoadBenevolenceResult( csvData );
                }
                else if ( csvData.RecordType == CSVInstance.RockDataType.PERSONHISTORY )
                {
                    completed += LoadPersonHistory( csvData );
                }
            } //read all files

            ReportProgress( 100, $"Completed import: {completed:N0} rows processed." );
            return completed;
        }

        /// <summary>
        /// Checks the database for existing import data.
        /// returns false if an error occurred
        /// </summary>
        /// <param name="importUser">The import user.</param>
        /// <returns></returns>
        private bool LoadExistingData( string importUser )
        {
            var lookupContext = new RockContext();
            var personService = new PersonService( lookupContext );
            var importPerson = personService.GetByFullName( importUser, includeDeceased: false, allowFirstNameOnly: true ).FirstOrDefault();
            if ( importPerson == null )
            {
                importPerson = personService.Queryable().FirstOrDefault();
                if ( importPerson == null )
                {
                    LogException( "CheckExistingImport", "The named import user was not found, and none could be created." );
                    return false;
                }
            }

            ImportPersonAliasId = importPerson.PrimaryAliasId;

            var anonymousGiver = personService.GetByFullName( "Anonymous, Giver", includeDeceased: false, allowFirstNameOnly: true ).FirstOrDefault();
            if ( anonymousGiver == null )
            {
                anonymousGiver = personService.Queryable().FirstOrDefault( p => p.Guid.ToString().ToUpper() == "802235DC-3CA5-94B0-4326-AACE71180F48" );
                if ( anonymousGiver == null && requireAnonymousGiver )
                {
                    LogException( "CheckExistingImport", "The record for anonymous giving could not be found, please add a person with first name 'Giver' and last name 'Anonymous'.  Optionally, consider disabling RequireAnonymousGiver in the App.Config file." );
                    return false;
                }
            }

            if ( anonymousGiver != null )
            {
                AnonymousGiverAliasId = anonymousGiver.PrimaryAliasId;
            }

            PersonAttributeCategoryEntityTypeId = EntityTypeCache.Get( "Rock.Model.Attribute" ).Id;

            ReportProgress( 0, "Checking for existing data..." );

            // Don't track groups in this context, just use it as a static reference
            ImportedFamilies = lookupContext.Groups.AsNoTracking()
                .Where( g => g.GroupTypeId == FamilyGroupTypeId && g.ForeignKey != null )
                .OrderBy( g => g.ForeignKey ).ToList();

            LoadCampuses();

            LoadPersonKeys( lookupContext );

            ImportedAccounts = new FinancialAccountService( lookupContext ).Queryable().AsNoTracking()
                .Where( a => a.ForeignId != null )
                .ToDictionary( a => ( int ) a.ForeignId, a => ( int? ) a.Id );

            if ( csvBatchUseForeignKey )
            {
                ImportedBatches = new FinancialBatchService( lookupContext ).Queryable().AsNoTracking()
                    .Where( b => b.ForeignKey != null )
                    .ToDictionary( b => b.ForeignKey, b => ( int? ) b.Id );
            }
            else
            {
                ImportedBatches = new FinancialBatchService( lookupContext ).Queryable().AsNoTracking()
                    .Where( b => b.ForeignId != null )
                    .ToDictionary( b => b.ForeignId.ToString(), b => ( int? ) b.Id );
            }

            LoadImportedGroups( lookupContext );

            LoadImportedGroupTypes( lookupContext );

            LoadImportedLocations( lookupContext );

            LoadImportedPersonHistory( lookupContext );

            return true;
        }

        /// <summary>
        /// Gets the type of the attribute.
        /// </summary>
        /// <param name="attributeTypeString">The attribute type string.</param>
        /// <returns></returns>
        public static int GetAttributeFieldType( string attributeTypeString )
        {
            switch ( attributeTypeString.ToUpper() )
            {
                case "D":
                    return DateFieldTypeId;

                case "B":
                    return BooleanFieldTypeId;

                case "V":
                case "VM":
                    return DefinedValueFieldTypeId;

                case "E":
                    return EncryptedTextFieldTypeId;

                case "L":
                case "VL":
                    return ValueListFieldTypeId;

                case "S":
                    return SingleSelectFieldTypeId;     // Creates a pass/fail list for requirements.

                case "U":
                    return URLLinkFieldTypeId;

                case "H":
                    return HTMLFieldTypeId;

                case "SN":
                    return SsnFieldTypeId;

                default:
                    return TextFieldTypeId;
            }
        }

        /// <summary>
        /// Gets the person keys.
        /// </summary>
        /// <param name="individualId">The individual identifier.</param>
        /// <returns></returns>
        protected static PersonKeys GetPersonKeys( int? individualId = null )
        {
            return individualId.HasValue ? ImportedPeopleKeys.FirstOrDefault( p => p.PersonForeignId == individualId ) : null;
        }

        /// <summary>
        /// Gets the person keys.
        /// </summary>
        /// <param name="individualKey">The individual identifier.</param>
        /// <returns></returns>
        protected static PersonKeys GetPersonKeys( string individualKey = null )
        {
            if ( individualKey.AsIntegerOrNull() != null )
            {
                return GetPersonKeys( individualKey.AsIntegerOrNull() );
            }
            else
            {
                return !string.IsNullOrWhiteSpace( individualKey ) ? ImportedPeopleKeys.FirstOrDefault( p => p.PersonForeignKey == individualKey ) : null;
            }
        }

        /// <summary>
        /// Loads the person keys.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected static void LoadPersonKeys( RockContext lookupContext )
        {
            ImportedPeopleKeys = new PersonAliasService( lookupContext ).Queryable().AsNoTracking()
                .Where( pa => pa.ForeignKey != null )
                .Select( pa => new PersonKeys
                {
                    PersonAliasId = pa.Id,
                    PersonId = pa.PersonId,
                    PersonForeignId = pa.ForeignId,
                    PersonForeignKey = pa.ForeignKey
                } ).ToList();
        }

        /// <summary>
        /// Loads the campuses.
        /// </summary>
        protected void LoadCampuses()
        {
            using ( var rockContext = new RockContext() )
            {
                var campusList = new CampusService( rockContext ).Queryable().ToList();

                foreach ( var campus in campusList )
                {
                    if ( string.IsNullOrWhiteSpace( campus.ShortCode ) )
                    {
                        campus.ShortCode = campus.Name.RemoveAllNonAlphaNumericCharacters();
                    }
                }

                if ( rockContext.ChangeTracker.HasChanges() )
                {
                    rockContext.SaveChanges();
                }

                CampusList = campusList;
            }
        }

        /// <summary>
        /// Loads the imported groups.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadImportedGroups( RockContext lookupContext )
        {
            ImportedGroups = lookupContext.Groups.AsNoTracking()
                .Where( g => ( g.GroupTypeId != FamilyGroupTypeId ) && g.ForeignKey != null ).ToList();
        }

        /// <summary>
        /// Loads the imported group types.
        /// </summary>
        /// <param name="lookupContext"></param>
        protected void LoadImportedGroupTypes( RockContext lookupContext )
        {
            ImportedGroupTypes = lookupContext.GroupTypes.AsNoTracking()
                .Where( t => ( t.Id != FamilyGroupTypeId ) && t.ForeignKey != null ).ToList();
        }

        /// <summary>
        /// Loads the imported locations.
        /// </summary>
        /// <param name="lookupContext"></param>
        protected void LoadImportedLocations( RockContext lookupContext )
        {
            ImportedLocations = lookupContext.Locations.AsNoTracking()
                .Where( l => l.ForeignKey != null ).ToList();
        }

        /// <summary>
        /// Loads the imported histories.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadImportedPersonHistory( RockContext lookupContext )
        {
            var personEntityTypeGuid = Rock.SystemGuid.EntityType.PERSON.AsGuid();
            var personEntityType = lookupContext.EntityTypes.FirstOrDefault( et => et.Guid == personEntityTypeGuid );
            ImportedPersonHistory = lookupContext.Histories.AsNoTracking()
                .Where( h => h.EntityTypeId == personEntityType.Id && h.ForeignKey != null ).ToList();
        }

        #endregion Methods

        #region File Processing Methods

        /// <summary>
        /// Gets the name of the file without the extension.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <returns></returns>
        private static string GetFileRootName( string fileName )
        {
            var root = Path.GetFileName( fileName ).ToLower().Replace( ".csv", string.Empty );
            return root;
        }

        /// <summary>
        /// Checks if the file matches a known format.
        /// </summary>
        /// <param name="filetype">The filetype.</param>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        private static bool FileTypeMatches( CSVInstance.RockDataType filetype, string name )
        {
            if ( name.ToUpper().EndsWith( filetype.ToString() ) )
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Checks if the file matches a known format.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <returns></returns>
        private static bool FileIsKnown( string fileName )
        {
            var name = GetFileRootName( fileName );
            foreach ( var filetype in Get<CSVInstance.RockDataType>() )
            {
                if ( FileTypeMatches( filetype, name ) )
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Gets the record type based on the filename.
        /// </summary>
        /// <param name="filename">The filename.</param>
        /// <returns></returns>
        private static CSVInstance.RockDataType GetRecordTypeFromFilename( string filename )
        {
            var name = GetFileRootName( filename );
            foreach ( var filetype in Get<CSVInstance.RockDataType>() )
            {
                if ( FileTypeMatches( filetype, name ) )
                {
                    return filetype;
                }
            }

            return CSVInstance.RockDataType.NONE;
        }

        #endregion File Processing Methods

        #region Individual Constants

        /*
         * This is the definition of the csv format for the Individual.csv file
         */

        private const int FamilyId = 0;
        private const int FamilyName = 1;
        private const int CreatedDate = 2;
        private const int PersonId = 3;
        private const int Prefix = 4;
        private const int FirstName = 5;
        private const int NickName = 6;
        private const int MiddleName = 7;
        private const int LastName = 8;
        private const int Suffix = 9;
        private const int FamilyRole = 10;
        private const int MaritalStatus = 11;
        private const int ConnectionStatus = 12;
        private const int RecordStatus = 13;
        private const int IsDeceased = 14;
        private const int HomePhone = 15;
        private const int MobilePhone = 16;
        private const int WorkPhone = 17;
        private const int AllowSMS = 18;
        private const int Email = 19;
        private const int IsEmailActive = 20;
        private const int AllowBulkEmail = 21;
        private const int Gender = 22;
        private const int DateOfBirth = 23;
        private const int School = 24;
        private const int GraduationDate = 25;
        private const int Anniversary = 26;
        private const int GeneralNote = 27;
        private const int MedicalNote = 28;
        private const int SecurityNote = 29;
        private const int PreviousPersonIds = 30;
        private const int AlternateEmails = 31;

        #endregion Individual Constants

        #region Family Constants

        /*
         * This is the definition for the Family.csv import file:
         *
         * Columns already numbered from Individuals file:
         * private const int FamilyId = 0;
         * private const int FamilyName = 1;
         * private const in CreatedDate = 2;
         */

        private const int Campus = 3;
        private const int Address = 4;
        private const int Address2 = 5;
        private const int City = 6;
        private const int State = 7;
        private const int Zip = 8;
        private const int Country = 9;
        private const int SecondaryAddress = 10;
        private const int SecondaryAddress2 = 11;
        private const int SecondaryCity = 12;
        private const int SecondaryState = 13;
        private const int SecondaryZip = 14;
        private const int SecondaryCountry = 15;

        #endregion Family Constants

        #region Metrics Constants

        /*
         * Definition for the Metrics.csv import file:
         */

        private const int MetricCampus = 0;
        private const int MetricName = 1;
        private const int MetricValue = 2;
        private const int MetricService = 3;
        private const int MetricCategory = 4;
        private const int MetricNote = 5;

        #endregion Metrics Constants

        #region Batch Constants

        /*
         * Definition for the Batch.csv import file:
         */

        private const int BatchId = 0;
        private const int BatchName = 1;
        private const int BatchDate = 2;
        private const int BatchAmount = 3;

        #endregion Batch Constants

        #region Account Constants

        /*
         * Definition for the Account.csv import file:
         */

        private const int FinancialFundID = 0;
        private const int FinancialFundName = 1;
        private const int FinancialFundDescription = 2;
        private const int FinancialFundGLAccount = 3;
        private const int FinancialFundIsActive = 4;
        private const int FinancialFundStartDate = 5;
        private const int FinancialFundEndDate = 6;
        private const int FinancialFundOrder = 7;
        private const int FinancialFundParentID = 8;
        private const int FinancialFundPublicName = 9;
        private const int FinancialFundIsTaxDeductible = 10;
        private const int FinancialFundCampusName = 11;

        #endregion Account Constants

        #region Contribution Constants

        /*
         * Definition for the Contribution.csv import file:
         */

        private const int IndividualID = 0;
        private const int FundName = 1;
        private const int SubFundName = 2;
        private const int FundGLAccount = 3;
        private const int SubFundGLAccount = 4;
        private const int FundIsActive = 5;
        private const int SubFundIsActive = 6;
        private const int ReceivedDate = 7;
        private const int CheckNumber = 8;
        private const int Memo = 9;
        private const int ContributionTypeName = 10;
        private const int Amount = 11;
        private const int StatedValue = 12;
        private const int ContributionID = 13;
        private const int ContributionBatchID = 14;
        private const int ContributionCreditCardType = 15;
        private const int IsAnonymous = 16;
        private const int Gateway = 17;
        private const int ScheduledTransactionForeignKey = 18;
        private const int FundId = 19;
        private const int SourceType = 20;

        #endregion Contribution Constants

        #region Pledge Constants

        /*
         * Definition for the Pledge.csv import file:
         *
         * Columns already numbered from Contribution file:
         * private const int IndividualID = 0;
         * private const int FundName = 1;
         * private const int SubFundName = 2;
         * private const int FundGLAccount = 3;
         * private const int SubFundGLAccount = 4;
         * private const int FundIsActive = 5;
         * private const int SubFundIsActive = 6
         */

        private const int PledgeFrequencyName = 7;
        private const int TotalPledge = 8;
        private const int StartDate = 9;
        private const int EndDate = 10;
        private const int PledgeId = 11;
        private const int PledgeCreatedDate = 12;
        private const int PledgeModifiedDate = 13;

        #endregion Pledge Constants

        #region Named Location Constants

        /*
         * This is the definition of the csv format for the NamedLocation.csv file
         */

        private const int NamedLocationId = 0;                  /* String | Int [Requred] */
        private const int NamedLocationName = 1;                /* String [Requred] */
        private const int NamedLocationCreatedDate = 2;         /* Date (mm/dd/yyyy) [Optional] */
        private const int NamedLocationType = 3;                /* Guid | Int | String [Optional] */
        private const int NamedLocationParent = 4;              /* String | Int [Optional] */
        private const int NamedLocationSoftRoomThreshold = 5;   /* Int [Optional] */
        private const int NamedLocationFirmRoomThreshold = 6;   /* Int [Optional] */

        #endregion Named Location Constants

        #region Group Type Constants

        /*
         * This is the definition of the csv format for the GroupType.csv file
         */

        private const int GroupTypeId = 0;                  /* String | Int [Requred] */
        private const int GroupTypeName = 1;                /* String [Requred] */
        private const int GroupTypeCreatedDate = 2;         /* Date (mm/dd/yyyy) [Optional] */
        private const int GroupTypePurpose = 3;             /* Guid | Int | String [Optional] */
        private const int GroupTypeInheritedGroupType = 4;  /* Guid | Int | String [Optional] */
        private const int GroupTypeTakesAttendance = 5;     /* "YES" | "NO" [Optional] */
        private const int GroupTypeWeekendService = 6;      /* "YES" | "NO" [Optional] */
        private const int GroupTypeShowInGroupList = 7;     /* "YES" | "NO" [Optional] */
        private const int GroupTypeShowInNav = 8;           /* "YES" | "NO" [Optional] */
        private const int GroupTypeParentId = 9;            /* String | Int [Optional] */
        private const int GroupTypeSelfReference = 10;      /* "YES" | "NO" [Optional] */
        private const int GroupTypeWeeklySchedule = 11;     /* "YES" | "NO" [Optional] */
        private const int GroupTypeDescription = 12;        /* String [Optional] */

        #endregion Group Type Constants

        #region Group Constants

        /*
         * This is the definition of the csv format for the Group.csv file
         */

        private const int GroupId = 0;                      /* String | Int [Requred] */
        private const int GroupName = 1;                    /* String [Requred] */
        private const int GroupCreatedDate = 2;             /* Date (mm/dd/yyyy) [Optional] */
        private const int GroupType = 3;                    /* Guid | Int | String [Required] */
        private const int GroupParentGroupId = 4;           /* String | Int [Optional] */
        private const int GroupActive = 5;                  /* "YES" | "NO" [Optional] */
        private const int GroupOrder = 6;                   /* Int [Optional] */
        private const int GroupCampus = 7;                  /* String [Optional] */
        private const int GroupAddress = 8;                 /* String [Optional] */
        private const int GroupAddress2 = 9;                /* String [Optional] */
        private const int GroupCity = 10;                   /* String [Optional] */
        private const int GroupState = 11;                  /* String [Optional] */
        private const int GroupZip = 12;                    /* String [Optional] */
        private const int GroupCountry = 13;                /* String [Optional] */
        private const int GroupSecondaryAddress = 14;       /* String [Optional] */
        private const int GroupSecondaryAddress2 = 15;      /* String [Optional] */
        private const int GroupSecondaryCity = 16;          /* String [Optional] */
        private const int GroupSecondaryState = 17;         /* String [Optional] */
        private const int GroupSecondaryZip = 18;           /* String [Optional] */
        private const int GroupSecondaryCountry = 19;       /* String [Optional] */
        private const int GroupNamedLocation = 20;          /* String [Optional] */
        private const int GroupDayOfWeek = 21;              /* String [Optional] */
        private const int GroupTime = 22;                   /* String [Optional] */
        private const int GroupDescription = 23;            /* String [Optional] */
        private const int GroupCapacity = 24;               /* Int [Optional] */

        #endregion Group Constants

        #region Group Polygon Constants

        /*
         * This is the definition of the csv format for the GroupPolygon.csv file
         *
         * Columns already numbered from Group file:
         * private const int GroupId = 0;                      String | Int [Requred]
         * private const int GroupName = 1;                    String [Requred]
         * private const int GroupCreatedDate = 2;             Date (mm/dd/yyyy) [Optional]
         * private const int GroupType = 3;                    Guid | Int | String [Required]
         * private const int GroupParentGroupId = 4;           String | Int [Optional]
         * private const int GroupActive = 5;                  "YES" | "NO" [Optional]
         * private const int GroupOrder = 6;                   Int [Optional]
         * private const int GroupCampus = 7;                  String [Required]
         */

        private const int Latitude = 8;                 /* String [Required] */
        private const int Longitude = 9;                /* String [Required] */

        #endregion Group Polygon Constants

        #region GroupMember Constants

        /*
         * This is the definition of the csv format for the GroupMember.csv file.
         * The Group.csv file MUST be run first to populate the groups correctly. This
         * is due to the fact that parent groups may not have any members and thus
         * would not appear in this file so the tree could not be constructed correctly.
         */

        private const int GroupMemberId = 0;                        /* String [Required] */
        private const int GroupMemberGroupId = 1;                   /* String | Int [Requred] */
        private const int GroupMemberPersonId = 2;                  /* String | Int [Required] */
        private const int GroupMemberCreatedDate = 3;               /* Date (mm/dd/yyyy) [Optional] */
        private const int GroupMemberRole = 4;                      /* String [Optional] */
        private const int GroupMemberActive = 5;                    /* "YES" | "NO" | "PENDING" [Optional, Default = YES] */

        #endregion GroupMember Constants

        #region Attendance Constants

        /*
         * This is the definition of the csv format for the Attendance.csv file.
         * Other files, such as Individuals, Groups, Locations, etc. should be
         * imported first as they will not be created automatically.
         *
         * For performance reasons, it is recommended that your csv file be ordered
         * by GroupId. This reduces database overhead.
         */

        private const int AttendanceId = 0;                         /* String | Int [Required] */
        private const int AttendanceGroupId = 1;                    /* String | Int [Required] */
        private const int AttendancePersonId = 2;                   /* String | Int [Required] */
        private const int AttendanceCreatedDate = 3;                /* Date (mm/dd/yyyy) [Optional] */
        private const int AttendanceDate = 4;                       /* Date (mm/dd/yyyy) [Required] */
        private const int AttendanceAttended = 5;                   /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=TRUE] */
        private const int AttendanceLocationId = 6;                 /* String | Int [Optional] */

        #endregion Attendance Constants

        #region ScheduledTransaction Constants

        /*
         * This contains the definition of the csv format for the
         * ScheduledTransaction.csv file. The Individuals file should be imported
         * first and any Payment Gateways and Accounts should be setup or
         * imported.
         *
         * For best performance, group scheduled transactions by ID.
         */

        private const int ScheduledTransactionId = 0;                   /* String [Required] */
        private const int ScheduledTransactionPersonId = 1;             /* String [Required] */
        private const int ScheduledTransactionCreatedDate = 2;          /* DateTime (mm/dd/yyyy hh:mm) [Optional Default=Now] */
        private const int ScheduledTransactionStartDate = 3;            /* Date (mm/dd/yyyy) [Required] */
        private const int ScheduledTransactionEndDate = 4;              /* Date (mm/dd/yyyy) [Optional] */
        private const int ScheduledTransactionNextPaymentDate = 5;      /* Date (mm/dd/yyyy) [Optional] */
        private const int ScheduledTransactionActive = 6;               /* Bool (TRUE|FALSE|1|0) [Optional Default=TRUE] */
        private const int ScheduledTransactionFrequency = 7;            /* String | Int [Required] */
        private const int ScheduledTransactionNumberOfPayments = 8;     /* Int [Optional] */
        private const int ScheduledTransactionTransactionCode = 9;      /* String [Required] */
        private const int ScheduledTransactionGatewaySchedule = 10;     /* String [Required] */
        private const int ScheduledTransactionGateway = 11;             /* String | Int [Required] */
        private const int ScheduledTransactionAccount = 12;             /* String | Int [Required] */
        private const int ScheduledTransactionAmount = 13;              /* Decimal [Required] */
        private const int ScheduledTransactionCurrencyType = 14;        /* String [Required] 'ACH' or 'Credit Card' */
        private const int ScheduledTransactionCreditCardType = 15;      /* String [Optional] */

        #endregion ScheduledTransaction Constants

        #region UserLogin Constants

        /*
         * This is the definition of the csv format for the UserLogin.csv file.
         * Individuals must be imported before this file.
         *
         * Example Authentication Type:
         * Rock.Security.Authentication.ActiveDirectory - Password should be blank
         */

        private const int UserLoginId = 0;                          /* String | Int [Required] */
        private const int UserLoginPersonId = 1;                    /* String | Int [Required] */
        private const int UserLoginUserName = 2;                    /* String [Required] */
        private const int UserLoginPassword = 3;                    /* Hex String [Optional] */
        private const int UserLoginDateCreated = 4;                 /* Date (mm/dd/yyyy) [Optional] */
        private const int UserLoginAuthenticationType = 5;          /* String [Required] */
        private const int UserLoginIsConfirmed = 6;                 /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=TRUE] */

        #endregion UserLogin Constants

        #region ContentChannel Constants

        /*
         * This is the definition of the csv format for the ContentChannel.csv file.
         */

        private const int ContentChannelName = 0;                   /* String [Required] */
        private const int ContentChannelTypeName = 1;               /* String [Required] */
        private const int ContentChannelDescription = 2;            /* String [Optional] */
        private const int ContentChannelId = 3;                     /* String | Int [Optional] */
        private const int ContentChannelRequiresApproval = 4;       /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=FALSE] [Optional] */
        private const int ContentChannelParentId = 5;               /* String | Int [Optional] */

        #endregion ContentChannel Constants

        #region ContentChannelItem Constants

        /*
         * This is the definition of the csv format for the ContentChannelItem.csv file.
         *
         * Columns already numbered from ContentChannel file:
         * private const int ContentChannelName = 0;    String [Required]
         */

        private const int ItemTitle = 1;                            /* String [Required] */
        private const int ItemContent = 2;                          /* String [Optional] */
        private const int ItemStart = 3;                            /* DateTime [Optional] */
        private const int ItemExpire = 4;                           /* DateTime [Optional] */
        private const int ItemId = 5;                               /* String | Int [Optional] */
        private const int ItemParentId = 6;                         /* String | Int [Optional] */

        #endregion ContentChannelItem Constants

        #region Note Constants

        /*
         * This is the definition of the csv format for the Note.csv file.
         *
         */

        private const int NoteType = 0;                            /* String [Required] */
        private const int EntityTypeName = 1;                      /* String [Required] */
        private const int EntityForeignId = 2;                     /* Int [Required] */
        private const int NoteCaption = 3;                         /* String [Optional] */
        private const int NoteText = 4;                            /* String [Optional] */
        private const int NoteDate = 5;                            /* DateTime [Optional] */
        private const int CreatedById = 6;                         /* Int [Optional] */
        private const int IsAlert = 7;                             /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=FALSE] [Optional] */
        private const int IsPrivate = 8;                           /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=FALSE] [Optional] */

        #endregion Note Constants

        #region Prayer Request Constants

        /*
         * This is the definition of the csv format for the PrayerRequest.csv file.
         *
         */

        private const int PrayerRequestCategory = 0;            /* String [Required] */
        private const int PrayerRequestText = 1;                /* String [Required] */
        private const int PrayerRequestDate = 2;                /* DateTime [Required] */
        private const int PrayerRequestId = 3;                  /* String [Optional] */
        private const int PrayerRequestFirstName = 4;           /* String [Required] */
        private const int PrayerRequestLastName = 5;            /* String [Optional] */
        private const int PrayerRequestEmail = 6;               /* String [Optional] */
        private const int PrayerRequestExpireDate = 7;          /* DateTime [Optional] */
        private const int PrayerRequestAllowComments = 8;       /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=TRUE] [Optional] */
        private const int PrayerRequestIsPublic = 9;            /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=TRUE] [Optional] */
        private const int PrayerRequestIsApproved = 10;         /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=TRUE] [Optional] */
        private const int PrayerRequestApprovedDate = 11;       /* DateTime [Optional] */
        private const int PrayerRequestApprovedById = 12;       /* String [Optional] */
        private const int PrayerRequestCreatedById = 13;        /* String [Optional] */
        private const int PrayerRequestRequestedById = 14;      /* String [Optional] */
        private const int PrayerRequestAnswerText = 15;         /* String [Optional] */
        private const int PrayerRequestCampus = 16;             /* String [Optional] */

        #endregion Prayer Request Constants

        #region Previous Last Name Constants

        /*
         * This is the definition of the csv format for the PreviousLastName.csv file.
         *
         */

        private const int PreviousLastNamePersonId = 0;         /* String | Int [Required] */
        private const int PreviousLastName = 1;                 /* String [Required] */
        private const int PreviousLastNameId = 2;               /* String | Int | Guid [Optional] */

        #endregion Previous Last Name Constants

        #region Bank Account Constants

        /*
         * This is the definition of the csv format for the BankAccount.csv file.
         *
         * Already defined:
         * FamilyId = 0
         * PersonId = 3
         */

        private const int RoutingNumber = 1;
        private const int AccountNumber = 2;

        #endregion Bank Account Constants

        #region Phone Number Constants

        /*
         * This is the definition of the csv format for the PhoneNumber.csv file.
         *
         */

        private const int PhonePersonId = 0;            /* String | Int [Required] */
        private const int PhoneType = 1;                /* String [Required] */
        private const int Phone = 2;                    /* String [Required] */
        private const int PhoneIsMessagingEnabled = 3;  /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=FALSE] [Optional] */
        private const int PhoneIsUnlisted = 4;          /* "TRUE" | "FALSE" | 1 | 0 [Optional Default=FALSE] [Optional] */
        private const int PhoneId = 5;                  /* String | Int [Optional] */
        private const int CountryCode = 6;              /* Int [Optional] */

        #endregion Phone Number Constants

        #region EntityAttribute Constants

        /*
         * This is the definition of the csv format for the EntityAttribute.csv file.
         */

        private const int AttributeEntityTypeName = 0;              /* String [Required] */
        private const int AttributeId = 1;                          /* String | Int [Optional] */
        private const int AttributeRockKey = 2;                     /* String [Optional, defaults to AttributeName without whitespace]    Note: AttributeId will be added as FK/FID to attributes that match Entity and Attribute Key with null FK/FID */
        private const int AttributeName = 3;                        /* String [Required] */
        private const int AttributeCategoryName = 4;                /* String [Optional] */
        private const int AttributeType = 5;                        /* "D" | "B" | "V" | "E" | "L" | "VL" | "" */
        private const int AttributeDefinedTypeId = 6;               /* String | Int [Optional] */
        private const int AttributeEntityTypeQualifierName = 7;     /* String [Optional] */
        private const int AttributeEntityTypeQualifierValue = 8;    /* String | Int [Optional] */

        #endregion EntityAttribute Constants

        #region EntityAttributeValue Constants

        /*
         * This is the definition of the csv format for the EntityAttributeValue.csv file.
         *
         * EntityAttribute.csv file must be run before this file
         * or the attributes already exist in Rock DB.
         *
         * Columns already numbered from EntityAttribute file:
         * private const int AttributeEntityTypeName = 0;
         * private const int AttributeId = 1;
         * private const int AttributeRockKey = 2;    Note: Attribute values that do not find match on AttributeId to FK or FID will attempt to match on the Attribute Key.
         */

        private const int AttributeValueId = 3;             /* String | Int [Optional] */
        private const int AttributeValueEntityId = 4;       /* String | Int [Required] */
        private const int AttributeValue = 5;               /* String [Required] */

        #endregion EntityAttributeValue Constants

        #region ConnectionRequest Constants

        /*
         * This is the definition of the csv format for the ConnectionRequest.csv file.
         *
         */

        /* Opportunity */
        private const int OpportunityForeignKey = 0;
        private const int OpportunityName = 1;
        private const int ConnectionType = 2;
        private const int OpportunityDescription = 3;
        private const int OpportunityActive = 4;
        private const int OpportunityCreated = 5;
        private const int OpportunityModified = 6;
        /* Request */
        private const int RequestForeignKey = 7;
        private const int RequestPersonId = 8;
        private const int RequestConnectorId = 9;
        private const int RequestCreated = 10;
        private const int RequestModified = 11;
        private const int RequestStatus = 12;
        private const int RequestState = 13;
        private const int RequestComments = 14;
        private const int RequestFollowUp = 15;
        /* Activity */
        private const int ActivityType = 16;
        private const int ActivityNote = 17;
        private const int ActivityDate = 18;
        private const int ActivityConnectorId = 19;

        #endregion ConnectionRequest Constants

        #region BenevolenceRequest Constants

        /*
         * This is the definition of the csv format for the BenevolenceRequest.csv file.
         *
         */
         
        private const int BenevolenceRequestText = 0;                /* String [Required] */
        private const int BenevolenceRequestDate = 1;                /* DateTime [Required] */
        private const int BenevolenceRequestId = 2;                  /* String [Optional] */
        private const int BenevolenceRequestFirstName = 3;           /* String [Required] */
        private const int BenevolenceRequestLastName = 4;            /* String [Required] */
        private const int BenevolenceRequestEmail = 5;               /* String [Optional] */
        private const int BenevolenceRequestCreatedById = 6;        /* String [Optional] */
        private const int BenevolenceRequestCreatedDate = 7;         /* DateTime [Optional] */
        private const int BenevolenceRequestRequestedById = 8;      /* String [Optional] */
        private const int BenevolenceRequestCaseWorkerId = 9;       /* String [Optional] */
        private const int BenevolenceRequestCellPhone = 10;          /* String [Optional] */
        private const int BenevolenceRequestHomePhone = 11;          /* String [Optional] */
        private const int BenevolenceRequestWorkPhone = 12;          /* String [Optional] */
        private const int BenevolenceRequestGovernmentId = 13;       /* String [Optional] */
        private const int BenevolenceRequestProvidedNextSteps = 14;  /* String [Optional] */
        private const int BenevolenceRequestStatus = 15;              /* String | Int [Required] */
        private const int BenevolenceRequestResultSummary = 16;      /* String [Optional] */
        private const int BenevolenceRequestAddress = 17;             /* String [Optional] */
        private const int BenevolenceRequestAddress2 = 18;            /* String [Optional] */
        private const int BenevolenceRequestCity = 19;               /* String [Optional] */
        private const int BenevolenceRequestState = 20;              /* String [Optional] */
        private const int BenevolenceRequestZip = 21;                /* String [Optional] */
        private const int BenevolenceRequestCountry = 22;            /* String [Optional] */
        private const int BenevolenceType = 23;                      /* String [Optional: Rock v13+] */

        #endregion BenevolenceRequest Constants

        #region BenevolenceResult Constants

        /*
         * This is the definition of the csv format for the BenevolenceResult.csv file.
         *
         */

        private const int BenevolenceResultRequestId = 0;          /* String [Required] */
        private const int BenevolenceResultType = 1;               /* String | Int [Required] */
        private const int BenevolenceResultId = 2;                 /* String [Optional] */
        private const int BenevolenceResultAmount = 3;             /* Decimal [Optional] */
        private const int BenevolenceResultSummary = 4;            /* String [Optional] */
        private const int BenevolenceResultCreatedById = 5;        /* String [Optional] */
        private const int BenevolenceResultCreatedDate = 6;        /* DateTime [Optional] */

        #endregion BenevolenceResult Constants

        #region Person History

        /*
         * This is the definition of the csv format for the PersonHistory.csv file.
         */

        private const int HistoryId = 0;
        private const int HistoryPersonId = 1;
        private const int HistoryCategory = 2;
        private const int ChangedByPersonId = 3;
        private const int Verb = 4;
        private const int Caption = 5;
        private const int ChangeType = 6;
        private const int ValueName = 7;
        private const int RelatedEntityType = 8;
        private const int RelatedEntityId = 9;
        private const int NewValue = 10;
        private const int OldValue = 11;
        private const int HistoryDateTime = 12;
        private const int IsSensitive = 13;

        #endregion Person History
    }
}