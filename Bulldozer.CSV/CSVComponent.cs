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
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.Entity;
using System.Globalization;
using System.IO;
using System.Linq;
using Bulldozer.Model;
using CsvHelper.Configuration;
using LumenWorks.Framework.IO.Csv;
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

        public string ImportInstanceFKPrefix { get; set; } = "BDImport";

        public string EmailRegex { get; set; }

        public ImportUpdateType ImportUpdateOption { get; set; } = ImportUpdateType.AddOnly;

        /// <summary>
        /// The local data store, contains Database and TableNode list
        /// because multiple files can be uploaded
        /// </summary>
        private List<CSVInstance> CsvDataToImport { get; set; }

        /// <summary>
        /// The list of AttendanceCsv objects collected from
        /// the attendance csv file.
        /// </summary>
        private List<AttendanceCsv> AttendanceCsvList { get; set; }

        /// <summary>
        /// The list of BusinessCsv objects collected from
        /// the person csv file.
        /// </summary>
        private List<BusinessCsv> BusinessCsvList { get; set; }

        /// <summary>
        /// The list of EntityAttributeCsv objects for businesses collected from
        /// the person csv file.
        /// </summary>
        private List<EntityAttributeCsv> BusinessAttributeCsvList { get; set; }

        /// <summary>
        /// The list of PersonCsv objects collected from
        /// the person csv file.
        /// </summary>
        private List<PersonCsv> PersonCsvList { get; set; }

        /// <summary>
        /// The list of PersonAddressCsv objects collected from
        /// the person csv file.
        /// </summary>
        private List<PersonAddressCsv> PersonAddressCsvList { get; set; }

        /// <summary>
        /// The list of PersonAttributeValueCsv objects collected from
        /// the person csv file.
        /// </summary>
        private List<PersonAttributeValueCsv> PersonAttributeValueCsvList { get; set; }

        /// <summary>
        /// The list of PersonAttributeValueCsv objects collected from
        /// the person csv file.
        /// </summary>
        private List<EntityAttributeCsv> PersonAttributeCsvList { get; set; }

        /// <summary>
        /// The list of PersonPhoneCsv objects collected from
        /// the person csv file.
        /// </summary>
        private List<PersonPhoneCsv> PersonPhoneCsvList { get; set; }

        /// <summary>
        /// The list of PersonPhoneCsv objects collected from
        /// the person csv file.
        /// </summary>
        private List<PersonSearchKeyCsv> PersonSearchKeyCsvList { get; set; }

        /// <summary>
        /// The list of FamilyAttributeValueCsv objects collected from
        /// the person csv file.
        /// </summary>
        private List<EntityAttributeCsv> FamilyAttributeCsvList { get; set; }

        private Dictionary<string, DefinedValueCache> MaritalStatusDVDict { get; set; }

        private Dictionary<string, DefinedValueCache> ConnectionStatusDVDict { get; set; }

        private Dictionary<Guid, DefinedValueCache> RecordStatusDVDict { get; set; }

        private Dictionary<Guid, DefinedValueCache> RecordStatusReasonDVDict { get; set; }

        private Dictionary<Guid, DefinedValueCache> PersonRecordTypeValuesDict { get; set; }

        private Dictionary<string, DefinedValueCache> PhoneNumberTypeDVDict { get; set; }

        private Dictionary<string, DefinedValueCache> TitleDVDict { get; set; }

        private Dictionary<string, DefinedValueCache> SuffixDVDict { get; set; }

        private Dictionary<Guid, DefinedValueCache> GroupLocationTypeDVDict { get; set; }

        private Dictionary<string, AttributeCache> PersonAttributeDict { get; set; }

        private Dictionary<string, AttributeCache> FamilyAttributeDict { get; set; }

        private Dictionary<string, FieldTypeCache> FieldTypeDict { get; set; }

        private Dictionary<int, CampusCache> ImportedCampusDict { get; set; }

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
            var recordType = GetRecordTypeFromFilename( fileName );

            using ( CsvReader dbPreview = new CsvReader( new StreamReader( fileName ), true ) )
            {
                if ( CsvDataToImport == null )
                {
                    CsvDataToImport = new List<CSVInstance>();
                    DataNodes = new List<DataNode>();
                }

                //a local tableNode object, which will track this one of multiple CSV files that may be imported
                var tableNodes = new List<DataNode>();
                CsvDataToImport.Add( new CSVInstance( fileName ) { TableNodes = tableNodes, RecordType = recordType } );

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

            // Person data is important, so load it first or make sure some is already there
            if ( selectedCsvData.Any( d => d.RecordType == CSVInstance.RockDataType.Person ) )
            {
                selectedCsvData = selectedCsvData.OrderByDescending( d => d.RecordType == CSVInstance.RockDataType.Person ).ToList();
            }
            else if ( !ImportedPeopleKeys.Any() )
            {
                LogException( "Person Data", "No imported people were found and your data may not be matched correctly." );
            }

            SqlServerTypes.Utilities.LoadNativeAssemblies( AppDomain.CurrentDomain.BaseDirectory );

            ReportProgress( 0, "Preparing data for import..." );
            LoadImportEntityLists( selectedCsvData );

            ReportProgress( 0, "Importing new DefinedValues to support person data..." );

            // Populate Rock Defined Values with various supporting entities from the csv files
            AddPersonDataDVs( Rock.SystemGuid.DefinedType.PERSON_MARITAL_STATUS );
            AddPersonDataDVs( Rock.SystemGuid.DefinedType.PERSON_TITLE );
            AddPersonDataDVs( Rock.SystemGuid.DefinedType.PERSON_SUFFIX );
            AddPersonDataDVs( Rock.SystemGuid.DefinedType.PERSON_RECORD_STATUS_REASON );
            AddPersonDataDVs( Rock.SystemGuid.DefinedType.PERSON_CONNECTION_STATUS );
            AddPersonDataDVs( Rock.SystemGuid.DefinedType.PERSON_PHONE_TYPE );
            AddPersonDataDVs( Rock.SystemGuid.DefinedType.GROUP_LOCATION_TYPE );

            AddAttributeCategories();
            AddPersonAttributes();
            AddBusinessAttributes();
            AddFamilyAttributes();

            //AddGroupTypes();
            //AddGroupAttributes();

            // load dictionaries for entities to refer to when import is processing
            this.ReportProgress( 0, "Loading existing entity data dictionaries..." );
            LoadDictionaries();

            // Get Email regex pattern from Rock Person object
            this.EmailRegex = typeof( Person ).GetProperty( "Email" ).GetCustomAttributes( false ).FirstOrDefault( a => a.GetType() == typeof( RegularExpressionAttribute ) ).GetPropertyValue( "Pattern" ).ToString();

            completed += ImportPersonList();
            completed += ImportBusinesses();

            //// Attendance Related
            //SubmitLocationImport();
            //SubmitGroupImport();
            //SubmitScheduleImport();
            //SubmitAttendanceImport();

            //// Financial Transaction Related
            //SubmitFinancialAccountImport();
            //SubmitFinancialBatchImport();
            //SubmitFinancialTransactionImport();

            //// Financial Pledges
            //SubmitFinancialPledgeImport();

            //// Person Notes
            //SubmitEntityNotesImport<Person>( this.SlingshotPersonNoteList, null );

            //// Family Notes
            //SubmitEntityNotesImport<Group>( this.SlingshotFamilyNoteList, true );

            // Update any new AttributeValues to set the [ValueAsDateTime] field.
            //AttributeValueService.UpdateAllValueAsDateTimeFromTextValue();
            foreach ( var csvData in selectedCsvData )
            {
                //if ( csvData.RecordType == CSVInstance.RockDataType.Person )
                //{
                //    completed += LoadIndividuals( csvData );

                //    //
                //    // Refresh the list of imported Individuals for other record types to use.
                //    //
                //    LoadPersonKeys( new RockContext() );
                //}
                if ( csvData.RecordType == CSVInstance.RockDataType.FAMILY )
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
                    completed += ProcessAttendance( AttendanceCsvList );
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
                else if ( csvData.RecordType == CSVInstance.RockDataType.PersonPhone )
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
            if ( string.Join( "", name.Split( '-' ) ).ToUpper().EndsWith( filetype.ToString().ToUpper() ) )
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

        /// <summary>
        /// Loads all the import entity lists from the csv.
        /// </summary>
        private void LoadImportEntityLists( List<CSVInstance> csvInstances )
        {
            // Person Data
            LoadPersonDataLists( csvInstances );

            // Family Attributes
            var famInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.FamilyAttribute );
            if ( famInstance != null )
            {
                FamilyAttributeCsvList = LoadEntityImportListFromCsv<EntityAttributeCsv>( famInstance.FileName );
                ReportProgress( 0, string.Format( "{0} Family Attribute records to import...", FamilyAttributeCsvList.Count ) );
            }

            // Attendance

            var attInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.ATTENDANCE );
            if ( attInstance != null )
            {
                this.AttendanceCsvList = LoadEntityImportListFromCsv<AttendanceCsv>( attInstance.FileName );
                ReportProgress( 0, string.Format( "{0} Attendance records to import...", AttendanceCsvList.Count ) );
            }

            //// Groups (non-family) (Note: There may be duplicates, so only get the distinct ones.)
            //LoadGroupSlingshotLists();

            //// Group Members
            //var groupMemberList = LoadSlingshotListFromFile<SlingshotCore.Model.GroupMember>().GroupBy( a => a.GroupId ).ToDictionary( k => k.Key, v => v.ToList() );
            //var groupLookup = this.SlingshotGroupList.ToDictionary( k => k.Id, v => v );
            //foreach ( var groupIdMembers in groupMemberList )
            //{
            //    groupLookup[groupIdMembers.Key].GroupMembers = groupIdMembers.Value;
            //}

            //// Group Types
            //this.SlingshotGroupTypeList = LoadSlingshotListFromFile<SlingshotCore.Model.GroupType>();

            //// Locations (Note: There may be duplicates, so only get the distinct ones.)
            //this.SlingshotLocationList = LoadSlingshotListFromFile<SlingshotCore.Model.Location>().DistinctBy( a => a.Id ).ToList();

            //// Schedules (Note: There may be duplicates, so only get the distinct ones.)
            //this.SlingshotScheduleList = LoadSlingshotListFromFile<SlingshotCore.Model.Schedule>().DistinctBy( a => a.Id ).ToList();

            //// Financial Accounts
            //this.SlingshotFinancialAccountList = LoadSlingshotListFromFile<SlingshotCore.Model.FinancialAccount>();

            //// Financial Transactions and Financial Transaction Details
            //this.SlingshotFinancialTransactionList = LoadSlingshotListFromFile<SlingshotCore.Model.FinancialTransaction>();
            //var slingshotFinancialTransactionDetailList = LoadSlingshotListFromFile<SlingshotCore.Model.FinancialTransactionDetail>();
            //var slingshotFinancialTransactionLookup = this.SlingshotFinancialTransactionList.ToDictionary( k => k.Id, v => v );
            //foreach ( var slingshotFinancialTransactionDetail in slingshotFinancialTransactionDetailList )
            //{
            //    slingshotFinancialTransactionLookup[slingshotFinancialTransactionDetail.TransactionId].FinancialTransactionDetails.Add( slingshotFinancialTransactionDetail );
            //}

            //// Financial Batches
            //this.SlingshotFinancialBatchList = LoadSlingshotListFromFile<SlingshotCore.Model.FinancialBatch>();
            //var transactionsByBatch = this.SlingshotFinancialTransactionList.GroupBy( a => a.BatchId ).ToDictionary( k => k.Key, v => v.ToList() );
            //foreach ( var slingshotFinancialBatch in this.SlingshotFinancialBatchList )
            //{
            //    if ( transactionsByBatch.ContainsKey( slingshotFinancialBatch.Id ) )
            //    {
            //        slingshotFinancialBatch.FinancialTransactions = transactionsByBatch[slingshotFinancialBatch.Id];
            //    }
            //}

            //// Financial Pledges
            //this.SlingshotFinancialPledgeList = LoadSlingshotListFromFile<SlingshotCore.Model.FinancialPledge>( false );

            //// Person Notes
            //this.SlingshotPersonNoteList = LoadSlingshotListFromFile<SlingshotCore.Model.PersonNote>();

            //// Family Notes
            //this.SlingshotFamilyNoteList = LoadSlingshotListFromFile<SlingshotCore.Model.FamilyNote>();

            // Businesses
            LoadBusinessDataLists( csvInstances );

            // Business Contacts

            var busContactInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BusinessContact );
            if ( busContactInstance != null )
            {
                var businessContacts = LoadEntityImportListFromCsv<BusinessContactCsv>( busContactInstance.FileName );
                ReportProgress( 0, string.Format( "{0} Business Contacts to import...", businessContacts.Count ) );
                var businessContactsList = businessContacts.GroupBy( c => c.BusinessId ).ToDictionary( k => k.Key, v => v.ToList() );
                var businessLookup = this.BusinessCsvList.ToDictionary( k => k.Id, v => v );
                foreach ( var busisnessContacts in businessContactsList )
                {
                    businessLookup[busisnessContacts.Key].Contacts = busisnessContacts.Value;
                }
            }
        }


        /// <summary>
        /// Loads the import data lists related to Person data.
        /// </summary>
        private void LoadPersonDataLists( List<CSVInstance> csvInstances )
        {
            var personInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.Person );
            if ( personInstance != null )
            {
                PersonCsvList = LoadEntityImportListFromCsv<PersonCsv>( personInstance.FileName );
                ReportProgress( 0, string.Format( "{0} Person records to import...", PersonCsvList.Count ) );
            }

            var personAddressInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PersonAddress );
            if ( personAddressInstance != null )
            {
                PersonAddressCsvList = LoadEntityImportListFromCsv<PersonAddressCsv>( personAddressInstance.FileName );
                ReportProgress( 0, string.Format( "{0} PersonAddress records to import...", PersonAddressCsvList.Count ) );
            }

            var personAttributeValueInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PersonAttributeValue );
            if ( personAttributeValueInstance != null )
            {
                PersonAttributeValueCsvList = LoadEntityImportListFromCsv<PersonAttributeValueCsv>( personAttributeValueInstance.FileName );
                ReportProgress( 0, string.Format( "{0} PersonAttributeValue records to import...", PersonAttributeValueCsvList.Count ) );
            }

            var personPhoneInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PersonPhone );
            if ( personPhoneInstance != null )
            {
                PersonPhoneCsvList = LoadEntityImportListFromCsv<PersonPhoneCsv>( personPhoneInstance.FileName );
                ReportProgress( 0, string.Format( "{0} PersonPhone records to import...", PersonPhoneCsvList.Count ) );
            }

            var personSearchKeyInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PersonSearchKey );
            if ( personSearchKeyInstance != null )
            {
                PersonSearchKeyCsvList = LoadEntityImportListFromCsv<PersonSearchKeyCsv>( personSearchKeyInstance.FileName );
                ReportProgress( 0, string.Format( "{0} PersonSearchKey records to import...", PersonSearchKeyCsvList.Count ) );
            }

            var personAddressListLookup = PersonAddressCsvList.GroupBy( a => a.PersonId ).ToDictionary( k => k.Key, v => v.ToList() );
            var personAttributeValueListLookup = PersonAttributeValueCsvList.GroupBy( av => av.PersonId ).ToDictionary( k => k.Key, v => v.ToList() );
            var personPhoneListLookup = PersonPhoneCsvList.GroupBy( p => p.PersonId ).ToDictionary( k => k.Key, v => v.ToList() );
            var personSearchKeyListLookup = PersonSearchKeyCsvList.GroupBy( sk => sk.PersonId ).ToDictionary( k => k.Key, v => v.ToList() );

            foreach ( var personCsv in PersonCsvList )
            {
                personCsv.Addresses = personAddressListLookup.ContainsKey( personCsv.Id ) ? personAddressListLookup[personCsv.Id] : new List<PersonAddressCsv>();
                personCsv.Attributes = personAttributeValueListLookup.ContainsKey( personCsv.Id ) ? personAttributeValueListLookup[personCsv.Id].ToList() : new List<PersonAttributeValueCsv>();
                personCsv.PhoneNumbers = personPhoneListLookup.ContainsKey( personCsv.Id ) ? personPhoneListLookup[personCsv.Id].ToList() : new List<PersonPhoneCsv>();
                personCsv.PersonSearchKeys = personSearchKeyListLookup.GetValueOrNull( personCsv.Id )?.ToList() ?? new List<PersonSearchKeyCsv>();
            }

            var personAttributeInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PersonAttribute );
            if ( personAttributeInstance != null )
            {
                PersonAttributeCsvList = LoadEntityImportListFromCsv<EntityAttributeCsv>( personAttributeInstance.FileName );
                ReportProgress( 0, string.Format( "{0} PersonAttribute records to import...", PersonAttributeCsvList.Count ) );
            }
        }

        /// <summary>
        /// Loads the person slingshot lists.
        /// </summary>
        private void LoadBusinessDataLists( List<CSVInstance> csvInstances )
        {
            var businessInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.Business );
            if ( businessInstance != null )
            {
                BusinessCsvList = LoadEntityImportListFromCsv<BusinessCsv>( businessInstance.FileName );
                ReportProgress( 0, string.Format( "{0} Business records to import...", BusinessCsvList.Count ) );
            }

            var businessAddressList = new Dictionary<string, List<BusinessAddressCsv>>();
            var businessAttributeValueList = new Dictionary<string, List<BusinessAttributeValueCsv>>();
            var businessPhoneList = new Dictionary<string, List<BusinessPhoneCsv>>();

            var businessAddressInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BusinessAddress );
            if ( businessAddressInstance != null )
            {
                businessAddressList = LoadEntityImportListFromCsv<BusinessAddressCsv>( businessAddressInstance.FileName ).GroupBy( a => a.BusinessId ).ToDictionary( k => k.Key, v => v.ToList() ); ;
                ReportProgress( 0, string.Format( "{0} BusinessAddress records to import...", businessAddressList.Count ) );
            }

            var businessAttributeValueInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BusinessAttributeValue );
            if ( businessAttributeValueInstance != null )
            {
                businessAttributeValueList = LoadEntityImportListFromCsv<BusinessAttributeValueCsv>( businessAttributeValueInstance.FileName ).GroupBy( a => a.BusinessId ).ToDictionary( k => k.Key, v => v.ToList() ); ;
                ReportProgress( 0, string.Format( "{0} BusinessAttributeValue records to import...", businessAttributeValueList.Count ) );
            }

            var businessPhoneInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BusinessPhone );
            if ( businessPhoneInstance != null )
            {
                businessPhoneList = LoadEntityImportListFromCsv<BusinessPhoneCsv>( businessPhoneInstance.FileName ).GroupBy( a => a.BusinessId ).ToDictionary( k => k.Key, v => v.ToList() ); ;
                ReportProgress( 0, string.Format( "{0} BusinessPhone records to import...", businessPhoneList.Count ) );
            }

            foreach ( var business in BusinessCsvList )
            {
                business.Addresses = businessAddressList.ContainsKey( business.Id ) ? businessAddressList[business.Id] : new List<BusinessAddressCsv>();
                business.Attributes = businessAttributeValueList.ContainsKey( business.Id ) ? businessAttributeValueList[business.Id].ToList() : new List<BusinessAttributeValueCsv>();
                business.PhoneNumbers = businessPhoneList.ContainsKey( business.Id ) ? businessPhoneList[business.Id].ToList() : new List<BusinessPhoneCsv>();
            }

            var businessAttributeInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BusinessAttribute );
            if ( businessAttributeInstance != null )
            {
                BusinessAttributeCsvList = LoadEntityImportListFromCsv<EntityAttributeCsv>( businessAttributeInstance.FileName );
                ReportProgress( 0, string.Format( "{0} BusinessAttribute records to import...", BusinessCsvList.Count ) );
            }
        }

        /// <summary>
        /// Loads the Bulldozer entity list from csv file.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        private List<T> LoadEntityImportListFromCsv<T>( string fileName )
        {
            if ( File.Exists( fileName ) )
            {
                try
                {
                    using ( var fileStream = File.OpenText( fileName ) )
                    {
                        CsvHelper.CsvReader csvReader = new CsvHelper.CsvReader( fileStream, new CsvConfiguration( CultureInfo.InvariantCulture ) { HasHeaderRecord = true } );
                        return csvReader.GetRecords<T>().ToList();
                    }
                }
                catch ( Exception ex )
                {
                    var exception = new AggregateException( $"File '{Path.GetFileName( fileName )}' cannot be properly read during import. See InnerExceptions for line number(s).", ex );
                    LogException( "Data Preparation", $"File '{Path.GetFileName( fileName )}' cannot be properly read during import. See InnerExceptions for line number(s)." );
                    LogException( "Data Preparation", string.Format( "{0}/n{1}", ex.Message, ex.InnerException ) );
                    throw exception;
                }
            }
            else
            {
                return new List<T>();
            }
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
        private const int NonCashTypeType = 21;

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

        /// <summary>
        /// Add any suffixes that aren't in Rock yet
        /// </summary>
        private void AddPersonDataDVs( string definedTypeSystemGuid )
        {
            var importEntityList = new List<string>();
            var csvEntityValues = new List<string>();
            switch ( definedTypeSystemGuid )
            {
                case Rock.SystemGuid.DefinedType.PERSON_CONNECTION_STATUS:
                    csvEntityValues = this.PersonCsvList.Select( p => p.ConnectionStatus ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    break;
                case Rock.SystemGuid.DefinedType.PERSON_MARITAL_STATUS:
                    csvEntityValues = this.PersonCsvList.Select( p => p.MaritalStatus ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    break;
                case Rock.SystemGuid.DefinedType.PERSON_PHONE_TYPE:
                    var importedPhoneTypes_Person = this.PersonCsvList
                        .SelectMany( a => a.PhoneNumbers )
                        .Select( a => a.PhoneType )
                        .Distinct()
                        .ToList();

                    var importedPhoneTypes_Business = this.BusinessCsvList
                        .SelectMany( a => a.PhoneNumbers )
                        .Select( a => a.PhoneType )
                        .Distinct()
                        .ToList();

                    csvEntityValues = importedPhoneTypes_Person
                        .Concat( importedPhoneTypes_Business )
                        .Distinct()
                        .ToList();
                    break;
                case Rock.SystemGuid.DefinedType.PERSON_TITLE:
                    csvEntityValues = PersonCsvList.Select( p => p.Salutation ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    break;
                case Rock.SystemGuid.DefinedType.PERSON_SUFFIX:
                    csvEntityValues = PersonCsvList.Select( p => p.Suffix ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    break;
                case Rock.SystemGuid.DefinedType.PERSON_RECORD_STATUS_REASON:
                    var csvEntityValues_Person = this.PersonCsvList.Select( p => p.InactiveReason ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    var csvEntityValues_Business = this.BusinessCsvList.Select( p => p.InactiveReason ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    csvEntityValues = csvEntityValues_Person.Concat( csvEntityValues_Business ).Distinct().ToList();
                    break;
                case Rock.SystemGuid.DefinedType.GROUP_LOCATION_TYPE:
                    var importedAddressTypes_Person = this.PersonCsvList
                        .SelectMany( a => a.Addresses )
                        .Select( a => Enum.GetName( typeof( CSVInstance.AddressType ), a.AddressType ) )
                        .Distinct()
                        .ToList();

                    var importedAddressTypes_Business = this.BusinessCsvList
                        .SelectMany( a => a.Addresses )
                        .Select( a => Enum.GetName( typeof( CSVInstance.AddressType ), a.AddressType ) )
                        .Distinct()
                        .ToList();

                    csvEntityValues = importedAddressTypes_Person
                        .Concat( importedAddressTypes_Business )
                        .Distinct()
                        .ToList();
                    break;
                default:
                    break;
            }

            foreach ( var entityDT in csvEntityValues )
            {
                if ( !importEntityList.Any( a => a == entityDT ) )
                {
                    importEntityList.Add( entityDT );
                }
            }

            var rockContext = new RockContext();
            var dtService = new DefinedTypeService( rockContext );
            var entityDefinedType = dtService.Get( definedTypeSystemGuid.AsGuid() );

            DefinedTypeCache.Clear();

            var usedDTValues = DefinedTypeCache.Get( entityDefinedType.Guid ).DefinedValues.Select( v => v.Value ).ToList();

            foreach ( var importValue in importEntityList.Where( a => !usedDTValues.Any( r => a == r ) ) )
            {
                var newValue = new DefinedValue()
                {
                    ForeignKey = ImportInstanceFKPrefix + importValue,
                    Value = importValue,
                    Guid = Guid.NewGuid()
                };

                usedDTValues.Add( newValue.Value );
                entityDefinedType.DefinedValues.Add( newValue );
                rockContext.SaveChanges();
            }

            DefinedTypeCache.Clear();
        }

        /// <summary>
        /// Adds any attribute categories that are in the csv files (person and family attributes)
        /// </summary>
        private void AddAttributeCategories()
        {
            int entityTypeIdPerson = EntityTypeCache.GetId<Person>().Value;
            int entityTypeIdAttribute = EntityTypeCache.GetId<Rock.Model.Attribute>().Value;
            int entityTypeIdGroup = EntityTypeCache.GetId<Group>().Value;
            var csvCategoryNames = PersonAttributeCsvList.Where( a => !string.IsNullOrWhiteSpace( a.Category ) ).Select( a => a.Category ).Distinct().ToList();
            csvCategoryNames.AddRange( FamilyAttributeCsvList.Where( a => !string.IsNullOrWhiteSpace( a.Category ) ).Select( a => a.Category ).Distinct().ToList() );

            var rockContext = new RockContext();
            var categoryService = new CategoryService( rockContext );

            var attributeCategoryList = categoryService.Queryable().Where( a => a.EntityTypeId == entityTypeIdAttribute ).ToList();

            foreach ( var categoryName in csvCategoryNames.Distinct().ToList() )
            {
                if ( !attributeCategoryList.Any( a => a.Name.Equals( categoryName, StringComparison.OrdinalIgnoreCase ) ) )
                {
                    var attributeCategory = new Category()
                    {
                        Name = categoryName,
                        EntityTypeId = entityTypeIdAttribute,
                        EntityTypeQualifierColumn = "EntityTypeId",
                        EntityTypeQualifierValue = entityTypeIdPerson.ToString(),
                        Guid = Guid.NewGuid()
                    };

                    categoryService.Add( attributeCategory );
                    attributeCategoryList.Add( attributeCategory );
                }

                rockContext.SaveChanges();
            }
        }

        /// <summary>
        /// Adds the person attributes.
        /// </summary>
        private void AddPersonAttributes()
        {
            int entityTypeIdPerson = EntityTypeCache.GetId<Person>().Value;

            var rockContext = new RockContext();
            var attributeService = new AttributeService( rockContext );

            var entityTypeIdAttribute = EntityTypeCache.GetId<Rock.Model.Attribute>().Value;

            var attributeCategoryList = new CategoryService( rockContext ).Queryable().Where( a => a.EntityTypeId == entityTypeIdAttribute ).ToList();

            // Add any Person Attributes to Rock that aren't in Rock yet
            var newAttributes = PersonAttributeCsvList.Where( a => !PersonAttributeDict.Keys.Any( ad => ad.Equals( a.Key, StringComparison.OrdinalIgnoreCase ) ) );
            ReportProgress( 0, string.Format( "Creating {0} new Person Attributes...", newAttributes.Count() ) );
            foreach ( var attribute in newAttributes )
            {
                var newPersonAttribute = new Rock.Model.Attribute()
                {
                    Key = attribute.Key,
                    Name = attribute.Name,
                    Guid = Guid.NewGuid(),
                    EntityTypeId = entityTypeIdPerson,
                    FieldTypeId = FieldTypeDict[attribute.FieldType].Id
                };

                if ( !string.IsNullOrWhiteSpace( attribute.Category ) )
                {
                    var attributeCategory = attributeCategoryList.FirstOrDefault( a => a.Name.Equals( attribute.Category, StringComparison.OrdinalIgnoreCase ) );
                    if ( attributeCategory != null )
                    {
                        newPersonAttribute.Categories = new List<Category>();
                        newPersonAttribute.Categories.Add( attributeCategory );
                    }
                }
                attributeService.Add( newPersonAttribute );
            }
            rockContext.SaveChanges();
        }

        /// <summary>
        /// Adds the business attributes.
        /// </summary>
        private void AddBusinessAttributes()
        {
            int entityTypeIdPerson = EntityTypeCache.GetId<Person>().Value;

            var rockContext = new RockContext();
            var attributeService = new AttributeService( rockContext );

            var entityTypeIdAttribute = EntityTypeCache.GetId<Rock.Model.Attribute>().Value;

            var attributeCategoryList = new CategoryService( rockContext ).Queryable().Where( a => a.EntityTypeId == entityTypeIdAttribute ).ToList();

            // Add any Business Attributes to Rock that aren't in Rock yet
            var newAttributes = this.BusinessAttributeCsvList.Where( a => !PersonAttributeDict.Keys.Any( ad => ad.Equals( a.Key, StringComparison.OrdinalIgnoreCase ) ) );
            ReportProgress( 0, string.Format( "Creating {0} new Person Attributes for businesses...", newAttributes.Count() ) );
            foreach ( var attribute in newAttributes )
            {
                var newBusinessAttribute = new Rock.Model.Attribute()
                {
                    Key = attribute.Key,
                    Name = attribute.Name,
                    Guid = Guid.NewGuid(),
                    EntityTypeId = entityTypeIdPerson,
                    FieldTypeId = this.FieldTypeDict[attribute.FieldType].Id,
                    EntityTypeQualifierColumn = "RecordTypeValueId",
                    EntityTypeQualifierValue = this.PersonRecordTypeValuesDict[Rock.SystemGuid.DefinedValue.PERSON_RECORD_TYPE_BUSINESS.AsGuid()].Id.ToString()
                };

                if ( !string.IsNullOrWhiteSpace( attribute.Category ) )
                {
                    var attributeCategory = attributeCategoryList.FirstOrDefault( a => a.Name.Equals( attribute.Category, StringComparison.OrdinalIgnoreCase ) );
                    if ( attributeCategory != null )
                    {
                        newBusinessAttribute.Categories = new List<Category>() { attributeCategory };
                    }
                }
                attributeService.Add( newBusinessAttribute );
            }
            rockContext.SaveChanges();
        }

        /// <summary>
        /// Adds the family attributes.
        /// </summary>
        private void AddFamilyAttributes()
        {
            int entityTypeIdGroup = EntityTypeCache.GetId<Group>().Value;

            var rockContext = new RockContext();
            var attributeService = new AttributeService( rockContext );
            var entityTypeIdAttribute = EntityTypeCache.GetId<Rock.Model.Attribute>().Value;
            var attributeCategoryList = new CategoryService( rockContext ).Queryable().Where( a => a.EntityTypeId == entityTypeIdAttribute ).ToList();
            int groupTypeIdFamily = GroupTypeCache.GetFamilyGroupType().Id;

            // Add any Family Attributes to Rock that aren't in Rock yet
            foreach ( var attribute in FamilyAttributeCsvList )
            {
                if ( !FamilyAttributeDict.Keys.Any( a => a.Equals( attribute.Key, StringComparison.OrdinalIgnoreCase ) ) )
                {
                    var newFamilyAttribute = new Rock.Model.Attribute()
                    {
                        Key = attribute.Key,
                        Name = attribute.Name,
                        Guid = Guid.NewGuid(),
                        EntityTypeId = entityTypeIdGroup,
                        EntityTypeQualifierColumn = "GroupTypeId",
                        EntityTypeQualifierValue = groupTypeIdFamily.ToString(),
                        FieldTypeId = FieldTypeDict[attribute.FieldType].Id
                    };

                    if ( !string.IsNullOrWhiteSpace( attribute.Category ) )
                    {
                        var attributeCategory = attributeCategoryList.FirstOrDefault( a => a.Name.Equals( attribute.Category, StringComparison.OrdinalIgnoreCase ) );
                        if ( attributeCategory != null )
                        {
                            newFamilyAttribute.Categories = new List<Category>() { attributeCategory };
                        }
                    }
                    attributeService.Add( newFamilyAttribute );
                }
            }

            rockContext.SaveChanges();
        }

        /// <summary>
        /// Loads the dictionaries.
        /// </summary>
        private void LoadDictionaries()
        {
            this.TitleDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_TITLE.AsGuid() ).GetUniqueValues();
            this.SuffixDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_SUFFIX.AsGuid() ).GetUniqueValues();
            this.MaritalStatusDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_MARITAL_STATUS.AsGuid() ).GetUniqueValues();
            this.PhoneNumberTypeDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_PHONE_TYPE.AsGuid() ).GetUniqueValues();
            this.GroupLocationTypeDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.GROUP_LOCATION_TYPE.AsGuid() );
            this.PersonRecordTypeValuesDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_RECORD_TYPE.AsGuid() );
            this.RecordStatusDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_RECORD_STATUS.AsGuid() );
            this.RecordStatusReasonDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_RECORD_STATUS_REASON.AsGuid() );
            this.ConnectionStatusDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_CONNECTION_STATUS.AsGuid() ).GetUniqueValues();
            //this.LocationTypeValues = LoadDefinedValues( SystemGuid.DefinedType.LOCATION_TYPE.AsGuid() );
            //this.CurrencyTypeValues = LoadDefinedValues( SystemGuid.DefinedType.FINANCIAL_CURRENCY_TYPE.AsGuid() );
            //this.TransactionSourceTypeValues = LoadDefinedValues( SystemGuid.DefinedType.FINANCIAL_SOURCE_TYPE.AsGuid() );
            //this.TransactionTypeValues = LoadDefinedValues( SystemGuid.DefinedType.FINANCIAL_TRANSACTION_TYPE.AsGuid() );

            int entityTypeIdPerson = EntityTypeCache.GetId<Person>().Value;
            int entityTypeIdGroup = EntityTypeCache.GetId<Group>().Value;
            int entityTypeIdAttribute = EntityTypeCache.GetId<Rock.Model.Attribute>().Value;

            var rockContext = new RockContext();

            // Person Attributes
            var personAttributes = new AttributeService( rockContext ).Queryable().Where( a => a.EntityTypeId == entityTypeIdPerson ).Select( a => a.Id ).ToList().Select( a => AttributeCache.Get( a ) ).ToList();
            PersonAttributeDict = personAttributes.ToDictionary( k => k.Key, v => v, StringComparer.OrdinalIgnoreCase );

            // Family Attributes
            string groupTypeIdFamily = GroupTypeCache.GetFamilyGroupType().Id.ToString();

            var familyAttributes = new AttributeService( rockContext ).Queryable().Where( a => a.EntityTypeId == entityTypeIdGroup && a.EntityTypeQualifierColumn == "GroupTypeId" && a.EntityTypeQualifierValue == groupTypeIdFamily ).Select( a => a.Id ).ToList().Select( a => AttributeCache.Get( a ) ).ToList();
            FamilyAttributeDict = familyAttributes.ToDictionary( k => k.Key, v => v, StringComparer.OrdinalIgnoreCase );

            // FieldTypes
            FieldTypeDict = new FieldTypeService( rockContext ).Queryable().Select( a => a.Id ).ToList().Select( a => FieldTypeCache.Get( a ) ).ToDictionary( k => k.Class, v => v, StringComparer.OrdinalIgnoreCase );

            //// Group Attributes
            //var groupAttributes = new AttributeService( rockContext ).Queryable().Where( a => a.EntityTypeId == entityTypeIdGroup ).Select( a => a.Id ).ToList().Select( a => AttributeCache.Get( a ) ).ToList();
            //this.GroupAttributeKeyLookup = groupAttributes.ToDictionary( k => k.Key, v => v, StringComparer.OrdinalIgnoreCase );

            //// GroupTypes
            //this.GroupTypeLookupByForeignId = new GroupTypeService( rockContext ).Queryable().Where( a => a.ForeignId.HasValue && a.ForeignKey == this.ForeignSystemKey ).ToList().Select( a => GroupTypeCache.Get( a ) ).ToDictionary( k => k.ForeignId.Value, v => v );

            // Campuses
            ImportedCampusDict = CampusCache.All().Where( a => a.ForeignId.HasValue && a.ForeignKey?.Split( '_' )[0] == ImportInstanceFKPrefix ).ToList().ToDictionary( k => k.ForeignId.Value, v => v );
        }

        /// <summary>
        /// Loads the defined values.
        /// </summary>
        /// <param name="definedTypeGuid">The defined type unique identifier.</param>
        /// <returns></returns>
        private Dictionary<Guid, DefinedValueCache> LoadDefinedValues( Guid definedTypeGuid )
        {
            return DefinedTypeCache.Get( definedTypeGuid ).DefinedValues.ToDictionary( k => k.Guid );
        }
    }

    /// <summary>
    /// Dictionary Extensions Helper Class
    /// </summary>
    public static class DictionaryExtensions
    {
        /// <summary>
        /// Converts a DefinedValue dictionary (indexed by Guid) into a dictionary indexed by unique values.
        /// </summary>
        /// <param name="inputDictionary">The source dictionary (indexed by Guid).</param>
        /// <returns></returns>
        public static Dictionary<string, DefinedValueCache> GetUniqueValues( this Dictionary<Guid, DefinedValueCache> inputDictionary )
        {
            return inputDictionary.Values
                .GroupBy( k => k.Value ).Select( grp => grp.First() )
                .ToDictionary( v => v.Value, p => p, StringComparer.OrdinalIgnoreCase );
        }
    }
}