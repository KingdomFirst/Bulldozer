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
using CsvHelper.Configuration;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Data;
using System.Data.Entity;
using System.Globalization;
using System.IO;
using System.Linq;
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

        public string EmailRegex { get; set; }

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

        // Custom attribute types

        protected static AttributeCache IndividualIdAttribute;
        protected static AttributeCache HouseholdIdAttribute;

        #endregion Fields

        #region Csv Object Lists

        /// <summary>
        /// The local data store, contains Database and TableNode list
        /// because multiple files can be uploaded
        /// </summary>
        private List<CSVInstance> CsvDataToImport { get; set; }

        /// <summary>
        /// The list of AttendanceCsv objects collected from
        /// the attendance csv file.
        /// </summary>
        private List<AttendanceCsv> AttendanceCsvList { get; set; } = new List<AttendanceCsv>();

        /// <summary>
        /// The list of BusinessCsv objects collected from
        /// the business csv file.
        /// </summary>
        private List<BusinessCsv> BusinessCsvList { get; set; } = new List<BusinessCsv>();

        /// <summary>
        /// The list of BusinessAddressCsv objects collected from
        /// the business-address csv file.
        /// </summary>
        private List<BusinessAddressCsv> BusinessAddressCsvList { get; set; } = new List<BusinessAddressCsv>();

        /// <summary>
        /// The list of EntityAttributeCsv objects for businesses collected from
        /// the business-attribute csv file.
        /// </summary>
        private List<EntityAttributeCsv> BusinessAttributeCsvList { get; set; } = new List<EntityAttributeCsv>();

        /// <summary>
        /// The list of BusinessAttributeValueCsv objects collected from
        /// the person-attributevalue csv file.
        /// </summary>
        private List<BusinessAttributeValueCsv> BusinessAttributeValueCsvList { get; set; } = new List<BusinessAttributeValueCsv>();

        /// <summary>
        /// The list of BusinessContactCsv objects collected from
        /// the business-address csv file.
        /// </summary>
        private List<BusinessContactCsv> BusinessContactCsvList { get; set; } = new List<BusinessContactCsv>();

        /// <summary>
        /// The list of BusinessPhoneCsv objects collected from
        /// the person-phone csv file.
        /// </summary>
        private List<BusinessPhoneCsv> BusinessPhoneCsvList { get; set; } = new List<BusinessPhoneCsv>();

        /// <summary>
        /// The list of EntityAttributeValueCsv objects collected from
        /// the entity-attributeValue csv file.
        /// </summary>
        private List<EntityAttributeValueCsv> EntityAttributeValueCsvList { get; set; } = new List<EntityAttributeValueCsv>();

        /// <summary>
        /// The list of FamilyAttributeCsv objects collected from
        /// the family-attribute csv file.
        /// </summary>
        private List<EntityAttributeCsv> FamilyAttributeCsvList { get; set; } = new List<EntityAttributeCsv>();

        /// <summary>
        /// The list of FinancialAccountCsv objects collected from
        /// the attendance csv file.
        /// </summary>
        private List<FinancialAccountCsv> FinancialAccountCsvList { get; set; } = new List<FinancialAccountCsv>();

        /// <summary>
        /// The list of FinancialBatchCsv objects collected from
        /// the attendance csv file.
        /// </summary>
        private List<FinancialBatchCsv> FinancialBatchCsvList { get; set; } = new List<FinancialBatchCsv>();

        /// <summary>
        /// The list of FinancialTransactionCsv objects collected from
        /// the attendance csv file.
        /// </summary>
        private List<FinancialTransactionCsv> FinancialTransactionCsvList { get; set; } = new List<FinancialTransactionCsv>();

        /// <summary>
        /// The list of FinancialTransactionDetailCsv objects collected from
        /// the attendance csv file.
        /// </summary>
        private List<FinancialTransactionDetailCsv> FinancialTransactionDetailCsvList { get; set; } = new List<FinancialTransactionDetailCsv>();

        /// <summary>
        /// The list of FundraisingGroupCsv objects collected from
        /// the group csv file.
        /// </summary>
        private List<FundraisingGroupCsv> FundraisingGroupCsvList { get; set; } = new List<FundraisingGroupCsv>();

        /// <summary>
        /// The list of GroupCsv objects collected from
        /// the group csv file.
        /// </summary>
        private List<GroupCsv> GroupCsvList { get; set; } = new List<GroupCsv>();

        /// <summary>
        /// The list of GroupAddressCsv objects collected from
        /// the group csv file.
        /// </summary>
        private List<GroupAddressCsv> GroupAddressCsvList { get; set; } = new List<GroupAddressCsv>();

        /// <summary>
        /// The list of GroupAttributeCsv objects collected from
        /// the group csv file.
        /// </summary>
        private List<EntityAttributeCsv> GroupAttributeCsvList { get; set; } = new List<EntityAttributeCsv>();

        /// <summary>
        /// The list of GroupAttributeValueCsv objects collected from
        /// the group csv file.
        /// </summary>
        private List<GroupAttributeValueCsv> GroupAttributeValueCsvList { get; set; } = new List<GroupAttributeValueCsv>();

        /// <summary>
        /// The list of GroupMemberCsv objects collected from
        /// the group csv file.
        /// </summary>
        private List<GroupMemberCsv> GroupMemberCsvList { get; set; } = new List<GroupMemberCsv>();

        /// <summary>
        /// The list of GroupMemberHistoricalCsv objects collected from
        /// the group csv file.
        /// </summary>
        private List<GroupMemberHistoricalCsv> GroupMemberHistoricalCsvList { get; set; } = new List<GroupMemberHistoricalCsv>();

        /// <summary>
        /// The list of GroupTypeCsv objects collected from
        /// the group csv file.
        /// </summary>
        private List<GroupTypeCsv> GroupTypeCsvList { get; set; } = new List<GroupTypeCsv>();

        /// <summary>
        /// The list of LocationCsv objects collected from
        /// the locations csv file.
        /// </summary>
        private List<LocationCsv> LocationCsvList { get; set; } = new List<LocationCsv>();

        /// <summary>
        /// The list of PersonCsv objects collected from
        /// the person csv file.
        /// </summary>
        private List<PersonCsv> PersonCsvList { get; set; } = new List<PersonCsv>();

        /// <summary>
        /// The list of PersonAddressCsv objects collected from
        /// the person-address csv file.
        /// </summary>
        private List<PersonAddressCsv> PersonAddressCsvList { get; set; } = new List<PersonAddressCsv>();

        /// <summary>
        /// The list of PersonAttributeValueCsv objects collected from
        /// the person-attributevalue csv file.
        /// </summary>
        private List<PersonAttributeValueCsv> PersonAttributeValueCsvList { get; set; } = new List<PersonAttributeValueCsv>();

        /// <summary>
        /// The list of PersonAttributeCsv objects collected from
        /// the person-attribute csv file.
        /// </summary>
        private List<EntityAttributeCsv> PersonAttributeCsvList { get; set; } = new List<EntityAttributeCsv>();

        /// <summary>
        /// The list of PersonPhoneCsv objects collected from
        /// the person-phone csv file.
        /// </summary>
        private List<PersonHistoryCsv> PersonHistoryCsvList { get; set; } = new List<PersonHistoryCsv>();

        /// <summary>
        /// The list of PersonNoteCsv objects collected from
        /// the person-searchkey csv file.
        /// </summary>
        private List<PersonNoteCsv> PersonNoteCsvList { get; set; } = new List<PersonNoteCsv>();

        /// <summary>
        /// The list of PersonPhoneCsv objects collected from
        /// the person-phone csv file.
        /// </summary>
        private List<PersonPhoneCsv> PersonPhoneCsvList { get; set; } = new List<PersonPhoneCsv>();

        /// <summary>
        /// The list of PersonSearchKeyCsv objects collected from
        /// the person-searchkey csv file.
        /// </summary>
        private List<PersonSearchKeyCsv> PersonSearchKeyCsvList { get; set; } = new List<PersonSearchKeyCsv>();

        /// <summary>
        /// The list of ScheduleCsv objects collected from
        /// the schedule csv file.
        /// </summary>
        private List<ScheduleCsv> ScheduleCsvList { get; set; } = new List<ScheduleCsv>();

        #endregion Csv Object Lists

        #region Defined Value Dictionaries

        private Dictionary<string, DefinedValueCache> ConnectionStatusDVDict { get; set; }

        private Dictionary<string, DefinedValueCache> CreditCardTypeValues { get; set; }

        private Dictionary<string, DefinedValueCache> CurrencyTypeValues { get; set; }

        private Dictionary<string, DefinedValueCache> MaritalStatusDVDict { get; set; }

        private Dictionary<string, DefinedValueCache> NonCashAssetTypeValues { get; set; }

        private Dictionary<Guid, DefinedValueCache> RecordStatusReasonDVDict { get; set; }

        private Dictionary<Guid, DefinedValueCache> PersonRecordTypeValuesDict { get; set; }

        private Dictionary<string, DefinedValueCache> PhoneNumberTypeDVDict { get; set; }

        private Dictionary<Guid, DefinedValueCache> RecordStatusDVDict { get; set; }

        private Dictionary<string, DefinedValueCache> SuffixDVDict { get; set; }

        private Dictionary<string, DefinedValueCache> TitleDVDict { get; set; }

        private Dictionary<string, DefinedValueCache> TransactionSourceTypeValues { get; set; }

        private Dictionary<string, DefinedValueCache> TransactionTypeValues { get; set; }

        #endregion Defined Value Dictionaries

        #region Imported Entity Lookup Dictionaries

        private Dictionary<string, AttendanceOccurrence> AttendanceOccurrenceDict { get; set; }

        private Dictionary<string, Campus> CampusImportDict { get; set; }

        private Dictionary<int, Campus> CampusesDict { get; set; }

        private Dictionary<string, DefinedTypeCache> DefinedTypeDict { get; set; }

        private Dictionary<string, Group> FamilyDict { get; set; }

        private Dictionary<string, Rock.Model.Attribute> FamilyAttributeDict { get; set; }

        private Dictionary<string, FieldTypeCache> FieldTypeDict { get; set; }

        private Dictionary<string, Group> GroupDict { get; set; }

        private Dictionary<string, GroupMember> GroupMemberDict { get; set; }

        private Dictionary<Guid, DefinedValueCache> GroupLocationTypeDVDict { get; set; }

        private Dictionary<string, Rock.Model.Attribute> GroupAttributeDict { get; set; }

        private Dictionary<string, GroupType> GroupTypeDict { get; set; }

        private Dictionary<string, Location> LocationsDict;

        private Dictionary<string, Person> PersonDict { get; set; }

        private Dictionary<string, Rock.Model.Attribute> PersonAttributeDict { get; set; }

        private Dictionary<string, PersonSearchKey> PersonSearchKeyDict { get; set; }

        private Dictionary<string, UserLogin> UserLoginDict { get; set; }

        protected Dictionary<string, FinancialAccount> ImportedAccounts;

        protected Dictionary<string, int?> ImportedBatches;

        protected static Dictionary<string, int?> ImportedPeople;

        protected static Dictionary<string, PersonKeys> ImportedPeopleKeys;

        #endregion Imported Entity Lookup Dictionaries

        #region Global Entity Lists

        /// <summary>
        /// All the general groups that have been imported
        /// </summary>
        private List<Group> ImportedGroups;

        /// <summary>
        /// All imported person history. Used in PersonHistory
        /// </summary>
        protected static List<History> ImportedPersonHistory;

        #endregion Global Entity Lists

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

            using ( var dbPreview = new LumenWorks.Framework.IO.Csv.CsvReader( new StreamReader( fileName ), true ) )
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
            SqlServerTypes.Utilities.LoadNativeAssemblies( AppDomain.CurrentDomain.BaseDirectory );

            ReportProgress( 0, "Staging csv data..." );

            // only import things that the user checked
            var selectedCsvData = CsvDataToImport.Where( c => c.TableNodes.Any( n => n.Checked != false ) ).ToList();
            LoadImportEntityLists( selectedCsvData );

            var completed = 0;
            ReportProgress( 0, "Starting health checks..." );
            if ( !LoadExistingData( this.ImportUser ) )
            {
                return -1;
            }

            // Person data is important, so load it first or make sure some is already there
            if ( selectedCsvData.Any( d => d.RecordType == CSVInstance.RockDataType.Person ) )
            {
                selectedCsvData = selectedCsvData.OrderByDescending( d => d.RecordType == CSVInstance.RockDataType.Person ).ToList();
            }
            else if ( !ImportedPeopleKeys.Any() )
            {
                LogException( "Person Data", "No imported people were found and your data may not be matched correctly." );
            }

            // Pre-populate Rock with various supporting entities from the csv files

            if ( !UseExistingCampusIds )
            {
                AddCampuses();
            }

            ReportProgress( 0, "Checking for new GroupTypes" );
            if ( this.GroupTypeCsvList.Count > 0 )
            {
                ImportGroupTypes();
            }

            ReportProgress( 0, "Checking for new DefinedTypes in Attributes" );
            AddAttributeDefinedTypes();

            ReportProgress( 0, "Checking for new Attribute Categories" );
            AddAttributeCategories();

            ReportProgress( 0, "Checking for new Person, Business, Family, or Group Attributes" );
            var newAttributes = "0_0_0";
            if ( this.PersonAttributeCsvList.Count > 0 || this.BusinessAttributeCsvList.Count > 0 || this.FamilyAttributeCsvList.Count > 0 || this.GroupAttributeCsvList.Count > 0 )
            {
                newAttributes = AddAttributes();
            }

            var newAttributeArray = newAttributes.Split( '_' );
            if ( newAttributeArray[0].ToIntSafe( 0 ) > 0 )
            {
                LoadPersonAttributeDict();
            }
            if ( newAttributeArray[1].ToIntSafe( 0 ) > 0 )
            {
                LoadFamilyAttributeDict();
            }
            if ( newAttributeArray[2].ToIntSafe( 0 ) > 0 )
            {
                LoadGroupAttributeDict();
            }

            ReportProgress( 0, "Checking for new DefinedValues used in csv data." );
            var definedValuesAdded = 0;
            if ( this.PersonCsvList.Count > 0 )
            {
                definedValuesAdded += AddEntityDataDVs( Rock.SystemGuid.DefinedType.PERSON_MARITAL_STATUS );
                definedValuesAdded += AddEntityDataDVs( Rock.SystemGuid.DefinedType.PERSON_TITLE );
                definedValuesAdded += AddEntityDataDVs( Rock.SystemGuid.DefinedType.PERSON_SUFFIX );
                definedValuesAdded += AddEntityDataDVs( Rock.SystemGuid.DefinedType.PERSON_RECORD_STATUS_REASON );
                definedValuesAdded += AddEntityDataDVs( Rock.SystemGuid.DefinedType.PERSON_CONNECTION_STATUS );
            }
            if ( this.PersonAddressCsvList.Count > 0 || BusinessAddressCsvList.Count > 0 || GroupAddressCsvList.Count > 0 )
            {
                definedValuesAdded += AddEntityDataDVs( Rock.SystemGuid.DefinedType.GROUP_LOCATION_TYPE );
            }
            if ( this.PersonPhoneCsvList.Count > 0 || BusinessPhoneCsvList.Count > 0 )
            {
                definedValuesAdded += AddEntityDataDVs( Rock.SystemGuid.DefinedType.PERSON_PHONE_TYPE );
                definedValuesAdded += AddPhoneCountryCodeDVs();
            }
            if ( this.FinancialTransactionCsvList.Count > 0 )
            {
                definedValuesAdded += AddEntityDataDVs( Rock.SystemGuid.DefinedType.FINANCIAL_CURRENCY_TYPE );
                definedValuesAdded += AddEntityDataDVs( Rock.SystemGuid.DefinedType.FINANCIAL_SOURCE_TYPE );
                definedValuesAdded += AddEntityDataDVs( Rock.SystemGuid.DefinedType.FINANCIAL_NONCASH_ASSET_TYPE );
                definedValuesAdded += AddEntityDataDVs( Rock.SystemGuid.DefinedType.FINANCIAL_CREDIT_CARD_TYPE );
                definedValuesAdded += AddEntityDataDVs( Rock.SystemGuid.DefinedType.FINANCIAL_TRANSACTION_TYPE );
            }
            if ( this.PersonAttributeValueCsvList.Count > 0 || this.BusinessAttributeValueCsvList.Count > 0 || this.GroupAttributeValueCsvList.Count > 0 || this.EntityAttributeValueCsvList.Count > 0 )
            {
                var csvAttributeValues_Person = this.PersonAttributeValueCsvList.Select( a => new AttributeValueObject { AttributeKey = a.AttributeKey, AttributeValue = a.AttributeValue, EntityTypeId = PersonEntityTypeId } );
                var csvAttributeValues_Business = this.BusinessAttributeValueCsvList.Select( a => new AttributeValueObject { AttributeKey = a.AttributeKey, AttributeValue = a.AttributeValue, EntityTypeId = PersonEntityTypeId } );
                var csvAttributeValues_Group = this.GroupAttributeValueCsvList.Select( a => new AttributeValueObject { AttributeKey = a.AttributeKey, AttributeValue = a.AttributeValue, EntityTypeId = GroupEntityTypeId } );

                var csvAttributeValuesEnumerable = csvAttributeValues_Person
                    .Concat( csvAttributeValues_Business )
                    .Concat( csvAttributeValues_Group );

                if ( this.EntityAttributeValueCsvList.Count > 0 )
                {
                    var entityTypes = EntityTypeCache.All().Where( e => e.IsEntity && e.IsSecured ).ToList();
                    var csvAttributeValues_Entity = this.EntityAttributeValueCsvList.Select( a => new AttributeValueObject { AttributeKey = a.AttributeKey, AttributeValue = a.AttributeValue, EntityTypeId = entityTypes.FirstOrDefault( et => et.Name.Equals( a.EntityTypeName ) )?.Id } ).Where( ao => ao.EntityTypeId.HasValue );
                    csvAttributeValuesEnumerable = csvAttributeValuesEnumerable.Concat( csvAttributeValues_Entity );
                }

                var csvAttributeValues = csvAttributeValuesEnumerable.Distinct().ToList();

                definedValuesAdded += AddAttributeValueDVs( csvAttributeValues );
            }

            if ( definedValuesAdded > 0 )
            {
                LoadDefinedTypeDict();
                LoadDefinedValueDictionaries();
            }

            //// Import Person related data

            if ( this.PersonCsvList.Count > 0 )
            {
                completed += ImportPersonList();
            }
            if ( this.PersonAddressCsvList.Count > 0 )
            {
                completed += ImportPersonAddresses();
            }
            if ( this.PersonAttributeValueCsvList.Count > 0 )
            {
                completed += ImportPersonAttributeValues();
            }
            if ( this.PersonPhoneCsvList.Count > 0 )
            {
                completed += ImportPersonPhones();
            }
            if ( this.PersonSearchKeyCsvList.Count > 0 )
            {
                completed += ImportPersonSearchKeys();
            }
            if ( this.PersonHistoryCsvList.Count > 0 )
            {
                completed += ImportPersonHistory();
            }
            if ( this.BusinessCsvList.Count > 0 )
            {
                completed += ImportBusinesses();
            }
            if ( this.BusinessAddressCsvList.Count > 0 )
            {
                completed += ImportBusinessAddresses();
            }
            if ( this.BusinessAttributeValueCsvList.Count > 0 )
            {
                completed += ImportBusinessAttributeValues();
            }
            if ( this.BusinessPhoneCsvList.Count > 0 )
            {
                completed += ImportBusinessPhones();
            }
            if ( this.BusinessContactCsvList.Count > 0 )
            {
                completed += ImportBusinessContacts();
            }

            //// Insert Group related Data

            if ( this.LocationCsvList.Count > 0 )
            {
                completed += ImportLocations();
            }
            if ( this.ScheduleCsvList.Count > 0 )
            {
                completed += ImportSchedules();
            }
            if ( this.GroupCsvList.Count > 0 )
            {
                completed += ImportGroups();
            }
            if ( this.GroupAddressCsvList.Count > 0 )
            {
                completed += ImportGroupAddresses();
            }
            if ( this.GroupAttributeValueCsvList.Count > 0 )
            {
                completed += ImportGroupAttributeValues();
            }

            // need to import financial accounts before fundraising groups
            if ( this.FinancialAccountCsvList.Count > 0 )
            {
                completed += ImportFinancialAccounts();
            }
            
            if ( this.FundraisingGroupCsvList.Count > 0 )
            {
                completed += ImportFundraisingGroups();
            }
            if ( this.GroupMemberCsvList.Count > 0 )
            {
                completed += ImportGroupMembers();
            }
            if ( this.GroupMemberHistoricalCsvList.Count > 0 )
            {
                completed += ImportGroupMemberHistorical();
            }
            if ( this.AttendanceCsvList.Count > 0 )
            {
                completed += ProcessAttendance();
            }

            var groupPolygonInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.GROUPPOLYGON );
            if ( groupPolygonInstance != null )
            {
                completed += LoadGroupPolygon( groupPolygonInstance );
            }

            //// Insert Financial related data

            var financialbatchInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BATCH );
            if ( FinancialBatchCsvList.Count > 0 )
            {
                completed += ImportFinancialBatch();
            }

            var scheduledTransactionInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.SCHEDULEDTRANSACTION );
            if ( scheduledTransactionInstance != null )
            {
                completed += LoadScheduledTransaction( scheduledTransactionInstance );
            }

            if ( this.FinancialTransactionCsvList.Count > 0 && this.FinancialTransactionDetailCsvList.Count > 0  )
            {
                completed += ImportFinancialTransactions();
            }

            var financialPledgeInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PLEDGE );
            if ( financialPledgeInstance != null )
            {
                completed += MapPledge( financialPledgeInstance );
            }

            var bankAccountInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BANKACCOUNT );
            if ( bankAccountInstance != null )
            {
                completed += MapBankAccount( bankAccountInstance );
            }

            //// Miscellaneous data

            var metricInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.METRICS );
            if ( metricInstance != null )
            {
                completed += LoadMetrics( metricInstance );
            }

            var relationshipInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.RELATIONSHIP );
            if ( relationshipInstance != null )
            {
                completed += LoadRelationshipGroupMember( relationshipInstance );
            }

            var entityAttributeInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.ENTITYATTRIBUTE );
            if ( entityAttributeInstance != null )
            {
                completed += LoadEntityAttributes( entityAttributeInstance );
            }

            var contentChannelInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.CONTENTCHANNEL );
            if ( contentChannelInstance != null )
            {
                completed += LoadContentChannel( contentChannelInstance );
            }

            var contentChannelItemInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.CONTENTCHANNELITEM );
            if ( contentChannelInstance != null )
            {
                completed += LoadContentChannelItem( contentChannelItemInstance );
            }

            var noteInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.NOTE );
            if ( PersonNoteCsvList.Count > 0 )
            {
                completed += ImportEntityNote<Person>( PersonNoteCsvList, null );
            }
            else if ( noteInstance != null )
            {
                completed += LoadNote( noteInstance );
            }

            var prayerRequestInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PRAYERREQUEST );
            if ( prayerRequestInstance != null )
            {
                completed += LoadPrayerRequest( prayerRequestInstance );
            }

            var previousLastNameInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PREVIOUSLASTNAME );
            if ( previousLastNameInstance != null )
            {
                completed += LoadPersonPreviousName( previousLastNameInstance );
            }

            var connectionRequestInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.CONNECTIONREQUEST );
            if ( connectionRequestInstance != null )
            {
                completed += LoadConnectionRequest( connectionRequestInstance );
            }

            var benevolenceRequestInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BENEVOLENCEREQUEST );
            if ( benevolenceRequestInstance != null )
            {
                completed += LoadBenevolenceRequest( benevolenceRequestInstance );
            }

            var benevolenceResultInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BENEVOLENCERESULT );
            if ( benevolenceResultInstance != null )
            {
                completed += LoadBenevolenceResult( benevolenceResultInstance );
            }

            var userLoginInstance = selectedCsvData.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.USERLOGIN );
            if ( userLoginInstance != null )
            {
                completed += LoadUserLogin( userLoginInstance );
            }

            if ( this.EntityAttributeValueCsvList.Count > 0 )
            {
                completed += LoadEntityAttributeValues();
            }

            // Update any new AttributeValues to set the [ValueAsDateTime] field.
            AttributeValueService.UpdateAllValueAsDateTimeFromTextValue();

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

            ReportProgress( 0, "Loading relavent existing data dictionaries." );

            LoadDictionaries( lookupContext );

            LoadImportedGroups( lookupContext );

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
            return individualId.HasValue ? ImportedPeopleKeys.Values.FirstOrDefault( p => p.PersonForeignId == individualId ) : null;
        }

        /// <summary>
        /// Gets the person keys.
        /// </summary>
        /// <param name="individualKey">The individual identifier.</param>
        /// <returns></returns>
        protected PersonKeys GetPersonKeys( string individualKey = null )
        {
            if ( individualKey.AsIntegerOrNull() != null )
            {
                return GetPersonKeys( individualKey.AsIntegerOrNull() );
            }
            else
            {
                return ImportedPeopleKeys.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, individualKey ) );
            }
        }

        /// <summary>
        /// Loads the person keys.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadPersonKeys( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            ImportedPeopleKeys = new PersonAliasService( lookupContext ).Queryable().AsNoTracking()
                .Where( pa => pa.Person.ForeignKey != null && pa.Person.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                .Select( pa => new PersonKeys
                {
                    PersonAliasId = pa.Id,
                    PersonId = pa.PersonId,
                    PersonForeignId = pa.Person.ForeignId,
                    PersonForeignKey = pa.Person.ForeignKey
                } )
                .DistinctBy( k => k.PersonForeignKey )
                .ToDictionary( k => k.PersonForeignKey, v => v );
        }

        /// <summary>
        /// Loads the dictionaries.
        /// </summary>
        private void LoadDictionaries( RockContext rockContext )
        {
            LoadDefinedValueDictionaries( rockContext );

            // People
            LoadPersonDict( rockContext );
            LoadFamilyDict( rockContext );

            // Person Attributes
            if ( this.PersonAttributeCsvList.Count > 0 || this.PersonAttributeValueCsvList.Count > 0 || this.BusinessAttributeCsvList.Count > 0 || this.BusinessAttributeValueCsvList.Count > 0 )
            {
                LoadPersonAttributeDict( rockContext );
            }

            // Family Attributes
            if ( this.FamilyAttributeCsvList.Count > 0 )
            {
                LoadFamilyAttributeDict( rockContext );
            }

            // FieldTypes
            this.FieldTypeDict = new FieldTypeService( rockContext ).Queryable().Select( a => a.Id ).ToList().Select( a => FieldTypeCache.Get( a ) ).ToDictionary( k => k.Class, v => v, StringComparer.OrdinalIgnoreCase );

            // Defined Types
            LoadDefinedTypeDict();

            // Group Attributes
            if ( this.GroupAttributeCsvList.Count > 0 || this.GroupAttributeValueCsvList.Count > 0 )
            {
                LoadGroupAttributeDict( rockContext );
            }

            // Person SearchKeys
            if ( this.PersonSearchKeyCsvList.Count > 0 )
            {
                LoadPersonSearchKeyDict( rockContext );
            }

            // Campuses
            LoadCampusDict( rockContext );

            // Locations
            LoadLocationDict( rockContext );
        }

        /// <summary>
        /// Loads the imported families.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadDefinedValueDictionaries( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            if ( this.PersonCsvList.Count > 0 || this.BusinessCsvList.Count > 0 )
            {
                if ( this.PersonCsvList.Count > 0 )
                {
                    this.TitleDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_TITLE.AsGuid() ).GetUniqueValues();
                    this.SuffixDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_SUFFIX.AsGuid() ).GetUniqueValues();
                    this.MaritalStatusDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_MARITAL_STATUS.AsGuid() ).GetUniqueValues();
                }
                this.PersonRecordTypeValuesDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_RECORD_TYPE.AsGuid() );
                this.RecordStatusDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_RECORD_STATUS.AsGuid() );
                this.RecordStatusReasonDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_RECORD_STATUS_REASON.AsGuid() );
                this.ConnectionStatusDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_CONNECTION_STATUS.AsGuid() ).GetUniqueValues();
            }
            if ( this.PersonAddressCsvList.Count > 0 || this.BusinessAddressCsvList.Count > 0 || this.GroupAddressCsvList.Count > 0 )
            {
                this.GroupLocationTypeDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.GROUP_LOCATION_TYPE.AsGuid() );
            }
            if ( this.PersonPhoneCsvList.Count > 0 || this.BusinessPhoneCsvList.Count > 0 )
            {
                this.PhoneNumberTypeDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.PERSON_PHONE_TYPE.AsGuid() ).GetUniqueValues();
            }
            if ( this.FinancialTransactionCsvList.Count > 0 )
            {
                this.CurrencyTypeValues = LoadDefinedValues( Rock.SystemGuid.DefinedType.FINANCIAL_CURRENCY_TYPE.AsGuid() ).GetUniqueValues();
                this.NonCashAssetTypeValues = LoadDefinedValues( Rock.SystemGuid.DefinedType.FINANCIAL_NONCASH_ASSET_TYPE.AsGuid() ).GetUniqueValues();
                this.CreditCardTypeValues = LoadDefinedValues( Rock.SystemGuid.DefinedType.FINANCIAL_CREDIT_CARD_TYPE.AsGuid() ).GetUniqueValues();
                this.TransactionSourceTypeValues = LoadDefinedValues( Rock.SystemGuid.DefinedType.FINANCIAL_SOURCE_TYPE.AsGuid() ).GetUniqueValues();
                this.TransactionTypeValues = LoadDefinedValues( Rock.SystemGuid.DefinedType.FINANCIAL_TRANSACTION_TYPE.AsGuid() ).GetUniqueValues();
            }
        }

        /// <summary>
        /// Loads the person keys.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadPersonDict( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            this.PersonDict = new PersonService( lookupContext )
                                            .Queryable( true, true )
                                            .Include( a => a.PhoneNumbers )
                                            .Include( a => a.Aliases )
                                            .Include( a => a.PrimaryFamily )
                                            .AsNoTracking()
                                            .Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                                            .ToList()
                                            .ToDictionary( k => k.ForeignKey, v => v );
            LoadPersonKeys();
        }

        /// <summary>
        /// Loads the imported families.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadFamilyDict( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            this.FamilyDict = new GroupService( lookupContext )
                                            .Queryable()
                                            .Include( g => g.Members )
                                            .Include( g => g.GroupLocations )
                                            .AsNoTracking()
                                            .Where( g => g.GroupTypeId == FamilyGroupTypeId && g.ForeignKey != null && g.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                                            .ToList()
                                            .ToDictionary( k => k.ForeignKey, v => v );
        }

        /// <summary>
        /// Loads the imported families.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadPersonAttributeDict( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            var personAttributes = new AttributeService( lookupContext ).Queryable().Where( a => a.EntityTypeId == PersonEntityTypeId ).ToList();
            this.PersonAttributeDict = personAttributes.ToDictionary( k => k.Key, v => v );
        }

        /// <summary>
        /// Loads the imported family Attributes.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadFamilyAttributeDict( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }

            var familyAttributes = new AttributeService( lookupContext ).Queryable().Where( a => a.EntityTypeId == GroupEntityTypeId && a.EntityTypeQualifierColumn == "GroupTypeId" && a.EntityTypeQualifierValue == FamilyGroupTypeId.ToString() );
            this.FamilyAttributeDict = familyAttributes.ToDictionary( k => k.Key, v => v, StringComparer.OrdinalIgnoreCase );
        }

        /// <summary>
        /// Loads the imported families.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadDefinedTypeDict()
        {
            DefinedTypeCache.Clear();
            this.DefinedTypeDict = DefinedTypeCache.All().Where( dt => dt.ForeignKey != null && dt.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) ).ToDictionary( k => k.ForeignKey, v => v );
        }

        /// <summary>
        /// Loads the imported group attributes.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadGroupAttributeDict( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            var groupAttributes = new AttributeService( lookupContext ).Queryable().Where( a => a.EntityTypeId == GroupEntityTypeId ).ToList();
            this.GroupAttributeDict = groupAttributes.ToDictionary( k => k.Key, v => v, StringComparer.OrdinalIgnoreCase );
        }

        /// <summary>
        /// Loads the imported group types.
        /// </summary>
        /// <param name="lookupContext"></param>
        protected void LoadPersonSearchKeyDict( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            this.PersonSearchKeyDict = new PersonSearchKeyService( lookupContext ).Queryable()
                .Where( o => o.ForeignKey != null && o.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                .ToDictionary( k => k.ForeignKey, v => v );
        }

        /// <summary>
        /// Loads the campuses.
        /// </summary>
        protected void LoadCampusDict( RockContext rockContext = null )
        {
            if ( rockContext == null )
            {
                rockContext = new RockContext();
            }
            if ( !this.UseExistingCampusIds )
            {
                this.CampusImportDict = rockContext.Campuses.AsNoTracking()
                    .Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) ).ToList().ToDictionary( k => k.ForeignKey, v => v );
            }
            else
            {
                this.CampusesDict = rockContext.Campuses.AsNoTracking().ToDictionary( k => k.Id, v => v );
            }
        }

        /// <summary>
        /// Loads the imported locations.
        /// </summary>
        /// <param name="lookupContext"></param>
        protected void LoadLocationDict( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            this.LocationsDict = lookupContext.Locations.AsNoTracking()
                .Where( l => l.ForeignKey != null && l.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                .ToDictionary( k => k.ForeignKey, v => v );
        }

        /// <summary>
        /// Loads the imported accounts.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadImportedAccounts( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            ImportedAccounts = new FinancialAccountService( lookupContext ).Queryable().AsNoTracking()
                .Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                .ToDictionary( a => a.ForeignKey, a => a );
        }

        /// <summary>
        /// Loads the imported accounts.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadImportedBatches( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            if ( csvBatchUseForeignKey )
            {
                ImportedBatches = new FinancialBatchService( lookupContext ).Queryable().AsNoTracking()
                    .Where( b => b.ForeignKey != null && b.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                    .ToDictionary( b => b.ForeignKey, b => ( int? ) b.Id );
            }
            else
            {
                ImportedBatches = new FinancialBatchService( lookupContext ).Queryable().AsNoTracking()
                    .Where( b => b.ForeignId != null && b.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                    .ToDictionary( b => b.ForeignId.ToString(), b => ( int? ) b.Id );
            }
        }

        /// <summary>
        /// Loads the imported groups.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadImportedGroups( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            this.ImportedGroups = lookupContext.Groups.AsNoTracking()
                .Where( g => ( g.GroupTypeId != FamilyGroupTypeId ) && g.ForeignKey != null && g.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) ).ToList();
        }

        /// <summary>
        /// Loads the imported groups.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadGroupDict( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            this.GroupDict = new GroupService( lookupContext )
                                            .Queryable()
                                            .Include( g => g.Members )
                                            .Include( g => g.GroupLocations )
                                            .AsNoTracking()
                                            .Where( g => g.GroupTypeId != FamilyGroupTypeId && g.GroupTypeId != KnownRelationshipGroupType.Id && g.ForeignKey != null && g.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                                            .ToList()
                                            .ToDictionary( k => k.ForeignKey, v => v );
        }

        /// <summary>
        /// Loads the imported group members.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        protected void LoadGroupMemberDict( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            this.GroupMemberDict = new GroupMemberService( lookupContext )
                                            .Queryable()
                                            .AsNoTracking()
                                            .Where( g => g.GroupTypeId != FamilyGroupTypeId && g.ForeignKey != null && g.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                                            .ToList()
                                            .ToDictionary( k => k.ForeignKey, v => v );
        }

        /// <summary>
        /// Loads the imported group types.
        /// </summary>
        /// <param name="lookupContext"></param>
        protected void LoadGroupTypeDict( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            this.GroupTypeDict = new GroupTypeService( lookupContext ).Queryable()
                .Where( t => t.Id != FamilyGroupTypeId && t.ForeignKey != null && t.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                .ToDictionary( k => k.ForeignKey, v => v );
        }

        /// <summary>
        /// Loads the imported group types.
        /// </summary>
        /// <param name="lookupContext"></param>
        protected void LoadAttendanceOccurrenceDict( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            this.AttendanceOccurrenceDict = new AttendanceOccurrenceService( lookupContext ).Queryable()
                .Where( o => o.ForeignKey != null && o.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                .ToDictionary( k => k.ForeignKey, v => v );
        }

        /// <summary>
        /// Loads the imported group types.
        /// </summary>
        /// <param name="lookupContext"></param>
        protected void LoadUserLoginDict( RockContext lookupContext = null )
        {
            if ( lookupContext == null )
            {
                lookupContext = new RockContext();
            }
            this.UserLoginDict = new UserLoginService( lookupContext ).Queryable()
                .Where( o => o.ForeignKey != null && o.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                .ToDictionary( k => k.ForeignKey, v => v );
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
                .Where( h => h.EntityTypeId == personEntityType.Id && h.ForeignKey != null && h.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) ).ToList();
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
                ReportProgress( 0, string.Format( "Family Attribute records: {0}", FamilyAttributeCsvList.Count ) );
            }

            // Attendance
            var attInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.ATTENDANCE );
            if ( attInstance != null )
            {
                this.AttendanceCsvList = LoadEntityImportListFromCsv<AttendanceCsv>( attInstance.FileName );
                ReportProgress( 0, string.Format( "Attendance records: {0}", this.AttendanceCsvList.Count ) );
            }

            //Locations
            var locInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.Locations );
            if ( locInstance != null )
            {
                LocationCsvList = LoadEntityImportListFromCsv<LocationCsv>( locInstance.FileName );
                ReportProgress( 0, string.Format( "Location records: {0}", LocationCsvList.Count ) );
            }

            //Schedules
            var scheduleInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.Schedule );
            if ( scheduleInstance != null )
            {
                ScheduleCsvList = LoadEntityImportListFromCsv<ScheduleCsv>( scheduleInstance.FileName );
                ReportProgress( 0, string.Format( "Schedule records: {0}", ScheduleCsvList.Count ) );
            }

            // Groups (non-family)
            LoadGroupDataLists( csvInstances );

            // Financial Accounts
            var financialAccountInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.FinancialAccount );
            if ( financialAccountInstance != null )
            {
                FinancialAccountCsvList = LoadEntityImportListFromCsv<FinancialAccountCsv>( financialAccountInstance.FileName );
                ReportProgress( 0, string.Format( "FinancialAcount records: {0}", FinancialAccountCsvList.Count ) );
            }

            // Financial Transactions and Financial Transaction Details
            var financialTransactionInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.FinancialTransaction );
            var financialTransactionDetailInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.FinancialTransactionDetail );
            if ( financialTransactionInstance != null && financialTransactionDetailInstance != null )
            {
                FinancialTransactionCsvList = LoadEntityImportListFromCsv<FinancialTransactionCsv>( financialTransactionInstance.FileName );
                ReportProgress( 0, string.Format( "FinancialTransaction records: {0}", FinancialTransactionCsvList.Count ) );
                FinancialTransactionDetailCsvList = LoadEntityImportListFromCsv<FinancialTransactionDetailCsv>( financialTransactionDetailInstance.FileName );
                ReportProgress( 0, string.Format( "FinancialTransactionDetail records: {0}", FinancialTransactionDetailCsvList.Count ) );
                var transactionLookup = FinancialTransactionCsvList.ToDictionary( k => k.Id, v => v );
                foreach ( var transactionDetailCsv in FinancialTransactionDetailCsvList )
                {
                    transactionLookup[transactionDetailCsv.TransactionId].FinancialTransactionDetails.Add( transactionDetailCsv );
                }
            }

            // Financial Batches
            var financialBatchInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.FinancialBatch );
            if ( financialBatchInstance != null )
            {
                FinancialBatchCsvList = LoadEntityImportListFromCsv<FinancialBatchCsv>( financialBatchInstance.FileName );
                var transactionCsvsByBatch = FinancialTransactionCsvList.GroupBy( b => b.BatchId ).ToDictionary( k => k.Key, v => v.ToList() );
                foreach ( var batchCsv in FinancialBatchCsvList )
                {
                    if ( transactionCsvsByBatch.ContainsKey( batchCsv.Id ) )
                    {
                        batchCsv.FinancialTransactions = transactionCsvsByBatch[batchCsv.Id];
                    }
                }
                ReportProgress( 0, string.Format( "FinancialBatch records: {0}", FinancialAccountCsvList.Count ) );
            }

            // Businesses
            LoadBusinessDataLists( csvInstances );
        }


        /// <summary>
        /// Loads the import data lists related to Person records.
        /// </summary>
        private void LoadPersonDataLists( List<CSVInstance> csvInstances )
        {
            var personInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.Person );
            if ( personInstance != null )
            {
                this.PersonCsvList = LoadEntityImportListFromCsv<PersonCsv>( personInstance.FileName );
                ReportProgress( 0, string.Format( "Person records from file: {0}", this.PersonCsvList.Count ) );
            }

            var personAddressInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PersonAddress );
            if ( personAddressInstance != null )
            {
                this.PersonAddressCsvList = LoadEntityImportListFromCsv<PersonAddressCsv>( personAddressInstance.FileName );
                ReportProgress( 0, string.Format( "PersonAddress records: {0}", this.PersonAddressCsvList.Count ) );
            }

            var personAttributeInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PersonAttribute );
            if ( personAttributeInstance != null )
            {
                this.PersonAttributeCsvList = LoadEntityImportListFromCsv<EntityAttributeCsv>( personAttributeInstance.FileName );
                ReportProgress( 0, string.Format( "PersonAttribute records: {0}", this.PersonAttributeCsvList.Count ) );
            }

            var personAttributeValueInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PersonAttributeValue );
            if ( personAttributeValueInstance != null )
            {
                this.PersonAttributeValueCsvList = LoadEntityImportListFromCsv<PersonAttributeValueCsv>( personAttributeValueInstance.FileName );
                ReportProgress( 0, string.Format( "PersonAttributeValue records: {0}", this.PersonAttributeValueCsvList.Count ) );
            }

            var personPhoneInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PersonPhone );
            if ( personPhoneInstance != null )
            {
                this.PersonPhoneCsvList = LoadEntityImportListFromCsv<PersonPhoneCsv>( personPhoneInstance.FileName );
                ReportProgress( 0, string.Format( "Person Phone records in file: {0}", this.PersonPhoneCsvList.Count ) );
            }

            var personSearchKeyInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PersonSearchKey );
            if ( personSearchKeyInstance != null )
            {
                this.PersonSearchKeyCsvList = LoadEntityImportListFromCsv<PersonSearchKeyCsv>( personSearchKeyInstance.FileName );
                ReportProgress( 0, string.Format( "PersonSearchKey records: {0}", this.PersonSearchKeyCsvList.Count ) );
            }

            var personHistoryInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PERSONHISTORY );
            if ( personHistoryInstance != null )
            {
                this.PersonHistoryCsvList = LoadEntityImportListFromCsv<PersonHistoryCsv>( personHistoryInstance.FileName );
                ReportProgress( 0, string.Format( "PersonHistory records: {0}", this.PersonHistoryCsvList.Count ) );
            }

            var personNoteInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.PersonNote );
            if ( personNoteInstance != null )
            {
                this.PersonNoteCsvList = LoadEntityImportListFromCsv<PersonNoteCsv>( personNoteInstance.FileName );
                ReportProgress( 0, string.Format( "PersonNote records: {0}", this.PersonNoteCsvList.Count ) );
            }
        }

        /// <summary>
        /// Loads the import data lists related to Busniesses.
        /// </summary>
        private void LoadBusinessDataLists( List<CSVInstance> csvInstances )
        {
            var businessInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.Business );
            if ( businessInstance != null )
            {
                this.BusinessCsvList = LoadEntityImportListFromCsv<BusinessCsv>( businessInstance.FileName );
                ReportProgress( 0, string.Format( "Business records: {0}", this.BusinessCsvList.Count ) );
            }

            var businessAddressInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BusinessAddress );
            if ( businessAddressInstance != null )
            {
                this.BusinessAddressCsvList = LoadEntityImportListFromCsv<BusinessAddressCsv>( businessAddressInstance.FileName );
                ReportProgress( 0, string.Format( "BusinessAddress records: {0}", this.BusinessAddressCsvList.Count ) );
            }

            var businessAttributeInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BusinessAttribute );
            if ( businessAttributeInstance != null )
            {
                this.BusinessAttributeCsvList = LoadEntityImportListFromCsv<EntityAttributeCsv>( businessAttributeInstance.FileName );
                ReportProgress( 0, string.Format( "BusinessAttribute records: {0}", this.BusinessAttributeCsvList.Count ) );
            }

            var businessAttributeValueInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BusinessAttributeValue );
            if ( businessAttributeValueInstance != null )
            {
                this.BusinessAttributeValueCsvList = LoadEntityImportListFromCsv<BusinessAttributeValueCsv>( businessAttributeValueInstance.FileName );
                ReportProgress( 0, string.Format( "BusinessAttributeValue records: {0}", this.BusinessAttributeValueCsvList.Count ) );
            }

            var businessPhoneInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BusinessPhone );
            if ( businessPhoneInstance != null )
            {
                this.BusinessPhoneCsvList = LoadEntityImportListFromCsv<BusinessPhoneCsv>( businessPhoneInstance.FileName );
                ReportProgress( 0, string.Format( "Business Phone records in file: {0}", this.BusinessPhoneCsvList.Count ) );
            }

            var businessContactInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.BusinessContact );
            if ( businessContactInstance != null )
            {
                this.BusinessContactCsvList = LoadEntityImportListFromCsv<BusinessContactCsv>( businessContactInstance.FileName );
                ReportProgress( 0, string.Format( "BusinessContact records: {0}", this.BusinessContactCsvList.Count ) );
            }
        }

        /// <summary>
        /// Loads the import data lists related to Groups.
        /// </summary>
        private void LoadGroupDataLists( List<CSVInstance> csvInstances )
        {
            var groupInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.Group );
            if ( groupInstance != null )
            {
                this.GroupCsvList = LoadEntityImportListFromCsv<GroupCsv>( groupInstance.FileName ).DistinctBy( g => g.Id ).ToList();
                ReportProgress( 0, string.Format( "Group records: {0}", this.GroupCsvList.Count ) );
            }

            var groupAddressInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.GroupAddress );
            if ( groupAddressInstance != null )
            {
                this.GroupAddressCsvList = LoadEntityImportListFromCsv<GroupAddressCsv>( groupAddressInstance.FileName );
                ReportProgress( 0, string.Format( "GroupAddress records: {0}", this.GroupAddressCsvList.Count ) );
            }

            var groupAttributeInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.GroupAttribute );
            if ( groupAttributeInstance != null )
            {
                this.GroupAttributeCsvList = LoadEntityImportListFromCsv<EntityAttributeCsv>( groupAttributeInstance.FileName );
                ReportProgress( 0, string.Format( "GroupAttribute records: {0}", this.GroupAttributeCsvList.Count ) );
            }

            var groupAttributeValueInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.GroupAttributeValue );
            if ( groupAttributeValueInstance != null )
            {
                this.GroupAttributeValueCsvList = LoadEntityImportListFromCsv<GroupAttributeValueCsv>( groupAttributeValueInstance.FileName );
                ReportProgress( 0, string.Format( "GroupAttributeValue records: {0}", this.GroupAttributeValueCsvList.Count ) );
            }

            var groupMemberInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.GroupMember );
            if ( groupMemberInstance != null )
            {
                this.GroupMemberCsvList = LoadEntityImportListFromCsv<GroupMemberCsv>( groupMemberInstance.FileName );
                ReportProgress( 0, string.Format( "GroupMember records: {0}", this.GroupMemberCsvList.Count ) );
            }

            var groupMemberHistoricalInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.GroupMemberHistorical );
            if ( groupMemberHistoricalInstance != null )
            {
                this.GroupMemberHistoricalCsvList = LoadEntityImportListFromCsv<GroupMemberHistoricalCsv>( groupMemberHistoricalInstance.FileName );
                ReportProgress( 0, string.Format( "GroupMemberHistorical records: {0}", this.GroupMemberHistoricalCsvList.Count ) );
            }

            var groupTypeInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.GroupType );
            if ( groupTypeInstance != null )
            {
                this.GroupTypeCsvList = LoadEntityImportListFromCsv<GroupTypeCsv>( groupTypeInstance.FileName );
                ReportProgress( 0, string.Format( "GroupType records: {0}", this.GroupTypeCsvList.Count ) );
            }

            var fundraisingGroupInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.Fundraising );
            if ( fundraisingGroupInstance != null )
            {
                this.FundraisingGroupCsvList = LoadEntityImportListFromCsv<FundraisingGroupCsv>( fundraisingGroupInstance.FileName ).DistinctBy( g => g.Id ).ToList();
                ReportProgress( 0, string.Format( "Fundraising Group records: {0}", this.FundraisingGroupCsvList.Count ) );
            }

            var entityAttributeValueInstance = csvInstances.FirstOrDefault( i => i.RecordType == CSVInstance.RockDataType.EntityAttributeValue );
            if ( entityAttributeValueInstance != null )
            {
                this.EntityAttributeValueCsvList = LoadEntityImportListFromCsv<EntityAttributeValueCsv>( entityAttributeValueInstance.FileName );
                ReportProgress( 0, string.Format( "EntityAttributeValue records: {0}", this.EntityAttributeValueCsvList.Count ) );
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
                    var config = new CsvConfiguration( CultureInfo.InvariantCulture )
                    {
                        PrepareHeaderForMatch = args => args.Header.ToLower(),
                        HasHeaderRecord = true,
                        HeaderValidated = null,
                        MissingFieldFound = null
                    };
                    using ( var fileStream = File.OpenText( fileName ) )
                    {
                        CsvHelper.CsvReader csvReader = new CsvHelper.CsvReader( fileStream, config );
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

        private const int MetricId = 0;
        private const int MetricName = 1;
        private const int MetricCategory = 2;
        private const int PartitionCampus = 3;
        private const int PartitionService = 4;
        private const int PartitionGroup = 5;
        private const int MetricValue = 6;
        private const int MetricValueId = 7;
        private const int MetricValueDate = 8;
        private const int MetricValueNote = 9;

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
        private const int EntityId = 2;                            /* Int [Required] */
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

        #region EntityAttribute Constants

        /*
         * This is the definition of the csv format for the EntityAttribute.csv file.
         */

        private const int AttributeEntityTypeName = 0;              /* String [Required] */
        private const int AttributeRockKey = 1;                     /* String [Optional, defaults to AttributeName without whitespace]    Note: AttributeId will be added as FK/FID to attributes that match Entity and Attribute Key with null FK/FID */
        private const int AttributeName = 2;                        /* String [Required] */
        private const int AttributeCategoryName = 3;                /* String [Optional] */
        private const int AttributeType = 4;                        /* "D" | "B" | "V" | "E" | "L" | "VL" | "" */
        private const int AttributeDefinedTypeId = 5;               /* String | Int [Optional] */
        private const int AttributeEntityTypeQualifierName = 6;     /* String [Optional] */
        private const int AttributeEntityTypeQualifierValue = 7;    /* String | Int [Optional] */

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

        /// <summary>
        /// Add any campuses from csv data that are not already in Rock
        /// </summary>
        private void AddCampuses()
        {
            List<CampusCsv> importCampuses = new List<CampusCsv>();
            var personCampuses = this.PersonCsvList.Select( a => a.Campus ).Where( a => a.CampusId.IsNotNullOrWhiteSpace() );
            var businessCampuses = this.BusinessCsvList.Select( a => a.Campus ).Where( a => a.CampusId.IsNotNullOrWhiteSpace() );
            var campuses = personCampuses.Concat( businessCampuses ).Distinct();

            foreach ( var campus in campuses )
            {
                if ( !importCampuses.Any( a => a.CampusId == campus.CampusId ) )
                {
                    importCampuses.Add( campus );
                }
            }

            var rockContext = new RockContext();
            var campusService = new CampusService( rockContext );

            CampusCache.Clear();

            var campusesToCreate = importCampuses.Where( a => !CampusCache.All().Any( c => c.ForeignKey == $"{this.ImportInstanceFKPrefix}^{a.CampusId}" ) );
            if ( campusesToCreate.Count() > 0 )
            {
                ReportProgress( 0, string.Format( "Creating {0} new Campuses...", campusesToCreate.Count() ) );
            }

            // Rock has a Unique Constraint on Campus.Name so, make sure campus name is unique and rename it if a new campus happens to have the same name as an existing campus
            var usedCampusNames = CampusCache.All().Select( a => a.Name ).ToList();

            foreach ( var importCampus in campusesToCreate )
            {
                var newCampus = new Campus()
                {
                    ForeignId = importCampus.CampusId.AsIntegerOrNull(),
                    ForeignKey = $"{this.ImportInstanceFKPrefix}^{importCampus.CampusId}",
                    IsActive = true,
                    Name = importCampus.CampusName,
                    Guid = Guid.NewGuid()
                };

                if ( usedCampusNames.Any( a => a.Equals( importCampus.CampusName ) ) )
                {
                    newCampus.Name = importCampus.CampusName + $" ({ImportInstanceFKPrefix})";
                }

                usedCampusNames.Add( newCampus.Name );
                campusService.Add( newCampus );
                rockContext.SaveChanges();
            }

            CampusCache.Clear();
            LoadCampusDict( rockContext );
        }

        /// <summary>
        /// Add Defined Values for core Defined Types in the data that are not in Rock yet
        /// </summary>
        private int AddEntityDataDVs( string definedTypeSystemGuid )
        {
            var rockContext = new RockContext();
            var dtService = new DefinedTypeService( rockContext );
            var importEntityList = new List<string>();
            var csvEntityValues = new List<string>();
            var definedTypeName = string.Empty;
            switch ( definedTypeSystemGuid )
            {
                case Rock.SystemGuid.DefinedType.PERSON_CONNECTION_STATUS:
                    csvEntityValues = this.PersonCsvList.Select( p => p.ConnectionStatus ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    definedTypeName = "Connection Status";
                    break;
                case Rock.SystemGuid.DefinedType.PERSON_MARITAL_STATUS:
                    csvEntityValues = this.PersonCsvList.Select( p => p.MaritalStatus ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    definedTypeName = "Marital Status";
                    break;
                case Rock.SystemGuid.DefinedType.PERSON_PHONE_TYPE:
                    var importedPhoneTypes_Person = this.PersonPhoneCsvList.Select( p => p.PhoneType ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    var importedPhoneTypes_Business = this.BusinessPhoneCsvList.Select( p => p.PhoneType ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();

                    csvEntityValues = importedPhoneTypes_Person
                        .Concat( importedPhoneTypes_Business )
                        .Distinct()
                        .ToList();
                    definedTypeName = "Phone Type";
                    break;
                case Rock.SystemGuid.DefinedType.PERSON_TITLE:
                    csvEntityValues = PersonCsvList.Select( p => p.Salutation ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    definedTypeName = "Title";
                    break;
                case Rock.SystemGuid.DefinedType.PERSON_SUFFIX:
                    csvEntityValues = PersonCsvList.Select( p => p.Suffix ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    definedTypeName = "Suffix";
                    break;
                case Rock.SystemGuid.DefinedType.PERSON_RECORD_STATUS_REASON:
                    var csvEntityValues_Person = this.PersonCsvList.Select( p => p.InactiveReason ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    var csvEntityValues_Business = this.BusinessCsvList.Select( p => p.InactiveReason ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    csvEntityValues = csvEntityValues_Person.Concat( csvEntityValues_Business ).Distinct().ToList();
                    definedTypeName = "Inactive Record Reason";
                    break;
                case Rock.SystemGuid.DefinedType.GROUP_LOCATION_TYPE:
                    var importedAddressTypes_Person = this.PersonAddressCsvList
                        .Select( a => Enum.GetName( typeof( CSVInstance.AddressType ), a.AddressType ) )
                        .Distinct()
                        .ToList();

                    var importedAddressTypes_Business = this.BusinessAddressCsvList
                        .Select( a => Enum.GetName( typeof( CSVInstance.AddressType ), a.AddressType ) )
                        .Distinct()
                        .ToList();

                    var importedAddressTypes_Group = this.GroupAddressCsvList
                        .Select( a => Enum.GetName( typeof( CSVInstance.AddressType ), a.AddressType ) )
                        .Distinct()
                        .ToList();

                    csvEntityValues = importedAddressTypes_Person
                        .Concat( importedAddressTypes_Business )
                        .Concat( importedAddressTypes_Group )
                        .Distinct()
                        .ToList();
                    definedTypeName = "Location Type (Group)";
                    break;
                case Rock.SystemGuid.DefinedType.FINANCIAL_CURRENCY_TYPE:
                    csvEntityValues = this.FinancialTransactionCsvList.Select( p => p.CurrencyType ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    definedTypeName = "Currency Type";
                    break;
                case Rock.SystemGuid.DefinedType.FINANCIAL_SOURCE_TYPE:
                    csvEntityValues = this.FinancialTransactionCsvList.Select( p => p.TransactionSource ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    definedTypeName = "Transaction Source";
                    break;
                case Rock.SystemGuid.DefinedType.FINANCIAL_NONCASH_ASSET_TYPE:
                    csvEntityValues = this.FinancialTransactionCsvList.Select( p => p.NonCashAssetType ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    definedTypeName = "Non-Cash Asset Types";
                    break;
                case Rock.SystemGuid.DefinedType.FINANCIAL_CREDIT_CARD_TYPE:
                    csvEntityValues = this.FinancialTransactionCsvList.Select( p => p.CreditCardType ).Where( r => !string.IsNullOrWhiteSpace( r ) ).Distinct().ToList();
                    definedTypeName = "Credit Card Type";
                    break;
                case Rock.SystemGuid.DefinedType.FINANCIAL_TRANSACTION_TYPE:
                    if ( this.FinancialTransactionCsvList.Any( r => r.TransactionType == CSVInstance.TransactionType.Receipt ) )
                    {
                        // Add the Transaction Type of 'Receipt' if there are in import records that use it
                        if ( DefinedValueCache.Get( Rock.SystemGuid.DefinedValue.TRANSACTION_TYPE_RECEIPT ) == null )
                        {
                            var definedType = dtService.Get( Rock.SystemGuid.DefinedType.FINANCIAL_TRANSACTION_TYPE );
                            var receiptDV = new DefinedValue
                            {
                                Guid = Rock.SystemGuid.DefinedValue.TRANSACTION_TYPE_RECEIPT.AsGuid(),
                                Value = "Receipt",
                                Description = "A Receipt Transaction"
                            };
                            definedType.DefinedValues.Add( receiptDV );
                            rockContext.SaveChanges();
                            ReportProgress( 0, "Created Receipt defined value in Transaction Type defined type." );
                            return 1;
                        }
                    }
                    definedTypeName = "Transaction Type";
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

            var entityDefinedType = dtService.Get( definedTypeSystemGuid.AsGuid() );

            DefinedTypeCache.Clear();

            var usedDTValues = DefinedTypeCache.Get( entityDefinedType.Guid ).DefinedValues.Select( v => v.Value ).ToList();
            importEntityList = importEntityList.Where( a => !usedDTValues.Any( r => a == r ) ).ToList();
            if ( importEntityList.Count() > 0 )
            {
                ReportProgress( 0, string.Format( "Creating {0} new {1} defined values...", importEntityList.Count(), definedTypeName ) );
                foreach ( var importValue in importEntityList )
                {
                    var newValue = new DefinedValue()
                    {
                        ForeignKey = $"{this.ImportInstanceFKPrefix}^{importValue}".Left( 100 ),
                        Value = importValue,
                        Guid = Guid.NewGuid(),
                    };

                    usedDTValues.Add( newValue.Value );
                    entityDefinedType.DefinedValues.Add( newValue );
                }
                rockContext.SaveChanges();
            }

            DefinedTypeCache.Clear();
            return importEntityList.Count();
        }

        /// <summary>
        /// Add Defined Values for phone country codes from csv data that are not yet in Rock
        /// </summary>
        private int AddPhoneCountryCodeDVs( RockContext rockContext = null )
        {
            if ( rockContext == null )
            {
                rockContext = new RockContext();
            }
            var importedCountryCodes_Person = this.PersonPhoneCsvList.Where( c => c.CountryCode.HasValue ).Select( p => p.CountryCode.Value ).Distinct().ToList();
            var importedCountryCodes_Business = this.BusinessPhoneCsvList.Where( c => c.CountryCode.HasValue ).Select( p => p.CountryCode.Value ).Distinct().ToList();

            var csvCountryCodes = importedCountryCodes_Person
                .Concat( importedCountryCodes_Business )
                .Distinct()
                .ToList();

            var countryCodeDT = new DefinedTypeService( rockContext ).Get( Rock.SystemGuid.DefinedType.COMMUNICATION_PHONE_COUNTRY_CODE.AsGuid() );
            var existingCountryCodeValues = countryCodeDT.DefinedValues.Select( v => v.Value.ToIntSafe() ).Distinct();

            var countryCodeDVsToCreate = csvCountryCodes.Where( c => !existingCountryCodeValues.Any( v => v == c ) );

            if ( countryCodeDVsToCreate.Any() )
            {
                foreach ( var countryCode in countryCodeDVsToCreate )
                {
                    var ccMatchExprAttribute = FindEntityAttribute( rockContext, string.Empty, "MatchRegEx", DefinedValueEntityTypeId, string.Empty );
                    var ccFormatExprAttribute = FindEntityAttribute( rockContext, string.Empty, "FormatRegEx", DefinedValueEntityTypeId, string.Empty );
                    var attributeValueService = new AttributeValueService( rockContext );
                    var countryCodeDefinitions = typeof( CSVPhoneCountryCode ).GetFields()
                                                                        .Select( c => ( CountryCodeData ) c.GetValue( null ) )
                                                                        .Where( c => c.CountryCode == countryCode ).ToList();

                    foreach ( var ccData in countryCodeDefinitions )
                    {
                        var newCountryCodeDVCache = AddDefinedValue( rockContext, Rock.SystemGuid.DefinedType.COMMUNICATION_PHONE_COUNTRY_CODE, ccData.CountryCode.ToString(), description: ccData.Description, order: ccData.Order );

                        // Set Matching Expression Attribute Value
                        if ( ccMatchExprAttribute != null )
                        {
                            var ccMatchExprAttributeVal = new AttributeValue
                            {
                                EntityId = newCountryCodeDVCache.Id,
                                AttributeId = ccMatchExprAttribute.Id,
                                Value = ccData.MatchExpression
                            };
                            attributeValueService.Add( ccMatchExprAttributeVal );

                            newCountryCodeDVCache.AttributeValues.Remove( ccMatchExprAttribute.Key );
                            newCountryCodeDVCache.AttributeValues.Add( ccMatchExprAttribute.Key, new AttributeValueCache
                            {
                                AttributeId = ccMatchExprAttribute.Id,
                                Value = ccData.MatchExpression
                            } );
                        }

                        // Set Format Expression Attribute Value
                        if ( ccFormatExprAttribute != null )
                        {
                            var ccFormatExprAttributeVal = new AttributeValue
                            {
                                EntityId = newCountryCodeDVCache.Id,
                                AttributeId = ccFormatExprAttribute.Id,
                                Value = ccData.FormatExpression
                            };
                            attributeValueService.Add( ccFormatExprAttributeVal );

                            newCountryCodeDVCache.AttributeValues.Remove( ccFormatExprAttribute.Key );
                            newCountryCodeDVCache.AttributeValues.Add( ccFormatExprAttribute.Key, new AttributeValueCache
                            {
                                AttributeId = ccFormatExprAttribute.Id,
                                Value = ccData.FormatExpression
                            } );
                        }
                    }
                }

                // Refresh and reorder defined values
                var definedType = new DefinedTypeService( rockContext ).Get( Rock.SystemGuid.DefinedType.COMMUNICATION_PHONE_COUNTRY_CODE.AsGuid() );
                var index = 0;
                foreach ( var dv in definedType.DefinedValues.OrderBy( dv => dv.Value.AsInteger() ).ThenBy( dv => dv.Order ) )
                {
                    dv.Order = index;
                    index++;
                }
                rockContext.SaveChanges();
            }
            return countryCodeDVsToCreate.Count();
        }

        /// <summary>
        /// Add Defined Values for attribute values from csv data that are not yet in Rock
        /// </summary>
        private int AddAttributeValueDVs( List<AttributeValueObject> attributeValues, RockContext rockContext = null )
        {
            if ( rockContext == null )
            {
                rockContext = new RockContext();
            }
            var entityTypeIds = new List<int>();

            entityTypeIds.Add( PersonEntityTypeId );
            entityTypeIds.Add( GroupEntityTypeId );

            var definedTypeDict = DefinedTypeCache.All().ToDictionary( k => k.Id, v => v );
            var attributeDefinedTypeDict = new AttributeService( rockContext ).Queryable()
                                                                                .Where( a => a.FieldTypeId == DefinedValueFieldTypeId && entityTypeIds.Any( e => e == a.EntityTypeId ) )
                                                                                .ToDictionary( k => $"{k.EntityTypeId}_{k.Key}", v => definedTypeDict.GetValueOrNull( v.AttributeQualifiers.FirstOrDefault( aq => aq.Key == "definedtype" ).Value.AsIntegerOrNull().Value ) );
            
            var attributeDefinedValuesDict = attributeDefinedTypeDict.ToDictionary( k => k.Key, v => v.Value.DefinedValues.ToDictionary( a => a.Value, b => b.DefinedTypeId ) );

            var attributeValuesToProcess = attributeValues.Where( v => attributeDefinedValuesDict.ContainsKey( $"{v.EntityTypeId}_{v.AttributeKey}" ) && !attributeDefinedValuesDict[$"{v.EntityTypeId}_{v.AttributeKey}"].ContainsKey( v.AttributeValue  ) )
                                                          .Select( v => new AttributeValueObject { AttributeKey = v.AttributeKey, AttributeValue = v.AttributeValue, EntityTypeId = v.EntityTypeId, DefinedTypeId = attributeDefinedTypeDict.GetValueOrNull( $"{v.EntityTypeId}_{v.AttributeKey}" )?.Id } )
                                                          .GroupBy( o => new { o.DefinedTypeId, o.AttributeValue } )
                                                          .Select( grp => grp.First() )
                                                          .OrderBy( g => g.DefinedTypeId )
                                                          .ToList();

            var definedValuesToAdd = new List<DefinedValue>();
            DefinedTypeCache attributeDefinedType = null;

            if ( attributeValuesToProcess.Count > 0 )
            {
                ReportProgress( 0, $"Creating {attributeValuesToProcess.Count} new DefinedValues..." );

                foreach ( var attributeValue in attributeValuesToProcess )
                {
                    attributeDefinedType = attributeDefinedTypeDict.GetValueOrNull( $"{attributeValue.EntityTypeId}_{attributeValue.AttributeKey}" );

                    var attributeDefinedValue = attributeDefinedType?.DefinedValues.FirstOrDefault( dv => dv.Value == attributeValue.AttributeValue );
                    if ( attributeDefinedValue != null )
                    {
                        continue;
                    }
                    else
                    {
                        var newDefinedValue = new DefinedValue
                        {
                            DefinedTypeId = attributeDefinedType.Id,
                            Value = attributeValue.AttributeValue,
                            Order = 0,
                            ForeignKey = this.ImportInstanceFKPrefix + "^"
                        };
                        definedValuesToAdd.Add( newDefinedValue );
                    }
                }

                if ( definedValuesToAdd.Count > 0 )
                {
                    rockContext.BulkInsert( definedValuesToAdd );
                    DefinedTypeCache.Clear();
                }
            }

            return definedValuesToAdd.Count();
        }

        /// <summary>
        /// Adds any attribute categories that are in the csv files (person and family attributes)
        /// </summary>
        private void AddAttributeCategories()
        {
            var csvCategoryNames = new List<string>();
            if ( this.PersonAttributeCsvList.Count > 0 )
            {
                csvCategoryNames = this.PersonAttributeCsvList.Where( a => !string.IsNullOrWhiteSpace( a.Category ) ).Select( a => a.Category ).Distinct().ToList();
            }
            if ( this.FamilyAttributeCsvList.Count > 0 )
            {
                csvCategoryNames.AddRange( this.FamilyAttributeCsvList.Where( a => !string.IsNullOrWhiteSpace( a.Category ) ).Select( a => a.Category ).Distinct().ToList() );
            }
            if ( this.BusinessAttributeCsvList.Count > 0 )
            {
                csvCategoryNames.AddRange( this.BusinessAttributeCsvList.Where( a => !string.IsNullOrWhiteSpace( a.Category ) ).Select( a => a.Category ).Distinct().ToList() );
            }

            var rockContext = new RockContext();
            var categoryService = new CategoryService( rockContext );

            var attributeCategoryList = categoryService.Queryable().Where( a => a.EntityTypeId == AttributeEntityTypeId ).ToList();

            foreach ( var categoryName in csvCategoryNames.Distinct().ToList() )
            {
                if ( !attributeCategoryList.Any( a => a.Name.Equals( categoryName, StringComparison.OrdinalIgnoreCase ) ) )
                {
                    var attributeCategory = new Category()
                    {
                        Name = categoryName,
                        EntityTypeId = AttributeEntityTypeId,
                        EntityTypeQualifierColumn = "EntityTypeId",
                        EntityTypeQualifierValue = PersonEntityTypeId.ToString(),
                        Guid = Guid.NewGuid()
                    };

                    categoryService.Add( attributeCategory );
                    attributeCategoryList.Add( attributeCategory );
                }

                rockContext.SaveChanges();
            }

            var csvGroupCategoryNames = GroupAttributeCsvList.Where( a => !string.IsNullOrWhiteSpace( a.Category ) ).Select( a => a.Category ).Distinct().ToList();

            foreach ( var categoryName in csvGroupCategoryNames.Distinct().ToList() )
            {
                if ( !attributeCategoryList.Any( a => a.Name.Equals( categoryName, StringComparison.OrdinalIgnoreCase ) ) )
                {
                    var attributeCategory = new Category()
                    {
                        Name = categoryName,
                        EntityTypeId = AttributeEntityTypeId,
                        EntityTypeQualifierColumn = "EntityTypeId",
                        EntityTypeQualifierValue = GroupEntityTypeId.ToString(),
                        Guid = Guid.NewGuid()
                    };

                    categoryService.Add( attributeCategory );
                    attributeCategoryList.Add( attributeCategory );
                }

                rockContext.SaveChanges();
            }
        }

        /// <summary>
        /// Adds the attributes.
        /// </summary>
        private string AddAttributes()
        {
            var rockContext = new RockContext();
            var attributeService = new AttributeService( rockContext );

            var attributeCategoryList = new CategoryService( rockContext ).Queryable().Where( a => a.EntityTypeId == AttributeEntityTypeId ).ToList();

            // Add any Person, Family, or Group Attributes that aren't in Rock yet
            var newPersonAttributes = new List<EntityAttributeCsv>();
            var newBusinessAttributes = new List<EntityAttributeCsv>();
            var newFamilyAttributes = new List<EntityAttributeCsv>();
            var newGroupAttributes = new List<EntityAttributeCsv>();
            
            if ( this.PersonAttributeCsvList.Count > 0 )
            {
                newPersonAttributes = this.PersonAttributeCsvList.Where( a => !PersonAttributeDict.Keys.Any( ad => ad.Equals( a.Key, StringComparison.OrdinalIgnoreCase ) ) ).ToList();
                foreach ( var personAttribute in newPersonAttributes )
                {
                    personAttribute.AttributeEntityType = AttributeEntityType.Person;
                }
            }

            if ( this.BusinessAttributeCsvList.Count > 0 )
            {
                newBusinessAttributes = this.BusinessAttributeCsvList.Where( a => !PersonAttributeDict.Keys.Any( ad => ad.Equals( a.Key, StringComparison.OrdinalIgnoreCase ) ) ).ToList();
                foreach ( var businessAttribute in newBusinessAttributes )
                {
                    businessAttribute.AttributeEntityType = AttributeEntityType.Business;
                }
            }

            if ( this.FamilyAttributeCsvList.Count > 0 )
            {
                newFamilyAttributes = this.FamilyAttributeCsvList.Where( a => !FamilyAttributeDict.Keys.Any( ad => ad.Equals( a.Key, StringComparison.OrdinalIgnoreCase ) ) ).ToList();
                foreach ( var familyAttribute in newFamilyAttributes )
                {
                    familyAttribute.AttributeEntityType = AttributeEntityType.Family;
                }
            }

            if ( this.GroupAttributeCsvList.Count > 0 )
            {
                if ( this.GroupTypeDict == null )
                {
                    LoadGroupTypeDict();
                }
                newGroupAttributes = this.GroupAttributeCsvList.Where( a => !GroupAttributeDict.Keys.Any( ad => ad.Equals( a.Key, StringComparison.OrdinalIgnoreCase ) ) ).ToList();
                foreach ( var groupAttribute in newGroupAttributes )
                {
                    groupAttribute.AttributeEntityType = AttributeEntityType.Group;
                }
            }

            var newAttributes = newPersonAttributes
                                    .Concat( newBusinessAttributes )
                                    .Concat( newFamilyAttributes )
                                    .Concat( newGroupAttributes )
                                    .GroupBy( a => new { a.AttributeEntityType, a.Key } )
                                    .Select( grp => grp.First() );
            
            ReportProgress( 0, string.Format( "Creating {0} new Person, Family, or Group Attributes...", newAttributes.Count() ) );
            var invalidDefinedTypeAttributes = new List<string>();
            foreach ( var attribute in newAttributes )
            {
                var keyEntityTypeString = $"{attribute.Key}_{attribute.AttributeEntityType}";
                var newAttribute = new Rock.Model.Attribute()
                {
                    Key = attribute.Key,
                    Name = attribute.Name,
                    Guid = Guid.NewGuid(),
                    EntityTypeId = PersonEntityTypeId,
                    FieldTypeId = FieldTypeDict[attribute.FieldType].Id,
                    ForeignKey = $"{this.ImportInstanceFKPrefix}^{keyEntityTypeString}"
                };

                if ( attribute.AttributeEntityType == AttributeEntityType.Business )
                {
                    newAttribute.EntityTypeQualifierColumn = "RecordTypeValueId";
                    newAttribute.EntityTypeQualifierValue = this.PersonRecordTypeValuesDict[Rock.SystemGuid.DefinedValue.PERSON_RECORD_TYPE_BUSINESS.AsGuid()].Id.ToString();
                }
                else if ( attribute.AttributeEntityType == AttributeEntityType.Family )
                {
                    newAttribute.EntityTypeId = GroupEntityTypeId;
                    newAttribute.EntityTypeQualifierColumn = "GroupTypeId";
                    newAttribute.EntityTypeQualifierValue = FamilyGroupTypeId.ToString();
                }
                else if ( attribute.AttributeEntityType == AttributeEntityType.Group )
                {
                    int? groupTypeId = null;
                    if ( attribute.GroupTypeId.IsNotNullOrWhiteSpace() )
                    {
                        groupTypeId = this.GroupTypeDict.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{attribute.GroupTypeId}" )?.Id;
                    }
                    if ( !groupTypeId.HasValue)
                    {
                        groupTypeId = this.GroupTypeDict.Values.FirstOrDefault( gt => gt.Name.Equals( attribute.Category ) )?.Id;
                    }
                    if ( !groupTypeId.HasValue )
                    {
                        LogException( "Attribute", $"Invalid GroupTypeId for attribute \"{ attribute.Name }\" (Key: { attribute.Key }) in group-attribute csv file. Attribute was not imported." );
                        continue;
                    }
                    newAttribute.EntityTypeId = GroupEntityTypeId;
                    newAttribute.EntityTypeQualifierColumn = "GroupTypeId";
                    newAttribute.EntityTypeQualifierValue = groupTypeId.Value.ToString();
                }

                if ( !string.IsNullOrWhiteSpace( attribute.Category ) )
                {
                    var attributeCategory = attributeCategoryList.FirstOrDefault( a => a.Name.Equals( attribute.Category, StringComparison.OrdinalIgnoreCase ) );
                    if ( attributeCategory != null )
                    {
                        newAttribute.Categories = new List<Category>();
                        newAttribute.Categories.Add( attributeCategory );
                    }
                }

                // Add default attribute qualifiers based on field type.
                var attributeQualifier = new AttributeQualifier();
                var attributeQualifiers = new List<AttributeQualifier>();
                switch ( attribute.FieldType )
                {
                    case "Rock.Field.Types.DateFieldType":
                        newAttribute.Description = attribute.Name + " Date created by import";
                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "format",
                            Value = "",
                            IsSystem = false,
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );

                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "displayDiff",
                            Value = "false",
                            IsSystem = false
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );

                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "displayCurrentOption",
                            Value = "false",
                            IsSystem = false
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );
                        break;
                    case "Rock.Field.Types.BooleanFieldType":
                        newAttribute.Description = attribute.Name + " Boolean created by import";
                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "truetext",
                            Value = "Yes",
                            IsSystem = false
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );

                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "falsetext",
                            Value = "No",
                            IsSystem = false
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );
                        break;
                    case "Rock.Field.Types.DefinedValueFieldType":
                        newAttribute.Description = attribute.Name + " Defined Type created by import";
                        var definedType = this.DefinedTypeDict.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{attribute.DefinedTypeId}" );
                        if ( definedType != null )
                        {
                            attributeQualifier = new AttributeQualifier
                            {
                                Key = "definedtype",
                                Value = definedType.Id.ToString(),
                                IsSystem = false
                            };
                            newAttribute.AttributeQualifiers.Add( attributeQualifier );

                            attributeQualifier = new AttributeQualifier
                            {
                                Key = "displaydescription",
                                Value = "false",
                                IsSystem = false
                            };
                            newAttribute.AttributeQualifiers.Add( attributeQualifier );

                            attributeQualifier = new AttributeQualifier
                            {
                                Key = "allowmultiple",
                                Value = attribute.DefinedTypeAllowMultiple.GetValueOrDefault().ToString(),
                                IsSystem = false
                            };
                            newAttribute.AttributeQualifiers.Add( attributeQualifier );
                        }
                        else
                        {
                            invalidDefinedTypeAttributes.Add( keyEntityTypeString );
                        }
                        break;
                    case "Rock.Field.Types.ValueListFieldType":
                        newAttribute.Description = attribute.Name + " Value List Type created by import";
                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "definedtype",
                            Value = "",
                            IsSystem = false
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );

                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "customvalues",
                            Value = "",
                            IsSystem = false
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );

                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "valueprompt",
                            Value = "",
                            IsSystem = false
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );
                        break;
                    case "Rock.Field.Types.SelectSingleFieldType":
                        newAttribute.Description = attribute.Name + " Single Select created by import";
                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "values",
                            Value = "Pass,Fail",
                            IsSystem = false
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );

                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "fieldtype",
                            Value = "ddl",
                            IsSystem = false
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );
                        break;
                    case "Rock.Field.Types.UrlLinkFieldType":
                        newAttribute.Description = attribute.Name + " URL Link created by import";
                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "ShouldAlwaysShowCondensed",
                            Value = "False",
                            IsSystem = false
                        };

                        newAttribute.AttributeQualifiers.Add( attributeQualifier );

                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "ShouldRequireTrailingForwardSlash",
                            Value = "False",
                            IsSystem = false
                        };

                        newAttribute.AttributeQualifiers.Add( attributeQualifier );
                        break;
                    case "Rock.Field.Types.HtmlFieldType":
                        newAttribute.Description = attribute.Name + " HTML created by import";
                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "documentfolderroot",
                            Value = string.Empty,
                            IsSystem = false
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );

                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "imagefolderroot",
                            Value = string.Empty,
                            IsSystem = false
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );

                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "toolbar",
                            Value = "Light",
                            IsSystem = false
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );

                        attributeQualifier = new AttributeQualifier
                        {
                            Key = "userspecificroot",
                            Value = "False",
                            IsSystem = false
                        };
                        newAttribute.AttributeQualifiers.Add( attributeQualifier );
                        break;
                    default:
                        newAttribute.Description = attribute.Name + " created by import";
                        break;
                }
                attributeService.Add( newAttribute );
            }
            rockContext.SaveChanges();

            if ( invalidDefinedTypeAttributes.Count > 0 )
            {
                var invalidAttributeStrings = invalidDefinedTypeAttributes.Select( a => string.Format( "{0} ({1})", a.Split( '_' )[0], a.Split( '_' )[1] ) );
                LogException( "Attribute", $"The following Attributes where created but not connected to a DefinedType due to invalid DefinedTypeId in the attributes csv:\r\n{string.Join( ",", invalidAttributeStrings )}" );
            }
            return $"{newPersonAttributes.Count + newBusinessAttributes.Count}_{newFamilyAttributes.Count}_{newGroupAttributes.Count}";
        }

        /// <summary>
        /// Adds new defined types for attributes.
        /// </summary>
        private void AddAttributeDefinedTypes( RockContext rockContext = null )
        {
            // Add any Defined Types from imported person, family, business, or group attributes that aren't in Rock yet

            if ( rockContext == null )
            {
                rockContext = new RockContext();
            }

            var definedTypeList = new List<DefinedType>();

            var csvAttributes_Person = this.PersonAttributeCsvList.Select( a => new AttributeObject { AttributeKey = a.Key, AttributeEntityType = a.AttributeEntityType, AttributeName = a.Name, DefinedTypeId = a.DefinedTypeId } );
            var csvAttributes_Family = this.FamilyAttributeCsvList.Select( a => new AttributeObject { AttributeKey = a.Key, AttributeEntityType = a.AttributeEntityType, AttributeName = a.Name, DefinedTypeId = a.DefinedTypeId } );
            var csvAttributes_Business = this.BusinessAttributeCsvList.Select( a => new AttributeObject { AttributeKey = a.Key, AttributeEntityType = a.AttributeEntityType, AttributeName = a.Name, DefinedTypeId = a.DefinedTypeId } );
            var csvAttributes_Group = this.GroupAttributeCsvList.Select( a => new AttributeObject { AttributeKey = a.Key, AttributeEntityType = a.AttributeEntityType, AttributeName = a.Name, DefinedTypeId = a.DefinedTypeId } );

            var newDefinedTypeAttributes = csvAttributes_Person
                .Concat( csvAttributes_Family )
                .Concat( csvAttributes_Business )
                .Concat( csvAttributes_Group )
                .Where( a => !string.IsNullOrWhiteSpace( a.DefinedTypeId ) && !this.DefinedTypeDict.ContainsKey( $"{this.ImportInstanceFKPrefix}^{a.DefinedTypeId}" ) )
                .GroupBy( a => a.DefinedTypeId )
                .Select( grp => grp.First() );

            if ( newDefinedTypeAttributes.Count() > 0 )
            {
                ReportProgress( 0, string.Format( "Creating {0} new Defined Types for Attributes...", newDefinedTypeAttributes.Count() ) );
                foreach ( var attribute in newDefinedTypeAttributes )
                {
                    var newDefinedType = new DefinedType()
                    {
                        Name = attribute.AttributeName.Left( 87 ) + " Defined Type",
                        Guid = Guid.NewGuid(),
                        Description = string.Format( "Defined Type created by import for attribute {0}.", attribute.AttributeName ),
                        FieldTypeId = FieldTypeCache.Get( Rock.SystemGuid.FieldType.TEXT ).Id,
                        ForeignKey = $"{this.ImportInstanceFKPrefix}^{attribute.DefinedTypeId}",
                        ForeignId = attribute.DefinedTypeId.AsIntegerOrNull()
                    };
                    definedTypeList.Add( newDefinedType );
                }
                rockContext.BulkInsert( definedTypeList );
                LoadDefinedTypeDict();
            }
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

    public class AttributeObject
    {
        public string AttributeKey { get; set; }

        public AttributeEntityType? AttributeEntityType { get; set; }

        public string AttributeName { get; set; }

        public string DefinedTypeId { get; set; }
    }

    public class AttributeValueObject
    {
        public string AttributeKey { get; set; }

        public string AttributeValue { get; set; }

        public int? EntityTypeId { get; set; }

        public int? DefinedTypeId { get; set; }
    }
}