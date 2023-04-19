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
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Linq;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the PersonHistory related import methods
    /// </summary>
    partial class CSVComponent
    {
        #region PersonHistory Methods

        /// <summary>
        /// Loads the Person History data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadPersonHistory( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var entityTypes = EntityTypeCache.All().Where( e => e.IsEntity && e.IsSecured ).ToList();
            var personEntityType = entityTypes.FirstOrDefault( et => et.Guid == Rock.SystemGuid.EntityType.PERSON.AsGuid() );
            var importedHistory = lookupContext.Histories.AsNoTracking()
                .Where( h => h.EntityTypeId == personEntityType.Id && h.ForeignKey != null && h.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) ).ToList();


            var historyList = new List<History>();
            var skippedHistories = new Dictionary<string, string>();

            var completedItems = 0;
            var addedItems = 0;
            ReportProgress( 0, string.Format( "Verifying person history import ({0:N0} already imported).", importedHistory.Count ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var historyId = row[HistoryId];
                var rowHistoryPersonId = row[HistoryPersonId];
                var historyCategory = row[HistoryCategory];
                var changedByPersonId = row[ChangedByPersonId];
                var historyVerb = row[Verb];
                var caption = row[Caption];
                var valueName = row[ValueName];
                var changeType = row[ChangeType] ?? "Property";
                var relatedEntityType = row[RelatedEntityType];
                var relatedEntityId = row[RelatedEntityId].AsIntegerOrNull();
                var newValue = row[NewValue];
                var oldValue = row[OldValue];
                var historyDateTime = row[HistoryDateTime].AsDateTime();
                var isSensitive = row[IsSensitive].AsBoolean( false );

                if ( string.IsNullOrWhiteSpace( historyVerb ) )
                {
                    historyVerb = "[Imported]";
                }
                int? historyPersonId = null;
                var personKeys = GetPersonKeys( rowHistoryPersonId );
                if ( personKeys != null )
                {
                    historyPersonId = personKeys.PersonId;
                }

                if ( historyPersonId.HasValue && historyPersonId.Value > 0 )
                {
                    var history = importedHistory.FirstOrDefault( h => h.ForeignKey.Equals( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, historyId ) ) );
                    if ( history == null )
                    {
                        var creatorKeys = GetPersonKeys( changedByPersonId );
                        var creatorAliasId = creatorKeys != null ? ( int? ) creatorKeys.PersonAliasId : null;
                        int? relatedEntityTypeId = null;
                        if ( !string.IsNullOrWhiteSpace( relatedEntityType ) )
                        {
                            switch ( relatedEntityType )
                            {
                                case "Person":
                                    relatedEntityTypeId = entityTypes.FirstOrDefault( et => et.Guid == Rock.SystemGuid.EntityType.PERSON.AsGuid() ).Id;
                                    break;
                                case "Group":
                                    relatedEntityTypeId = entityTypes.FirstOrDefault( et => et.Guid == Rock.SystemGuid.EntityType.GROUP.AsGuid() ).Id;
                                    break;
                                case "Attribute":
                                    relatedEntityTypeId = entityTypes.FirstOrDefault( et => et.Guid == Rock.SystemGuid.EntityType.ATTRIBUTE.AsGuid() ).Id;
                                    break;
                                case "UserLogin":
                                    relatedEntityTypeId = entityTypes.FirstOrDefault( et => et.Guid == "0FA592F1-728C-4885-BE38-60ED6C0D834F".AsGuid() ).Id;
                                    break;
                                case "PersonSearchKey":
                                    relatedEntityTypeId = entityTypes.FirstOrDefault( et => et.Guid == "478F7E34-4AD8-4459-9D41-25C2907C1583".AsGuid() ).Id;
                                    break;
                                default:
                                    break;
                            }
                        }
                        if ( !relatedEntityTypeId.HasValue )
                        {
                            relatedEntityId = null;
                        }

                        history = AddHistory( lookupContext, personEntityType, historyPersonId.Value, historyCategory, verb: historyVerb, changeType: changeType, caption: caption,
                            valueName: valueName, newValue: newValue, oldValue: oldValue, relatedEntityTypeId: relatedEntityTypeId, relatedEntityId: relatedEntityId, isSensitive: isSensitive,
                            dateCreated: historyDateTime, foreignKey: historyId, creatorPersonAliasId: creatorAliasId, instantSave: false, foreignKeyPrefix: this.ImportInstanceFKPrefix );

                        historyList.Add( history );
                        addedItems++;
                    }
                    completedItems++;
                    if ( completedItems % ( DefaultChunkSize * 10 ) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} history entries processed.", completedItems ) );
                    }

                    if ( completedItems % DefaultChunkSize < 1 )
                    {
                        SavePersonHistory( historyList );
                        ReportPartialProgress();
                        importedHistory.AddRange( historyList );

                        lookupContext = new RockContext();
                        historyList.Clear();
                    }
                }
                else
                {
                    skippedHistories.Add( historyId, rowHistoryPersonId );
                }
            }

            if ( historyList.Any() )
            {
                SavePersonHistory( historyList );
            }

            if ( skippedHistories.Any() )
            {
                ReportProgress( 0, "The following history entries could not be imported and were skipped:" );
                foreach ( var keyValPair in skippedHistories )
                {
                    ReportProgress( 0, string.Format( "HistoryId {0} for HistoryPersonId {1}", keyValPair.Key, keyValPair.Value ) );
                }
            }

            ReportProgress( 100, string.Format( "Finished person history import: {0:N0} history entries imported.", addedItems ) );
            return completedItems;
        }

        /// <summary>
        /// Saves the histories.
        /// </summary>
        /// <param name="historyList">The history list.</param>
        private static void SavePersonHistory( List<History> historyList )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.Histories.AddRange( historyList );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }

        /// <summary>
        /// Loads the Person History data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int ImportPersonHistory()
        {
            this.ReportProgress( 0, "Preparing Person History data for import" );

            var entityTypes = EntityTypeCache.All().Where( e => e.IsEntity && e.IsSecured ).ToDictionary( k => k.Guid, v => v.Id );
            var personEntityTypeId = entityTypes[Rock.SystemGuid.EntityType.PERSON.AsGuid()];
            var historyEntityTypeId = entityTypes[Rock.SystemGuid.EntityType.HISTORY.AsGuid()];
            var personHistoryParentCategory = CategoryCache.Get( Rock.SystemGuid.Category.HISTORY_PERSON );
            var rockContext = new RockContext();
            var importedHistory = rockContext.Histories.AsNoTracking()
                .Where( h => h.EntityTypeId == personEntityTypeId && h.ForeignKey != null && h.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                .ToList()
                .ToDictionary( k => k.ForeignKey, v => v );

            // Load necessary dictionaries for relevant entity types

            this.ReportProgress( 0, "Loading relevant entity dictionaries" );
            var csvEntityTypes = this.PersonHistoryCsvList.Select( h => h.RelatedEntityType ).Distinct();
            foreach ( var entityType in csvEntityTypes )
            {
                switch ( entityType )
                {
                    case "Group":
                        if ( this.GroupDict == null )
                        {
                            LoadGroupDict();
                        }
                        break;
                    case "Attribute":
                        if ( this.PersonAttributeDict == null )
                        {
                            LoadPersonAttributeDict();
                        }
                        break;
                    case "UserLogin":
                        if ( this.UserLoginDict == null )
                        {
                            LoadUserLoginDict();
                        }
                        break;
                    case "PersonSearchKey":
                        if ( this.PersonSearchKeyDict == null )
                        {
                            LoadPersonSearchKeyDict();
                        }
                        break;
                    default:
                        break;
                }
            }

            // First, create any categories that do not yet exist

            this.ReportProgress( 0, "Check for Person History categories" );
            var categoryNameLookup = new CategoryService( rockContext ).Queryable().Where( c => c.EntityTypeId == historyEntityTypeId && c.ParentCategoryId == personHistoryParentCategory.Id ).Select( c => c.Name.ToLower() ).ToList();
            var csvCategoriesToCreate = this.PersonHistoryCsvList.Select( h => h.HistoryCategory.ToLower() ).Distinct().Where( c => !categoryNameLookup.Any( n => n == c ) ).ToList();

            var categoriesToInsert = new List<Category>();
            if ( csvCategoriesToCreate.Any() )
            {
                this.ReportProgress( 0, $"Creating {csvCategoriesToCreate} new Person History Categories" );
                foreach ( var category in csvCategoriesToCreate )
                {
                    var newCategory = new Category
                    {
                        IsSystem = false,
                        EntityTypeId = historyEntityTypeId,
                        ParentCategoryId = personHistoryParentCategory.Id,
                        Name = category,
                        Order = 0
                    };
                    categoriesToInsert.Add( newCategory );
                }
                rockContext.BulkInsert( categoriesToInsert );
                this.ReportProgress( 0, $"Finished creating Person History categories" );
            }

            var categoryNameIdLookup = new CategoryService( rockContext ).Queryable().Where( c => c.EntityTypeId == historyEntityTypeId && c.ParentCategoryId == personHistoryParentCategory.Id ).ToDictionary( k => k.Name.ToLower(), v => v.Id );

            var invalidPersonIds = new List<string>();
            var existingHistories = 0;
            var errors = string.Empty;

            // Slice data into chunks and process
            var personHistoriesRemainingToProcess = this.PersonHistoryCsvList.Count;
            var workingPersonHistoryImportList = this.PersonHistoryCsvList.ToList();
            var completed = 0;

            this.ReportProgress( 0, string.Format( "Begin processing {0} Person History Records...", this.PersonHistoryCsvList.Count ) );

            while ( personHistoriesRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.AttendanceChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} Histories processed." );
                }

                if ( completed % this.AttendanceChunkSize < 1 )
                {
                    var csvChunk = workingPersonHistoryImportList.Take( Math.Min( this.AttendanceChunkSize, workingPersonHistoryImportList.Count ) ).ToList();
                    var imported = BulkPersonHistoryImport( csvChunk, invalidPersonIds, importedHistory, existingHistories, entityTypes, categoryNameIdLookup, rockContext );
                    completed += imported;
                    personHistoriesRemainingToProcess -= csvChunk.Count;
                    workingPersonHistoryImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            this.ReportProgress( 0, $"{existingHistories} Histories already existed and were skipped." );
            this.ReportProgress( 0, $"{invalidPersonIds.Count} Histories were skipped due to the following invalid HistoryPersonIds:\r\n{string.Join( ",", invalidPersonIds.Distinct() )}" );
            if ( errors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, errors, showMessage: false, hasMultipleErrors: true );
            }
            return completed;
        }

        private int BulkPersonHistoryImport( List<PersonHistoryCsv> personHistoryCsvList, List<string> invalidPersonIds, Dictionary<string, History> importedHistory, int existingHistories, Dictionary<Guid, int> entityTypes, Dictionary<string, int> categoryNameIdLookup, RockContext rockContext = null )
        {
            if ( rockContext == null )
            {
                rockContext = new RockContext();
            }

            var personHistoryImports = new List<PersonHistoryImport>();

            foreach ( var personHistoryCsv in personHistoryCsvList )
            {
                var foreignKey = $"{this.ImportInstanceFKPrefix}^{personHistoryCsv.HistoryId}";
                var person = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, personHistoryCsv.HistoryPersonId ) );
                if ( person == null )
                {
                    invalidPersonIds.Add( personHistoryCsv.HistoryPersonId );
                    continue;
                }
                if ( importedHistory.ContainsKey( foreignKey ) )
                {
                    existingHistories++;
                    continue;
                }
                int? relatedEntityTypeId = null;
                int? relatedEntityId = null;
                if ( personHistoryCsv.RelatedEntityType.IsNotNullOrWhiteSpace() && personHistoryCsv.RelatedEntityId.IsNotNullOrWhiteSpace() )
                {
                    relatedEntityTypeId = GetRelatedEntityTypeId( personHistoryCsv.RelatedEntityType, entityTypes );
                    if ( relatedEntityTypeId.HasValue )
                    {
                        relatedEntityId = GetRelatedEntityId( personHistoryCsv.RelatedEntityId, entityTypes.FirstOrDefault( vp => vp.Value == relatedEntityTypeId.Value ) );
                    }
                    if ( !relatedEntityId.HasValue )
                    {
                        relatedEntityTypeId = null;
                    }
                }
                var categoryId = categoryNameIdLookup.GetValueOrNull( personHistoryCsv.HistoryCategory.ToLower() );
                if ( !categoryId.HasValue )
                {
                    continue;
                }
                var newPersonHistory = new PersonHistoryImport
                {
                    PersonId = person.Id,
                    Verb = personHistoryCsv.Verb.IsNotNullOrWhiteSpace() ? personHistoryCsv.Verb : "[Imported]",
                    CategoryId = categoryId,
                    ChangedByPersonAliasId = ImportedPeopleKeys.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{personHistoryCsv.ChangedByPersonId}" )?.PersonAliasId,
                    RelatedEntityTypeId = relatedEntityTypeId,
                    RelatedEntityId = relatedEntityId,
                    IsSensitive = personHistoryCsv.IsSensitive,
                    ChangeType = personHistoryCsv.ChangeType,
                    Caption = personHistoryCsv.Caption,
                    ValueName = personHistoryCsv.ValueName,
                    OldValue = personHistoryCsv.OldValue,
                    NewValue = personHistoryCsv.NewValue,
                    ForeignId = personHistoryCsv.HistoryId.AsIntegerOrNull(),
                    ForeignKey = $"{this.ImportInstanceFKPrefix}^{personHistoryCsv.HistoryId}",
                    HistoryDateTime = personHistoryCsv.HistoryDateTime
                };
                personHistoryImports.Add( newPersonHistory );
            }

            var personHistoryTypeId = entityTypes[Rock.SystemGuid.EntityType.PERSON.AsGuid()];
            var historiesToInsert = new List<History>();
            foreach ( var personHistoryImport in personHistoryImports )
            {
                var history = new History
                {
                    EntityTypeId = personHistoryTypeId,
                    EntityId = personHistoryImport.PersonId,
                    CategoryId = personHistoryImport.CategoryId.Value,
                    IsSystem = false,
                    IsSensitive = personHistoryImport.IsSensitive,
                    Verb = personHistoryImport.Verb,
                    ChangeType = personHistoryImport.ChangeType,
                    Caption = personHistoryImport.Caption,
                    ValueName = personHistoryImport.ValueName,
                    NewValue = personHistoryImport.NewValue,
                    OldValue = personHistoryImport.OldValue,
                    RelatedEntityTypeId = personHistoryImport.RelatedEntityTypeId,
                    RelatedEntityId = personHistoryImport.RelatedEntityId,
                    ForeignId = personHistoryImport.ForeignId,
                    ForeignKey = personHistoryImport.ForeignKey,
                    CreatedDateTime = personHistoryImport.HistoryDateTime,
                    ModifiedDateTime = personHistoryImport.HistoryDateTime,
                    CreatedByPersonAliasId = personHistoryImport.ChangedByPersonAliasId,
                    ModifiedByPersonAliasId = personHistoryImport.ChangedByPersonAliasId
                };
                historiesToInsert.Add( history );
            }

            rockContext.BulkInsert( historiesToInsert );
            return personHistoryCsvList.Count;
        }

        private int? GetRelatedEntityTypeId( string relatedEntityType, Dictionary<Guid, int> entityTypes )
        {
            int? relatedEntityTypeId = null;
            if ( !string.IsNullOrWhiteSpace( relatedEntityType ) )
            {
                switch ( relatedEntityType )
                {
                    case "Person":
                        relatedEntityTypeId = entityTypes[Rock.SystemGuid.EntityType.PERSON.AsGuid()];
                        break;
                    case "Group":
                        relatedEntityTypeId = entityTypes[Rock.SystemGuid.EntityType.GROUP.AsGuid()];
                        break;
                    case "Attribute":
                        relatedEntityTypeId = entityTypes[Rock.SystemGuid.EntityType.ATTRIBUTE.AsGuid()];
                        break;
                    case "UserLogin":
                        relatedEntityTypeId = entityTypes["0FA592F1-728C-4885-BE38-60ED6C0D834F".AsGuid()];
                        break;
                    case "PersonSearchKey":
                        relatedEntityTypeId = entityTypes["478F7E34-4AD8-4459-9D41-25C2907C1583".AsGuid()];
                        break;
                    default:
                        break;
                }
            }
            return relatedEntityTypeId;
        }

        private int? GetRelatedEntityId( string relatedEntityId, KeyValuePair<Guid, int> entityTypeId )
        {
            int? entityId = null;
            var entityIdString = $"{this.ImportInstanceFKPrefix}^{relatedEntityId}";
            if ( relatedEntityId.IsNotNullOrWhiteSpace() )
            {
                switch ( entityTypeId.Key.ToString() )
                {
                    case Rock.SystemGuid.EntityType.PERSON:
                        entityId = ImportedPeopleKeys.GetValueOrNull( relatedEntityId )?.PersonId;
                        break;
                    case Rock.SystemGuid.EntityType.GROUP:
                        entityId = this.GroupDict.GetValueOrNull( entityIdString )?.Id;
                        break;
                    case Rock.SystemGuid.EntityType.ATTRIBUTE:
                        entityId = this.PersonAttributeDict.GetValueOrNull( entityIdString )?.Id;
                        break;
                    case "0FA592F1-728C-4885-BE38-60ED6C0D834F":  // UserLogin
                        entityId = this.UserLoginDict.GetValueOrNull( entityIdString )?.Id;
                        break;
                    case "478F7E34-4AD8-4459-9D41-25C2907C1583":  // PersonSearchKey
                        entityId = this.PersonSearchKeyDict.GetValueOrNull( entityIdString )?.Id;
                        break;
                    default:
                        break;
                }
            }
            return entityId;
        }
    }


    #endregion
}
