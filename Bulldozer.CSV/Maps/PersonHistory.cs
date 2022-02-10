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
using System.Linq;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using static Bulldozer.Utility.CachedTypes;
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
            var importedHistory = new HistoryService( lookupContext ).Queryable().Count( n => n.ForeignKey != null );

            var entityTypes = EntityTypeCache.All().Where( e => e.IsEntity && e.IsSecured ).ToList();

            var historyList = new List<History>();
            var skippedHistories = new Dictionary<string, string>();

            var completedItems = 0;
            ReportProgress( 0, string.Format( "Verifying person history import ({0:N0} already imported).", importedHistory ) );

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

                if ( string.IsNullOrWhiteSpace( historyVerb ))
                {
                    historyVerb = "[Imported]";
                }
                int? historyPersonId = null;
                var personEntityType = entityTypes.FirstOrDefault( et => et.Guid == Rock.SystemGuid.EntityType.PERSON.AsGuid() );
                var personKeys = GetPersonKeys( rowHistoryPersonId );
                if ( personKeys != null )
                {
                    historyPersonId = personKeys.PersonId;
                }

                if ( historyPersonId.HasValue && historyPersonId.Value > 0 )
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

                    var history = AddHistory( lookupContext, personEntityType, historyPersonId.Value, historyCategory, verb: historyVerb, changeType: changeType, caption: caption, 
                        valueName: valueName, newValue: newValue, oldValue: oldValue, relatedEntityTypeId: relatedEntityTypeId, relatedEntityId: relatedEntityId, isSensitive: isSensitive,
                        dateCreated: historyDateTime, foreignKey: historyId, creatorPersonAliasId: creatorAliasId, instantSave: false );

                    historyList.Add( history );
                    completedItems++;
                    if ( completedItems % ( ReportingNumber * 10 ) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} history entries processed.", completedItems ) );
                    }

                    if ( completedItems % ReportingNumber < 1 )
                    {
                        SavePersonHistory( historyList );
                        ReportPartialProgress();
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

            ReportProgress( 100, string.Format( "Finished person history import: {0:N0} history entries imported.", completedItems ) );
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
    }

    #endregion
}
