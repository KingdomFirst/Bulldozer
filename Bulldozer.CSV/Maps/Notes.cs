// <copyright>
// Copyright 2019 by Kingdom First Solutions
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
    /// Partial of CSVComponent that holds the Note related import methods
    /// </summary>
    partial class CSVComponent
    {
        #region Notes Methods

        /// <summary>
        /// Loads the Notes data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadNote( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var importedNotes = new NoteService( lookupContext ).Queryable().Count( n => n.ForeignKey != null );

            var entityTypes = EntityTypeCache.All().Where( e => e.IsEntity && e.IsSecured ).ToList();

            var noteList = new List<Note>();
            var skippedNotes = new Dictionary<string, string>();

            var completedItems = 0;
            ReportProgress( 0, string.Format( "Verifying note import ({0:N0} already imported).", importedNotes ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var noteType = row[NoteType] as string;
                var entityTypeName = row[EntityTypeName] as string;
                var entityForeignKey = row[EntityForeignId];
                var noteCaption = row[NoteCaption] as string;
                var noteText = row[NoteText] as string;
                var createdDate = row[NoteDate].AsDateTime();
                var createdByKey = row[CreatedById];
                var rowIsAlert = row[IsAlert];
                var rowIsPrivate = row[IsPrivate];

                var isAlert = ( bool ) ParseBoolOrDefault( rowIsAlert, false );
                var isPrivate = ( bool ) ParseBoolOrDefault( rowIsPrivate, false );

                int? noteTypeId = null;
                int? noteEntityId = null;
                var entityType = entityTypes.FirstOrDefault( et => et.Name.Equals( entityTypeName ) );
                if ( entityType != null )
                {
                    var entityTypeInstance = entityType.GetEntityType();
                    if ( entityTypeInstance == typeof( Person ) )
                    {   // this is a person, reference the local keys that are already cached
                        var personKeys = GetPersonKeys( entityForeignKey );
                        if ( personKeys != null )
                        {
                            noteEntityId = personKeys.PersonId;
                        }

                        noteTypeId = noteType.StartsWith( "General", StringComparison.InvariantCultureIgnoreCase ) ? ( int? ) PersonalNoteTypeId : null;
                    }
                    else
                    {   // activate service type and query the foreign id for this entity type
                        var entityService = Reflection.GetServiceForEntityType( entityTypeInstance, lookupContext );
                        var entityQueryable = entityService.GetType().GetMethod( "Queryable", new Type[] { } );

                        // Note: reflection-invoked service can only return IEnumerable or primitive object types
                        noteEntityId = ( ( IQueryable<IEntity> ) entityQueryable.Invoke( entityService, new object[] { } ) )
                            .Where( q => entityForeignKey == q.ForeignKey )
                            .Select( q => q.Id )
                            .FirstOrDefault();
                    }

                    if ( noteEntityId > 0 && !string.IsNullOrWhiteSpace( noteText ) )
                    {
                        var creatorKeys = GetPersonKeys( createdByKey );
                        var creatorAliasId = creatorKeys != null ? ( int? ) creatorKeys.PersonAliasId : null;

                        var note = AddEntityNote( lookupContext, entityType.Id, ( int ) noteEntityId, noteCaption, noteText, isAlert, isPrivate, noteType, noteTypeId, false, createdDate,
                            string.Format( "Note imported {0}", ImportDateTime ), creatorAliasId );

                        noteList.Add( note );
                        completedItems++;
                        if ( completedItems % ( ReportingNumber * 10 ) < 1 )
                        {
                            ReportProgress( 0, string.Format( "{0:N0} notes processed.", completedItems ) );
                        }

                        if ( completedItems % ReportingNumber < 1 )
                        {
                            SaveNotes( noteList );
                            ReportPartialProgress();
                            noteList.Clear();
                        }
                    }
                }
                else
                {
                    skippedNotes.Add( entityForeignKey, noteType );
                }
            }

            if ( noteList.Any() )
            {
                SaveNotes( noteList );
            }

            if ( skippedNotes.Any() )
            {
                ReportProgress( 0, "The following notes could not be imported and were skipped:" );
                foreach ( var key in skippedNotes )
                {
                    ReportProgress( 0, string.Format( "{0} note for Foreign ID {1}.", key.Value, key ) );
                }
            }

            ReportProgress( 100, string.Format( "Finished note import: {0:N0} notes imported.", completedItems ) );
            return completedItems;
        }

        /// <summary>
        /// Saves the notes.
        /// </summary>
        /// <param name="noteList">The note list.</param>
        private static void SaveNotes( List<Note> noteList )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.Notes.AddRange( noteList );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }
    }

    #endregion
}
