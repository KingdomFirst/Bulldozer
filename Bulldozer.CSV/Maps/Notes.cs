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
using System.Linq;
using Bulldozer.Model;
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
            var importedNotes = new NoteService( lookupContext ).Queryable().Count( n => n.ForeignKey != null && n.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) );

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
                var entityForeignKey = row[EntityId];
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

                        noteTypeId = noteType.StartsWith( "General", StringComparison.OrdinalIgnoreCase ) ? ( int? ) PersonalNoteTypeId : null;
                    }
                    else
                    {   // activate service type and query the foreign id for this entity type
                        var entityService = Reflection.GetServiceForEntityType( entityTypeInstance, lookupContext );
                        var entityQueryable = entityService.GetType().GetMethod( "Queryable", new Type[] { } );

                        // Note: reflection-invoked service can only return IEnumerable or primitive object types
                        noteEntityId = ( ( IQueryable<IEntity> ) entityQueryable.Invoke( entityService, new object[] { } ) )
                            .Where( q => string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, entityForeignKey ) == q.ForeignKey )
                            .Select( q => q.Id )
                            .FirstOrDefault();
                    }

                    if ( noteEntityId > 0 && !string.IsNullOrWhiteSpace( noteText ) )
                    {
                        var creatorKeys = GetPersonKeys( createdByKey );
                        var creatorAliasId = creatorKeys != null ? ( int? ) creatorKeys.PersonAliasId : null;

                        var note = AddEntityNote( lookupContext, entityType.Id, ( int ) noteEntityId, noteCaption, noteText, isAlert, isPrivate, noteType, noteTypeId, false, createdDate,
                            string.Format( "Note imported {0}", ImportDateTime ), creatorAliasId, this.ImportInstanceFKPrefix );

                        noteList.Add( note );
                        completedItems++;
                        if ( completedItems % ( DefaultChunkSize * 10 ) < 1 )
                        {
                            ReportProgress( 0, string.Format( "{0:N0} notes processed.", completedItems ) );
                        }

                        if ( completedItems % DefaultChunkSize < 1 )
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

        /// <summary>
        /// Maps the person note data.
        /// </summary>
        /// <exception cref="System.NotImplementedException"></exception>
        private int ImportEntityNote<T>( IEnumerable<EntityNoteCsv> entityNoteList, bool? groupEntityIsFamily )
        {
            ReportProgress( 0, "Preparing PersonNote data for import..." );

            var entityType = EntityTypeCache.Get<T>();
            var rockContext = new RockContext();
            var notesToInsert = new List<FinancialBatch>();
            var noteImportList = new List<NoteImport>();
            var personEntityType = EntityTypeCache.Get( PersonEntityTypeId );

            var noteTypeService = new NoteTypeService( rockContext );

            var noteTypeLookup = noteTypeService.Queryable()
                .Where( a => a.EntityTypeId == entityType.Id ).Select( a => new
                {
                    a.Id,
                    a.Name
                } )
                .ToList()
                .DistinctBy( a => a.Name )
                .ToDictionary( k => k.Name, v => v.Id, StringComparer.OrdinalIgnoreCase );

            var importNoteTypeNames = this.PersonNoteCsvList.Select( a => a.NoteType ).Distinct().ToList();
            
            // Create new NoteTypes if needed
            foreach ( var noteTypeName in importNoteTypeNames )
            {
                if ( !noteTypeLookup.ContainsKey( noteTypeName ) )
                {
                    var newNoteType =  new NoteType
                    {
                        IsSystem = false,
                        EntityTypeId = PersonEntityTypeId,
                        EntityTypeQualifierColumn = string.Empty,
                        EntityTypeQualifierValue = string.Empty,
                        Name = noteTypeName,
                        UserSelectable = true,
                        IconCssClass = string.Empty
                    };

                    noteTypeService.Add( newNoteType );
                    rockContext.SaveChanges();

                    noteTypeLookup.Add( newNoteType.Name, newNoteType.Id );
                }
            }

            foreach ( var entityNoteCsv in entityNoteList )
            {
                var newNoteImport = new NoteImport
                    {
                        NoteForeignId = entityNoteCsv.Id.AsIntegerOrNull(),
                        NoteForeignKey = $"{this.ImportInstanceFKPrefix}^{entityNoteCsv.Id}",
                        NoteTypeId = noteTypeLookup[entityNoteCsv.NoteType],
                        Caption = entityNoteCsv.Caption,
                        IsAlert = entityNoteCsv.IsAlert.GetValueOrDefault(),
                        IsPrivateNote = entityNoteCsv.IsPrivateNote.GetValueOrDefault(),
                        Text = entityNoteCsv.Text,
                        DateTime = entityNoteCsv.DateTime?.ToSQLSafeDate(),
                        CreatedByPersonForeignKey = $"{this.ImportInstanceFKPrefix}^{entityNoteCsv.CreatedByPersonId}"
                    };

                if ( entityNoteCsv is PersonNoteCsv )
                {
                    newNoteImport.EntityForeignKey = $"{this.ImportInstanceFKPrefix}^{( entityNoteCsv as PersonNoteCsv ).PersonId}";
                }
                //else if ( entityNoteCsv is FamilyNoteCsv )
                //{
                //    noteImport.EntityForeignKey =  $"{this.ImportInstanceFKPrefix}^{( entityNoteCsv as FamilyNoteCsv ).FamilyId}";
                //}
                else
                {
                    LogException( $"{entityType.Name}Note", $"An unexpected Note Entity Type has been encountered. NoteId {entityNoteCsv.Id} was skipped." );
                }

                noteImportList.Add( newNoteImport );
            }

            // Slice data into chunks and process
            var workingNoteImportList = entityNoteList.ToList();
            var notesRemainingToProcess = entityNoteList.ToList().Count;
            var completed = 0;
            var insertedNotes = new List<Note>();

            while ( notesRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} {entityType.Name} Notes processed." );
                }

                if ( completed % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingNoteImportList.Take( Math.Min( this.DefaultChunkSize, workingNoteImportList.Count ) ).ToList();
                    completed += BulkNoteImport( noteImportList, entityType.Id, groupEntityIsFamily );
                    notesRemainingToProcess -= csvChunk.Count;
                    workingNoteImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }
            return completed;
        }

        /// <summary>
        /// Bulks the note import.
        /// </summary>
        /// <param name="noteImports">The note imports.</param>
        /// <param name="entityTypeId">The entity type identifier.</param>
        /// <param name="foreignSystemKey">The foreign system key.</param>
        /// <param name="groupEntityIsFamily">If this is a GroupEntity, is it a Family GroupType?</param>
        /// <returns></returns>
        public int BulkNoteImport( List<NoteImport> noteImports, int entityTypeId, bool? groupEntityIsFamily )
        {
            var entityTypeCache = EntityTypeCache.Get( entityTypeId );
            var entityFriendlyName = entityTypeCache.FriendlyName;
            if ( entityTypeId == EntityTypeCache.GetId<Group>().Value )
            {
                if ( groupEntityIsFamily.Value )
                {
                    entityFriendlyName = "Family";
                }
            }

            var rockContext = new RockContext();
            
            var importedNotes = new NoteService( rockContext ).Queryable().Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) && a.NoteType.EntityTypeId == entityTypeId );

            Dictionary<string, int> entityIdLookup;
            if ( entityTypeId == EntityTypeCache.GetId<Group>().Value )
            {
                int groupTypeIdFamily = GroupTypeCache.GetFamilyGroupType().Id;
                if ( groupEntityIsFamily.Value == true )
                {
                    entityIdLookup = new GroupService( rockContext ).Queryable().Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) && a.GroupTypeId == groupTypeIdFamily )
                        .Select( a => new { a.Id, a.ForeignKey } )
                        .ToList().ToDictionary( k => k.ForeignKey, v => v.Id );
                }
                else
                {
                    entityIdLookup = new GroupService( rockContext ).Queryable().Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) && a.GroupTypeId != groupTypeIdFamily )
                        .Select( a => new { a.Id, a.ForeignKey } )
                        .ToList().ToDictionary( k => k.ForeignKey, v => v.Id );
                }
            }
            else
            {
                var entityType = entityTypeCache.GetEntityType();
                var entityService = Reflection.GetServiceForEntityType( entityType, rockContext );
                var queryableMethodInfo = entityService.GetType().GetMethod( "Queryable", new Type[] { } );
                var entityQuery = queryableMethodInfo.Invoke( entityService, null ) as IQueryable<IEntity>;

                entityIdLookup = entityQuery.Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                    .Select( a => new { a.Id, a.ForeignKey } )
                    .ToList().ToDictionary( k => k.ForeignKey, v => v.Id );
            }

            var notesToInsert = new List<Note>();
            var newNoteImports = noteImports.Where( a => !importedNotes.Any( b => b.ForeignKey == a.NoteForeignKey ) ).ToList();

            var importDateTime = RockDateTime.Now;

            foreach ( var noteImport in newNoteImports )
            {
                var newNote = new Note
                {
                    ForeignId = noteImport.NoteForeignId,
                    ForeignKey = noteImport.NoteForeignKey,
                    EntityId = entityIdLookup.GetValueOrNull( noteImport.EntityForeignKey ),
                    NoteTypeId = noteImport.NoteTypeId,
                    Caption = noteImport.Caption ?? string.Empty,
                    IsAlert = noteImport.IsAlert,
                    IsPrivateNote = noteImport.IsPrivateNote,
                    Text = noteImport.Text,
                    CreatedDateTime = noteImport.DateTime.ToSQLSafeDate() ?? importDateTime,
                    ModifiedDateTime = noteImport.DateTime.ToSQLSafeDate() ?? importDateTime
                };

                if ( newNote.Caption.Length > 200 )
                {
                    newNote.Caption = newNote.Caption.Left( 200 );
                }

                if ( noteImport.CreatedByPersonForeignKey.IsNotNullOrWhiteSpace() )
                {
                    newNote.CreatedByPersonAliasId = ImportedPeopleKeys.GetValueOrNull( noteImport.CreatedByPersonForeignKey )?.PersonAliasId;
                }

                notesToInsert.Add( newNote );
            }

            rockContext.BulkInsert( notesToInsert );

            return notesToInsert.Count;
        }
    }

    #endregion
}
