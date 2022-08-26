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
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Bulldozer.Model;
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the Attendance import methods
    /// </summary>
    partial class CSVComponent
    {
        #region Main Methods

        /// <summary>
        /// Loads the attendance data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadAttendance( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var groupService = new GroupService( lookupContext );
            var locationService = new LocationService( lookupContext );
            var attendanceService = new AttendanceService( lookupContext );

            var currentGroup = new Group();
            int? currentGroupId = null;
            var location = new Location();
            int? locationId = null;
            int? campusId = null;
            var newAttendanceList = new List<Attendance>();
            var newOccurrences = new List<AttendanceOccurrence>();
            var existingOccurrences = new AttendanceOccurrenceService( lookupContext ).Queryable().AsNoTracking()
                .Select( o => new
                {
                    o.Id,
                    o.GroupId,
                    o.LocationId,
                    o.ScheduleId,
                    o.OccurrenceDate
                } ).ToDictionary( k => $"{k.GroupId}|{k.LocationId}|{k.ScheduleId}|{k.OccurrenceDate}", v => v.Id );

            var archivedScheduleName = "Archived Attendance";
            var archivedSchedule = new ScheduleService( lookupContext ).Queryable()
                .FirstOrDefault( s => s.Name.Equals( archivedScheduleName ) );
            if ( archivedSchedule == null )
            {
                archivedSchedule = AddNamedSchedule( lookupContext, archivedScheduleName, null, null, null,
                    ImportDateTime, archivedScheduleName.RemoveSpecialCharacters(), true, ImportPersonAliasId );
            }

            var completed = 0;
            var importedCount = 0;
            var alreadyImportedCount = attendanceService.Queryable().AsNoTracking().Count( a => a.ForeignKey != null );
            ReportProgress( 0, string.Format( "Starting attendance import ({0:N0} already exist).", alreadyImportedCount ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var rowAttendanceKey = row[AttendanceId];
                var rowGroupKey = row[AttendanceGroupId];
                var rowPersonKey = row[AttendancePersonId];
                var rowDate = row[AttendanceDate];
                var rowCreatedDate = row[AttendanceCreatedDate];
                var rowAttended = row[AttendanceAttended];
                var rowLocationKey = row[AttendanceLocationId];
                var rowAttendanceId = rowAttendanceKey.AsType<int?>();

                //
                // Find this person in the database.
                //
                var personKeys = GetPersonKeys( rowPersonKey );
                if ( personKeys == null || personKeys.PersonId == 0 )
                {
                    ReportProgress( 0, string.Format( "Person key {0} not found", rowPersonKey ) );
                }

                //
                // Check that this attendance record doesn't already exist.
                //
                var attendanceExists = false;
                if ( ImportedGroups.Count > 0 && rowGroupKey != currentGroup?.ForeignKey )
                {
                    currentGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey == rowGroupKey );
                    currentGroupId = currentGroup?.Id;
                }

                //
                // If we have a valid matching location, set the location and campus.
                //
                if ( !string.IsNullOrEmpty( rowLocationKey ) )
                {
                    location = locationService.Queryable().FirstOrDefault( l => l.ForeignKey == rowLocationKey );
                    if ( location != null )
                    {
                        locationId = location.Id;
                        campusId = location.CampusId;
                    }
                }

                if ( alreadyImportedCount > 0 )
                {
                    attendanceExists = attendanceService.Queryable().AsNoTracking().Any( a => a.ForeignKey == rowAttendanceKey );
                }

                if ( !attendanceExists && ( personKeys != null && personKeys.PersonId != 0 ) )
                {
                    //
                    // Create and populate the new attendance record.
                    //
                    var attendance = new Attendance
                    {
                        PersonAliasId = personKeys.PersonAliasId,
                        ForeignKey = rowAttendanceKey,
                        ForeignId = rowAttendanceId,
                        DidAttend = ParseBoolOrDefault( rowAttended, true ),
                        StartDateTime = ( DateTime ) ParseDateOrDefault( rowDate, ImportDateTime ),
                        CreatedDateTime = ParseDateOrDefault( rowCreatedDate, ImportDateTime ),
                        ModifiedDateTime = ImportDateTime,
                        CreatedByPersonAliasId = ImportPersonAliasId,
                        ModifiedByPersonAliasId = ImportPersonAliasId,
                        CampusId = campusId
                    };

                    var startDateString = ( ( DateTime ) ParseDateOrDefault( rowDate, ImportDateTime ) ).Date;

                    // occurrence is required for attendance
                    int? occurrenceId = existingOccurrences.GetValueOrNull( $"{currentGroupId}|{locationId}|{archivedSchedule.Id}|{startDateString}" );
                    if ( occurrenceId.HasValue )
                    {
                        attendance.OccurrenceId = occurrenceId.Value;
                    }
                    else
                    {
                        var scheduleId = currentGroup != null && currentGroup.ScheduleId.HasValue ? currentGroup.ScheduleId.Value : archivedSchedule.Id;
                        var newOccurrence = AddOccurrence( null, ( DateTime ) ParseDateOrDefault( rowDate, ImportDateTime ), currentGroupId, scheduleId, locationId, true );
                        if ( newOccurrence != null )
                        {
                            attendance.OccurrenceId = newOccurrence.Id;
                            existingOccurrences.Add( $"{currentGroupId}|{locationId}|{archivedSchedule.Id}|{startDateString}", newOccurrence.Id );
                        }
                    }

                    //
                    // Add the attendance record for delayed saving.
                    //
                    newAttendanceList.Add( attendance );
                    importedCount++;
                }

                //
                // Notify user of our status.
                //
                completed++;
                if ( completed % ( ReportingNumber * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} attendance records processed, {1:N0} imported.", completed, importedCount ) );
                }

                if ( completed % ReportingNumber < 1 )
                {
                    SaveAttendance( newAttendanceList );
                    lookupContext.SaveChanges();
                    ReportPartialProgress();

                    // Clear out variables
                    currentGroup = new Group();
                    newAttendanceList.Clear();
                    groupService = new GroupService( lookupContext );
                    locationService = new LocationService( lookupContext );
                    attendanceService = new AttendanceService( lookupContext );
                }
            }

            //
            // Save any final changes to new groups
            //
            if ( newAttendanceList.Any() )
            {
                SaveAttendance( newAttendanceList );
            }

            //
            // Save any changes to existing groups
            //
            lookupContext.SaveChanges();
            lookupContext.Dispose();

            ReportProgress( 0, string.Format( "Finished attendance import: {0:N0} records added.", importedCount ) );

            return completed;
        }

        /// <summary>
        /// Loads the attendance data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int ProcessAttendance( List<AttendanceCsv> attendanceData )
        {
            var completed = 0;
            var attendanceCount = attendanceData.Count;
            var attendanceImportList = new List<AttendanceImport>();
            HashSet<string> attendanceIds = new HashSet<string>();
            ReportProgress( 0, string.Format( "Preparing attendance data for import ({0:N0} to process).", attendanceCount ) );
            
            attendanceData = attendanceData.Where( a => !string.IsNullOrWhiteSpace( a.GroupId ) ).ToList();
            if ( attendanceData.Count < attendanceCount )
            {
                ReportProgress( 0, string.Format( "{0:N0} attendance records have no GroupId provided and will be skipped.", attendanceCount - attendanceData.Count ) );
                attendanceCount = attendanceData.Count;
            }
            attendanceData = attendanceData.Where( a => !string.IsNullOrWhiteSpace( a.PersonId ) ).ToList();
            if ( attendanceData.Count < attendanceCount )
            {
                ReportProgress( 0, string.Format( "{0:N0} attendance records have no PersonId provided and will be skipped.", attendanceCount - attendanceData.Count ) );
                attendanceCount = attendanceData.Count;
            }
            attendanceData = attendanceData.Where( a => !a.StartDateTime.HasValue ).ToList();
            if ( attendanceData.Count < attendanceCount )
            {
                ReportProgress( 0, string.Format( "{0:N0} attendance records have no StartDateTime provided and will be skipped.", attendanceCount - attendanceData.Count ) );
                attendanceCount = attendanceData.Count;
            }

            foreach ( var csvAttendance in attendanceData )
            {
                var attendanceImport = new AttendanceImport()
                {
                    PersonForeignId = csvAttendance.PersonId,
                    GroupForeignId = csvAttendance.GroupId,
                    LocationForeignId = csvAttendance.LocationId,
                    ScheduleForeignId = csvAttendance.ScheduleId,
                    StartDateTime = csvAttendance.StartDateTime.Value,
                    EndDateTime = csvAttendance.EndDateTime,
                    Note = csvAttendance.Note
                };

                if ( !string.IsNullOrWhiteSpace( csvAttendance.AttendanceId ) )
                {
                    attendanceImport.AttendanceForeignId = csvAttendance.AttendanceId;
                }
                else
                {
                    MD5 md5Hasher = MD5.Create();
                    var hashed = md5Hasher.ComputeHash( Encoding.UTF8.GetBytes( $@"
    {csvAttendance.PersonId}
    {csvAttendance.StartDateTime}
    {csvAttendance.LocationId}
    {csvAttendance.ScheduleId}
    {csvAttendance.GroupId}
" ) );
                    attendanceImport.AttendanceForeignId = Math.Abs( BitConverter.ToInt32( hashed, 0 ) ).ToString(); // used abs to ensure positive number */
                }

                if ( !attendanceIds.Add( attendanceImport.AttendanceForeignId ) )
                {
                    // shouldn't happen (but if it does, it'll be treated as a duplicate and not imported)
                    System.Diagnostics.Debug.WriteLine( $"#### Duplicate AttendanceId detected:{attendanceImport.AttendanceForeignId} ####" );
                }

                if ( !string.IsNullOrWhiteSpace( csvAttendance.CampusId ) )
                {
                    var campusIdInt = csvAttendance.CampusId.AsIntegerOrNull();
                    var campus = CampusList.FirstOrDefault( c => ( campusIdInt.HasValue && c.Id == campusIdInt.Value )
                        || ( c.ForeignKey.Equals( csvAttendance.CampusId, StringComparison.OrdinalIgnoreCase ) ) );
                    if ( campus != null )
                    {
                        attendanceImport.CampusId = campus.Id;
                    }
                }
                attendanceImportList.Add( attendanceImport );
            }
            completed += SaveAttendance( attendanceImportList );
            return completed;
        }

        /// <summary>
        /// Saves all group changes.
        /// </summary>
        /// <param name="attendanceList">The attendance list.</param>
        private static void SaveAttendance( List<Attendance> attendanceList )
        {
            //
            // Save any attendance records
            //
            if ( attendanceList.Count > 0 )
            {
                using ( var rockContext = new RockContext() )
                {
                    rockContext.Attendances.AddRange( attendanceList );
                    rockContext.SaveChanges( DisableAuditing );
                }
            }
        }


        /// <summary>
        /// Bulks the attendance import.
        /// </summary>
        /// <param name="attendanceImports">The attendance imports.</param>
        /// <returns></returns>
        public int SaveAttendance( List<AttendanceImport> attendanceImports )
        {
            var stopwatchTotal = Stopwatch.StartNew();
            var stopwatch = Stopwatch.StartNew();

            var rockContext = new RockContext();
            var attendanceService = new AttendanceService( rockContext );

            // Get list of existing attendance records that have already been imported
            var qryAttendancesWithForeignIds = attendanceService.Queryable().Where( a => a.ForeignKey != null );
            var attendancesAlreadyExistForeignIdHash = new HashSet<string>( qryAttendancesWithForeignIds.Select( a => a.ForeignKey ).ToList() );

            int groupTypeIdFamily = GroupTypeCache.GetFamilyGroupType().Id;
            var importDateTime = RockDateTime.Now;

            // Get all of the existing group ids that have been imported (excluding families and known relationships)
            var groupIdLookup = ImportedGroups.Where( g => g.GroupTypeId != CachedTypes.KnownRelationshipGroupType.Id )
                .Select( a => new { a.Id, a.ForeignKey } ).ToDictionary( k => k.Id, v => v.ForeignKey);

            // Get all the existing location ids that have been imported
            var locationIdLookup = ImportedLocations.Select( a => new { a.Id, a.ForeignKey } ).ToDictionary( k => k.Id, v => v.ForeignKey );

            // Get all the existing schedule ids that have been imported
            var scheduleIdLookup = new ScheduleService( rockContext ).Queryable().Where( a => a.ForeignKey != null )
                .Select( a => new { a.Id, a.ForeignKey } ).ToDictionary( k => k.Id, v => v.ForeignKey );

            // Get the primary alias id lookup for each person foreign id
            var personAliasIdLookup = new PersonAliasService( rockContext ).Queryable().Where( a => a.Person.ForeignKey != null && a.PersonId == a.AliasPersonId )
                .Select( a => new { PersonAliasId = a.Id, PersonForeignKey = a.Person.ForeignKey } ).ToDictionary( k => k.PersonAliasId, v => v.PersonForeignKey );

            // Get list of existing occurrence records that have already been created
            var existingOccurrencesLookup = new AttendanceOccurrenceService( rockContext ).Queryable()
                .Select( o => new
                {
                    Id = o.Id,
                    GroupId = o.GroupId,
                    LocationId = o.LocationId,
                    ScheduleId = o.ScheduleId,
                    OccurrenceDate = o.OccurrenceDate
                } ).ToDictionary( k => $"{k.GroupId}|{k.LocationId}|{k.ScheduleId}|{k.OccurrenceDate}", v => v.Id );

            // Get the attendance records being imported that are new
            var newAttendanceImports = attendanceImports.Where( a => a.AttendanceForeignId == null || !attendancesAlreadyExistForeignIdHash.Contains( a.AttendanceForeignId ) ).ToList();

            // Create list of occurrences to be bulk inserted
            var newOccurrences = new List<ImportOccurrence>();

            // Get unique combination of group/location/schedule/date for attendance records being added
            var newAttendanceOccurrenceKeys = newAttendanceImports
                .GroupBy( a => new
                {
                    a.GroupForeignId,
                    a.LocationForeignId,
                    a.ScheduleForeignId,
                    OccurrenceDate = a.StartDateTime.Date
                } )
                .Select( a => a.Key )
                .ToList();
            foreach ( var groupKey in newAttendanceOccurrenceKeys )
            {
                var occurrence = new ImportOccurrence();

                if ( !string.IsNullOrWhiteSpace( groupKey.GroupForeignId ) )
                {
                    occurrence.GroupId = groupIdLookup.FirstOrDefault( d => d.Value == groupKey.GroupForeignId ).Key;
                }

                if ( !string.IsNullOrWhiteSpace( groupKey.LocationForeignId ) )
                {
                    occurrence.LocationId = locationIdLookup.FirstOrDefault( d => d.Value == groupKey.LocationForeignId ).Key;
                }

                if ( !string.IsNullOrWhiteSpace( groupKey.ScheduleForeignId ) )
                {
                    occurrence.ScheduleId = scheduleIdLookup.FirstOrDefault( d => d.Value == groupKey.ScheduleForeignId ).Key;
                }

                occurrence.OccurrenceDate = groupKey.OccurrenceDate;

                // If we haven't already added it to list, and it doesn't already exist, add it to list
                if ( !existingOccurrencesLookup.ContainsKey( $"{occurrence.GroupId}|{occurrence.LocationId}|{occurrence.ScheduleId}|{occurrence.OccurrenceDate}" ) )
                {
                    newOccurrences.Add( occurrence );
                }
            }

            var occurrencesToInsert = newOccurrences
                .GroupBy( n => new
                {
                    n.GroupId,
                    n.LocationId,
                    n.ScheduleId,
                    n.OccurrenceDate
                } )
                .Select( o => new AttendanceOccurrence
                {
                    GroupId = o.Key.GroupId,
                    LocationId = o.Key.LocationId,
                    ScheduleId = o.Key.ScheduleId,
                    OccurrenceDate = o.Key.OccurrenceDate
                } )
                .ToList();

            // Add all the new occurrences
            rockContext.BulkInsert( occurrencesToInsert );

            // Load all the existing occurrences again.
            existingOccurrencesLookup = new AttendanceOccurrenceService( rockContext ).Queryable()
                .Select( o => new
                {
                    Id = o.Id,
                    GroupId = o.GroupId,
                    LocationId = o.LocationId,
                    ScheduleId = o.ScheduleId,
                    OccurrenceDate = o.OccurrenceDate
                } ).ToDictionary( k => $"{k.GroupId}|{k.LocationId}|{k.ScheduleId}|{k.OccurrenceDate}", v => v.Id );

            var attendancesToInsert = new List<Attendance>( newAttendanceImports.Count );

            foreach ( var attendanceImport in newAttendanceImports )
            {
                int? occurrenceId = null;

                var newAttendance = new Attendance();
                newAttendance.ForeignId = attendanceImport.AttendanceForeignId.AsIntegerOrNull();
                newAttendance.ForeignKey = attendanceImport.AttendanceForeignId;

                newAttendance.CampusId = attendanceImport.CampusId;
                newAttendance.StartDateTime = attendanceImport.StartDateTime;
                newAttendance.EndDateTime = attendanceImport.EndDateTime;

                int? groupId = null;
                int? locationId = null;
                int? scheduleId = null;
                var occurrenceDate = attendanceImport.StartDateTime.Date;

                if ( !string.IsNullOrWhiteSpace( attendanceImport.GroupForeignId ) )
                {
                    groupId = groupIdLookup.FirstOrDefault( d => d.Value == attendanceImport.GroupForeignId ).Key;
                }

                if ( !string.IsNullOrWhiteSpace( attendanceImport.LocationForeignId ) )
                {
                    locationId = locationIdLookup.FirstOrDefault( d => d.Value == attendanceImport.LocationForeignId ).Key;
                }

                if ( !string.IsNullOrWhiteSpace( attendanceImport.ScheduleForeignId ) )
                {
                    scheduleId = scheduleIdLookup.FirstOrDefault( d => d.Value == attendanceImport.ScheduleForeignId ).Key;
                }

                occurrenceId = existingOccurrencesLookup.GetValueOrNull( $"{groupId}|{locationId}|{scheduleId}|{occurrenceDate}" );
                if ( occurrenceId.HasValue )
                {
                    newAttendance.OccurrenceId = occurrenceId.Value;
                    newAttendance.PersonAliasId = personAliasIdLookup.FirstOrDefault( d => d.Value == attendanceImport.PersonForeignId ).Key;
                    newAttendance.Note = attendanceImport.Note;
                    newAttendance.DidAttend = true;
                    newAttendance.CreatedDateTime = importDateTime;
                    newAttendance.ModifiedDateTime = importDateTime;
                    attendancesToInsert.Add( newAttendance );
                }
            }

            // Slice data into chunks and process

            var completed = 0;
            var attendancesToInsertRemaining = attendancesToInsert.Count;
            var importChunk = new List<Attendance>();
            while ( attendancesToInsertRemaining > 0 )
            {
                if ( completed % ( ReportingNumber * 100 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} attendance records imported.", completed ) );
                }

                if ( completed % ( ReportingNumber * 10 ) < 1 )
                {
                    importChunk = attendancesToInsert.Take( Math.Min( ReportingNumber, attendancesToInsert.Count ) ).ToList();
                    rockContext.BulkInsert( importChunk );
                    completed += importChunk.Count;
                    attendancesToInsertRemaining -= importChunk.Count;
                    attendancesToInsert.RemoveRange( 0, Math.Min( ReportingNumber, attendancesToInsert.Count ) );
                    ReportPartialProgress();
                }
            }

            ReportProgress( 0, string.Format( "{0:N0} attendance records imported.", completed ) );

            return attendancesToInsert.Count;
        }
        #endregion Main Methods
    }
}