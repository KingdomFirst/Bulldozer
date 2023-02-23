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
using Rock;
using Rock.Data;
using Rock.Model;
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
                if ( completed % ( AttendanceChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} attendance records processed, {1:N0} imported.", completed, importedCount ) );
                }

                if ( completed % AttendanceChunkSize < 1 )
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

        #endregion Main Methods
    }
}