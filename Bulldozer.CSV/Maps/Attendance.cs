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
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the Attendance import methods
    /// </summary>
    partial class CSVComponent
    {
        /// <summary>
        /// Loads the attendance data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int ProcessAttendance()
        {
            ReportProgress( 0, "Preparing attendance data for processing." );
            var rockContext = new RockContext();
            if ( this.GroupDict == null )
            {
                LoadGroupDict( rockContext );
            }
            if ( this.LocationsDict == null )
            {
                LoadLocationDict( rockContext );
            }
            if ( this.AttendanceOccurrenceDict == null )
            {
                LoadAttendanceOccurrenceDict( rockContext );
            }

            var attendanceData = this.AttendanceCsvList.ToList();

            var archivedScheduleName = "Archived Attendance";
            var archivedSchedule = new ScheduleService( rockContext ).Queryable()
                .FirstOrDefault( s => s.Name.Equals( archivedScheduleName ) );
            if ( archivedSchedule == null )
            {
                archivedSchedule = AddNamedSchedule( rockContext, archivedScheduleName, null, null, null,
                    ImportDateTime, archivedScheduleName.RemoveSpecialCharacters(), true, ImportPersonAliasId );
            }
            var archivedScheduleId = archivedSchedule.Id;

            var attendanceCount = attendanceData.Count;
            HashSet<string> attendanceIds = new HashSet<string>();

            attendanceData = attendanceData.Where( a => !string.IsNullOrWhiteSpace( a.PersonId ) ).ToList();
            if ( attendanceData.Count < attendanceCount )
            {
                ReportProgress( 0, string.Format( "{0:N0} attendance records have no PersonId provided and will be skipped.", attendanceCount - attendanceData.Count ) );
                attendanceCount = attendanceData.Count;
            }
            attendanceData = attendanceData.Where( a => a.StartDateTime.HasValue ).ToList();
            if ( attendanceData.Count < attendanceCount )
            {
                ReportProgress( 0, string.Format( "{0:N0} attendance records have no StartDateTime provided and will be skipped.", attendanceCount - attendanceData.Count ) );
                attendanceCount = attendanceData.Count;
            }

            // Get list of existing attendance records that have already been imported
            var attendanceService = new AttendanceService( rockContext );
            var existingAttendanceForeignKeys = attendanceService.Queryable()
                                                                 .Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                                                 .Select( a => a.ForeignKey )
                                                                 .ToList();

            // Get all of the existing group ids that have been imported (excluding families and known relationships)
            var groupIdLookup = this.GroupDict.Where( d => d.Value.GroupTypeId != CachedTypes.KnownRelationshipGroupType.Id )
                .ToDictionary( k => k.Key, v => v.Value.Id );

            // Create a lookup dictionary of the existing location foreign keys and location ids
            var locationIdLookup = this.LocationsDict.ToDictionary( k => k.Key, v => v.Value.Id );

            // Get all the existing schedule ids that have been imported
            var scheduleIdLookup = new ScheduleService( rockContext ).Queryable().Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                .ToDictionary( k => k.ForeignKey, v => v.Id );

            // Get the primary alias id lookup for each person foreign id
            var personAliasIdLookup = ImportedPeopleKeys.ToDictionary( k => k.Key, v => v.Value.PersonAliasId );

            // Get list of existing occurrence records that have already been created
            var existingOccurrenceGuidLookup = this.AttendanceOccurrenceDict.ToDictionary( k => k.Key, v => v.Value.Guid );

            this.ReportProgress( 0, string.Format( "Begin processing {0} Attendance Records...", attendanceData.Count ) );

            // Create AttendanceOccurrence if needed
            this.ReportProgress( 0, "Creating AttendanceOccurrences for Attendance" );

            // Slice data into chunks and process

            var attendancesToProcessRemaining = attendanceData.Count;
            var workingAttendanceCsvList = attendanceData.ToList();
            var attendanceImportList = new List<AttendanceImport>();
            var invalidPersonIds = new List<string>();
            var attendanceCsvCompleted = 0;
            while ( attendancesToProcessRemaining > 0 )
            {
                if ( attendanceCsvCompleted > 0 && attendanceCsvCompleted % ( this.AttendanceChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} AttendanceOccurrences processed.", attendanceCsvCompleted ) );
                }

                if ( attendanceCsvCompleted % this.AttendanceChunkSize < 1 )
                {
                    var attendanceImportListChunk = new List<AttendanceImport>();
                    var csvChunk = workingAttendanceCsvList.Take( Math.Min( this.AttendanceChunkSize, workingAttendanceCsvList.Count ) ).ToList();
                    foreach ( var csvAttendance in csvChunk )
                    {
                        var groupId = groupIdLookup.GetValueOrNull( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, csvAttendance.GroupId ) );

                        var personAliasId = personAliasIdLookup.GetValueOrNull( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, csvAttendance.PersonId ) );
                        if ( !personAliasId.HasValue )
                        {
                            invalidPersonIds.Add( csvAttendance.PersonId );
                            continue;
                        }

                        var locationId = locationIdLookup.GetValueOrNull( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, csvAttendance.LocationId ) );
                        var scheduleId = scheduleIdLookup.GetValueOrNull( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, csvAttendance.ScheduleId ) );

                        var attendanceImport = new AttendanceImport()
                        {
                            PersonAliasId = personAliasId.Value,
                            GroupId = groupId,
                            LocationId = locationId,
                            ScheduleId = scheduleId,
                            StartDateTime = csvAttendance.StartDateTime.Value,
                            EndDateTime = csvAttendance.EndDateTime,
                            Note = csvAttendance.Note
                        };

                        if ( !string.IsNullOrWhiteSpace( csvAttendance.AttendanceId ) )
                        {
                            attendanceImport.AttendanceForeignId = csvAttendance.AttendanceId.AsIntegerOrNull();
                            attendanceImport.AttendanceForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, csvAttendance.AttendanceId );
                        }
                        else
                        {
                            attendanceImport.AttendanceForeignKey = string.Format( "{0}^{1}_{2}_{3}_{4}_{5}", this.ImportInstanceFKPrefix, csvAttendance.PersonId, long.Parse( csvAttendance.StartDateTime.Value.ToString( "yyMMddHHmm" ) ), locationId, scheduleId, csvAttendance.GroupId );
                        }

                        // Get unique combination of group/location/schedule/date for occurrence attendance records being added
                        attendanceImport.OccurrenceForeignKey = string.Format( "{0}^{1}_{2}_{3}_{4}", this.ImportInstanceFKPrefix, csvAttendance.GroupId, locationId, scheduleId, long.Parse( csvAttendance.StartDateTime.Value.Date.ToString( "yyMMdd" ) ) );

                        if ( !attendanceIds.Add( attendanceImport.AttendanceForeignKey ) )
                        {
                            // shouldn't happen (but if it does, it'll be treated as a duplicate and not imported)
                            System.Diagnostics.Debug.WriteLine( $"#### Duplicate AttendanceId detected:{attendanceImport.AttendanceForeignKey} ####" );
                        }

                        if ( !string.IsNullOrWhiteSpace( csvAttendance.CampusId ) )
                        {
                            var campusIdInt = csvAttendance.CampusId.AsIntegerOrNull();
                            Campus campus = null;
                            if ( this.UseExistingCampusIds && campusIdInt.HasValue )
                            {
                                campus = this.CampusesDict.GetValueOrNull( campusIdInt.Value );
                            }
                            else
                            {
                                campus = this.CampusImportDict.GetValueOrNull( $"{ImportInstanceFKPrefix}^{csvAttendance.CampusId}" );
                            }
                            attendanceImport.CampusId = campus?.Id;
                        }
                        attendanceImportListChunk.Add( attendanceImport );
                    }

                    // Get the attendance records being imported that are new
                    var newAttendanceImports = attendanceImportListChunk.Where( a => !existingAttendanceForeignKeys.Contains( a.AttendanceForeignKey ) ).ToList();
                    attendanceImportList.AddRange( newAttendanceImports );
                    CreateAttendanceOccurrences( rockContext, newAttendanceImports, existingOccurrenceGuidLookup, archivedScheduleId );
                    attendanceCsvCompleted += csvChunk.Count;
                    attendancesToProcessRemaining -= csvChunk.Count;
                    workingAttendanceCsvList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            this.ReportProgress( 0, "Finished creating AttendanceOccurrences" );
            this.ReportProgress( 0, "Preparing to import Attendance" );

            // Reload AttendanceOccurence dictionary to include all newly created records.
            LoadAttendanceOccurrenceDict();

            // Now that we have created any needed AttendancOccurrence records, we can begin importing the attendance
            this.ReportProgress( 0, "Importing Attendance Records" );

            // Slice data into chunks and process

            var attendanceImportsToInsertRemaining = attendanceImportList.Count;
            var workingAttendanceImportList = attendanceImportList.ToList();
            var attendanceImportCompleted = 0;
            while ( attendanceImportsToInsertRemaining > 0 )
            {
                if ( attendanceImportCompleted > 0 && attendanceImportCompleted % ( this.AttendanceChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} Attendance records imported.", attendanceImportCompleted ) );
                }

                if ( attendanceImportCompleted % this.AttendanceChunkSize < 1 )
                {
                    var importChunk = workingAttendanceImportList.Take( Math.Min( this.AttendanceChunkSize, workingAttendanceImportList.Count ) ).ToList();
                    SaveAttendance( rockContext, importChunk, archivedScheduleId );
                    attendanceImportCompleted += importChunk.Count;
                    attendanceImportsToInsertRemaining -= importChunk.Count;
                    workingAttendanceImportList.RemoveRange( 0, importChunk.Count );
                    ReportPartialProgress();
                }
            }

            if ( invalidPersonIds.Count > 0 )
            {
                var errorMsg = "The following PersonId(s) were not found, and therefore any related attendance was skipped.\r\n";
                errorMsg += string.Join( ", ", invalidPersonIds );
                LogException( "Attendance", errorMsg );
            }
            return attendanceImportCompleted;
        }

        /// <summary>
        /// Bulks the attendance import.
        /// </summary>
        /// <param name="rockContext">The Rock context.</param>
        /// <param name="attendanceImports">The attendance imports.</param>
        /// <param name="archivedAttendanceScheduleId">The Id for the archived attendance schedule.</param>
        /// <returns></returns>
        public int SaveAttendance( RockContext rockContext, List<AttendanceImport> attendanceImports, int archivedAttendanceScheduleId )
        {
            var importDateTime = RockDateTime.Now;

            var attendancesToInsert = new List<Attendance>();

            foreach ( var attendanceImport in attendanceImports )
            {
                var occurrence = this.AttendanceOccurrenceDict.GetValueOrNull( attendanceImport.OccurrenceForeignKey );
                if ( occurrence != null )
                {
                    var newAttendance = new Attendance
                    {
                        PersonAliasId = attendanceImport.PersonAliasId,
                        OccurrenceId = occurrence.Id,
                        Note = attendanceImport.Note,
                        DidAttend = true,
                        ForeignId = attendanceImport.AttendanceForeignId,
                        ForeignKey = attendanceImport.AttendanceForeignKey,
                        CampusId = attendanceImport.CampusId,
                        StartDateTime = attendanceImport.StartDateTime,
                        EndDateTime = attendanceImport.EndDateTime,
                        CreatedDateTime = importDateTime,
                        ModifiedDateTime = importDateTime
                    };
                    attendancesToInsert.Add( newAttendance );
                }
            }
            rockContext.BulkInsert( attendancesToInsert );
            return attendancesToInsert.Count;
        }

        /// <summary>
        /// Create AttendanceOccurrences for AttendanceImports.
        /// </summary>
        /// <param name="rockContext">The Rock context.</param>
        /// <param name="attendanceImports">The attendance imports.</param>
        /// <param name="existingOccurrenceGuidLookup">Dictionary of foreign keys and guids for existing imported occurrences.</param>
        /// <param name="archivedAttendanceScheduleId">The ScheduleId for the default archived attendance schedule.</param>
        /// <returns></returns>
        public int CreateAttendanceOccurrences( RockContext rockContext, List<AttendanceImport> attendanceImports, Dictionary<string, Guid> existingOccurrenceGuidLookup, int archivedAttendanceScheduleId )
        {
            var importDateTime = RockDateTime.Now;

            // Create list of occurrences to be bulk inserted
            var newOccurrences = new List<ImportOccurrence>();

            var newAttendanceOccurrenceImports = attendanceImports
                .Where( a => !existingOccurrenceGuidLookup.ContainsKey( a.OccurrenceForeignKey ) )
                .GroupBy( a => a.OccurrenceForeignKey )
                .Select( a => new
                {
                    OccurrenceForeignKey = a.Key,
                    AttendanceImport = a.FirstOrDefault()
                } )
                .ToList();

            var occurrencesToInsert = new List<AttendanceOccurrence>();
            foreach ( var attImport in newAttendanceOccurrenceImports )
            {
                var occurrence = new AttendanceOccurrence
                {
                    GroupId = attImport.AttendanceImport.GroupId,
                    LocationId = attImport.AttendanceImport.LocationId,
                    ScheduleId = attImport.AttendanceImport.ScheduleId,
                    OccurrenceDate = attImport.AttendanceImport.StartDateTime.Date,
                    ForeignId = attImport.AttendanceImport.OccurrenceForeignId,
                    ForeignKey = attImport.OccurrenceForeignKey
                };

                if ( !occurrence.ScheduleId.HasValue )
                {
                    occurrence.ScheduleId = archivedAttendanceScheduleId;
                }

                occurrencesToInsert.Add( occurrence );
            }

            // Add all the new occurrences
            rockContext.BulkInsert( occurrencesToInsert );
            occurrencesToInsert.ForEach( a => existingOccurrenceGuidLookup.Add( a.ForeignKey, a.Guid ) );
            return attendanceImports.Count;
        }

    }
}