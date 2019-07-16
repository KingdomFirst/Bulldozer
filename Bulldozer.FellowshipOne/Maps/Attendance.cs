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
using System.Data.Entity;
using System.Linq;
using OrcaMDF.Core.MetaData;
using Rock;
using Rock.Data;
using Rock.Model;
using static Bulldozer.Utility.CachedTypes;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.F1
{
    /// <summary>
    /// Partial of F1Component that imports Attendance
    /// </summary>
    public partial class F1Component
    {
        /// <summary>
        /// Maps the attendance data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="totalRows">The total rows.</param>
        private void MapAttendance( IQueryable<Row> tableData, long totalRows = 0 )
        {
            var lookupContext = new RockContext();
            var newAttendances = new List<Attendance>();
            var importedAttendancesCount = lookupContext.Attendances.AsNoTracking()
                .Count( a => a.ForeignKey != null );

            var importedCodes = lookupContext.AttendanceCodes.AsNoTracking()
                .Where( c => c.ForeignKey != null ).ToList();

            var importedDevices = lookupContext.Devices.AsNoTracking()
                .Where( d => d.DeviceTypeValueId == DeviceTypeCheckinKioskId ).ToList();

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

            if ( totalRows == 0 )
            {
                totalRows = tableData.Count();
            }

            var completedItems = 0;
            var percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, $"Verifying attendance import ({importedAttendancesCount:N0} already exist)." );

            foreach ( var row in tableData.Where( r => r != null ) )
            {
                var rlcId = row["RLC_ID"] as int?;
                var individualId = row["Individual_ID"] as int?;
                var startDate = row["Start_Date_Time"] as DateTime?;
                var attendanceCode = row["Tag_Code"] as string;
                var attendanceNote = row["BreakoutGroup_Name"] as string;
                var checkinDate = row["Check_In_Time"] as DateTime?;
                var checkoutDate = row["Check_Out_Time"] as DateTime?;
                var machineName = row["Checkin_Machine_Name"] as string;

                // at minimum, attendance needs a person and a date
                var personKeys = GetPersonKeys( individualId, null );
                if ( personKeys != null && personKeys.PersonAliasId > 0 && startDate.HasValue )
                {
                    // create the initial attendance
                    var attendance = new Attendance
                    {
                        PersonAliasId = personKeys.PersonAliasId,
                        DidAttend = true,
                        Note = attendanceNote,
                        StartDateTime = ( DateTime ) startDate,
                        EndDateTime = checkoutDate,
                        CreatedDateTime = checkinDate,
                        ForeignKey = $"Attendance imported {ImportDateTime}"
                    };

                    // add the RLC info if it exists
                    int? rockGroupId = null;
                    int? locationId = null;
                    var startDateString = ( ( DateTime ) startDate ).Date;
                    if ( rlcId.HasValue )
                    {
                        var rlcGroup = ImportedGroups.FirstOrDefault( g => g.ForeignId.Equals( rlcId ) );
                        rockGroupId = rlcGroup?.Id;
                        locationId = rlcGroup?.GroupLocations.Select( gl => ( int? ) gl.LocationId ).FirstOrDefault();
                        attendance.CampusId = rlcGroup?.CampusId;
                    }

                    // occurrence is required for attendance
                    int? occurrenceId = existingOccurrences.GetValueOrNull( $"{rockGroupId}|{locationId}|{archivedSchedule.Id}|{startDateString}" );
                    if ( occurrenceId.HasValue )
                    {
                        attendance.OccurrenceId = occurrenceId.Value;
                    }
                    else
                    {
                        var newOccurrence = AddOccurrence( null, ( DateTime ) startDate, rockGroupId, archivedSchedule.Id, locationId, true );
                        if ( newOccurrence != null )
                        {
                            attendance.OccurrenceId = newOccurrence.Id;
                            existingOccurrences.Add( $"{rockGroupId}|{locationId}|{archivedSchedule.Id}|{startDateString}", newOccurrence.Id );
                        }
                    }

                    // add the tag code
                    //if ( !string.IsNullOrWhiteSpace( attendanceCode ) )
                    //{
                    //var issueDatetime = checkinDate ?? (DateTime)startDate;
                    //var code = importedCodes.FirstOrDefault( c => c.Code.Equals( attendanceCode ) && c.IssueDateTime.Equals( issueDatetime ) );
                    //if ( code == null )
                    //{
                    //    code = new AttendanceCode
                    //    {
                    //        Code = attendanceCode,
                    //        IssueDateTime = issueDatetime,
                    //        ForeignKey = string.Format( "Attendance imported {0}", ImportDateTime )
                    //    };

                    //    lookupContext.AttendanceCodes.Add( code );
                    //    lookupContext.SaveChanges();
                    //    importedCodes.Add( code );
                    //}

                    //attendance.AttendanceCodeId = code.Id;
                    //}

                    // add the device
                    //if ( !string.IsNullOrWhiteSpace( machineName ) )
                    //{
                    //    var device = importedDevices.FirstOrDefault( d => d.Name.Equals( machineName, StringComparison.OrdinalIgnoreCase ) );
                    //    if ( device == null )
                    //    {
                    //        device = AddDevice( lookupContext, machineName, null, DeviceTypeCheckinKioskId, null, null, ImportDateTime,
                    //            $"{machineName} imported {ImportDateTime}", true, ImportPersonAliasId );
                    //        importedDevices.Add( device );
                    //    }

                    //    attendance.DeviceId = device.Id;
                    //}

                    newAttendances.Add( attendance );

                    completedItems++;
                    if ( completedItems % percentage < 1 )
                    {
                        var percentComplete = completedItems / percentage;
                        ReportProgress( percentComplete, $"{completedItems:N0} attendances imported ({percentComplete}% complete)." );
                    }

                    if ( completedItems % ReportingNumber < 1 )
                    {
                        SaveAttendances( newAttendances, false );
                        ReportPartialProgress();

                        // Reset lists and context
                        lookupContext.Dispose();
                        lookupContext = new RockContext();
                        newAttendances.Clear();
                    }
                }
            }

            if ( newAttendances.Any() )
            {
                SaveAttendances( newAttendances, false );
            }

            lookupContext.Dispose();
            ReportProgress( 100, $"Finished attendance import: {completedItems:N0} attendances imported." );
        }

        /// <summary>
        /// Maps the groups attendance data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="totalRows">The total rows.</param>
        private void MapGroupsAttendance( IQueryable<Row> tableData, long totalRows = 0 )
        {
            var lookupContext = new RockContext();
            var newAttendances = new List<Attendance>();
            var importedAttendancesCount = lookupContext.Attendances.AsNoTracking()
                .Count( a => a.ForeignKey != null && a.Occurrence.GroupId.HasValue && a.Occurrence.Group.GroupTypeId == GeneralGroupTypeId );

            var archivedScheduleName = "Archived Attendance";
            var archivedSchedule = new ScheduleService( lookupContext ).Queryable()
                .FirstOrDefault( s => s.Name.Equals( archivedScheduleName ) );
            if ( archivedSchedule == null )
            {
                archivedSchedule = AddNamedSchedule( lookupContext, archivedScheduleName, null, null, null,
                    ImportDateTime, archivedScheduleName.RemoveSpecialCharacters(), true, ImportPersonAliasId );
            }

            var existingOccurrences = new AttendanceOccurrenceService( lookupContext ).Queryable().AsNoTracking()
                .Select( o => new
                {
                    o.Id,
                    o.GroupId,
                    o.LocationId,
                    o.ScheduleId,
                    o.OccurrenceDate
                } ).ToDictionary( k => $"{k.GroupId}|{k.LocationId}|{k.ScheduleId}|{k.OccurrenceDate}", v => v.Id );

            if ( totalRows == 0 )
            {
                totalRows = tableData.Count();
            }

            var completedItems = 0;
            var percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, $"Verifying group attendance import, ({totalRows:N0} found, {importedAttendancesCount:N0} already exist)." );

            foreach ( var row in tableData.Where( r => r != null ) )
            {
                var groupId = row["GroupID"] as int?;
                var startDate = row["StartDateTime"] as DateTime?;
                var endDate = row["EndDateTime"] as DateTime?;
                var attendanceNote = row["Comments"] as string;
                var wasPresent = row["Individual_Present"] as int?;
                var individualId = row["IndividualID"] as int?;
                var checkinDate = row["CheckinDateTime"] as DateTime?;
                var checkoutDate = row["CheckoutDateTime"] as DateTime?;
                var createdDate = row["AttendanceCreatedDate"] as DateTime?;

                var personKeys = GetPersonKeys( individualId, null );
                if ( personKeys != null && personKeys.PersonAliasId > 0 && startDate.HasValue )
                {
                    // create the initial attendance
                    var attendance = new Attendance
                    {
                        PersonAliasId = personKeys.PersonAliasId,
                        DidAttend = wasPresent != 0,
                        Note = attendanceNote,
                        StartDateTime = ( DateTime ) startDate,
                        EndDateTime = checkoutDate,
                        CreatedDateTime = checkinDate,
                        ForeignKey = $"Group Attendance imported {ImportDateTime}"
                    };

                    // add the group info if it exists
                    int? rockGroupId = null;
                    int? locationId = null;
                    var startDateString = ( ( DateTime ) startDate ).Date;
                    if ( groupId.HasValue )
                    {
                        var peopleGroup = ImportedGroups.FirstOrDefault( g => g.ForeignId.Equals( groupId ) );
                        rockGroupId = peopleGroup?.Id;
                        locationId = peopleGroup?.GroupLocations.Select( gl => ( int? ) gl.LocationId ).FirstOrDefault();
                        attendance.CampusId = peopleGroup?.CampusId;
                    }

                    // occurrence is required for attendance
                    int? occurrenceId = existingOccurrences.GetValueOrNull( $"{rockGroupId}|{locationId}|{archivedSchedule.Id}|{startDateString}" );
                    if ( occurrenceId.HasValue )
                    {
                        attendance.OccurrenceId = occurrenceId.Value;
                    }
                    else
                    {
                        var newOccurrence = AddOccurrence( null, ( DateTime ) startDate, rockGroupId, archivedSchedule.Id, locationId, true );
                        if ( newOccurrence != null )
                        {
                            attendance.OccurrenceId = newOccurrence.Id;
                            existingOccurrences.Add( $"{rockGroupId}|{locationId}|{archivedSchedule.Id}|{startDateString}", newOccurrence.Id );
                        }
                    }

                    newAttendances.Add( attendance );

                    completedItems++;
                    if ( completedItems % percentage < 1 )
                    {
                        var percentComplete = completedItems / percentage;
                        ReportProgress( percentComplete, $"{completedItems:N0} group attendances imported ({percentComplete}% complete)." );
                    }

                    if ( completedItems % ReportingNumber < 1 )
                    {
                        SaveAttendances( newAttendances );
                        ReportPartialProgress();

                        // Reset lists and context
                        lookupContext.Dispose();
                        lookupContext = new RockContext();
                        newAttendances.Clear();
                    }
                }
            }

            if ( newAttendances.Any() )
            {
                SaveAttendances( newAttendances );
            }

            lookupContext.Dispose();
            ReportProgress( 100, $"Finished group attendance import: {completedItems:N0} attendances imported." );
        }

        /// <summary>
        /// Saves the attendances.
        /// </summary>
        /// <param name="newAttendances">The new attendances.</param>
        /// <param name="createWeeklySchedules">if set to <c>true</c> [create weekly schedules].</param>
        private static void SaveAttendances( List<Attendance> newAttendances, bool createWeeklySchedules = false )
        {
            if ( newAttendances.Count > 0 )
            {
                using ( var rockContext = new RockContext() )
                {
                    rockContext.Attendances.AddRange( newAttendances );
                    rockContext.SaveChanges();

                    if ( createWeeklySchedules )
                    {
                        // not compatible with v8
                        //var groupSchedules = newAttendances
                        //    .Where( a => a.Occurrence.GroupId.HasValue )
                        //    .DistinctBy( a => a.Occurrence.GroupId )
                        //    .ToDictionary( a => a.Occurrence.GroupId, a => a.StartDateTime );
                        //foreach ( var group in rockContext.Groups.Where( g => groupSchedules.Keys.Contains( g.Id ) ) )
                        //{
                        //    var attendanceDate = groupSchedules[group.Id];
                        //    group.Schedule = new Schedule
                        //    {
                        //        // Note: this depends on an iCal dependency at save
                        //        WeeklyDayOfWeek = attendanceDate.DayOfWeek,
                        //        WeeklyTimeOfDay = attendanceDate.TimeOfDay,
                        //        CreatedByPersonAliasId = ImportPersonAliasId,
                        //        CreatedDateTime = group.CreatedDateTime,
                        //        ForeignKey = $"Attendance imported {ImportDateTime}"
                        //    };
                        //}

                        //rockContext.SaveChanges();
                    }
                }
            }
        }
    }
}