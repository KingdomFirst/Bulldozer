// <copyright>
// Copyright 2024 by Kingdom First Solutions
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
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the location import methods
    /// </summary>
    partial class CSVComponent
    {
        /// <summary>
        /// Loads the named location data.
        /// </summary>

        public int ImportSchedules()
        {
            this.ReportProgress( 0, "Preparing Schedule data for import..." );
            var rockContext = new RockContext();

            var scheduleDict = rockContext.Schedules.AsNoTracking()
                .Where( s => s.ForeignKey != null && s.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                .ToDictionary( k => k.ForeignKey, v => v );

            var scheduleService = new ScheduleService( rockContext );

            var importedDateTime = RockDateTime.Now;

            var schedulesToInsert = new List<Schedule>();

            foreach ( var scheduleCsv in this.ScheduleCsvList )
            {
                var foreignKey = $"{this.ImportInstanceFKPrefix}^{scheduleCsv.Id}";
                if ( scheduleDict.ContainsKey( foreignKey ) )
                {
                    continue;
                }

                var newSchedule = new Schedule
                {
                    Name = scheduleCsv.Name.Left( 50 ),
                    WeeklyTimeOfDay = scheduleCsv.MeetingTime,
                    Description = scheduleCsv.Description,
                    CreatedDateTime = importedDateTime,
                    ModifiedDateTime = importedDateTime,
                    ForeignKey = foreignKey,
                    ForeignId = scheduleCsv.Id.AsIntegerOrNull(),
                    IsActive = scheduleCsv.IsActive
                };
                if ( scheduleCsv.IsValidMeetingDay )
                {
                    newSchedule.WeeklyDayOfWeek = scheduleCsv.MeetingDayEnum.Value;
                }

                schedulesToInsert.Add( newSchedule );
            }

            this.ReportProgress( 0, "Begin processing Schedule records." );
            rockContext.BulkInsert( schedulesToInsert );

            return schedulesToInsert.Count;
        }
    }
}