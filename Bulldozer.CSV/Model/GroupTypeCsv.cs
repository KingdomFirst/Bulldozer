using CsvHelper.Configuration;
using System;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class GroupTypeCsv
    {
        public string Id { get; set; }

        public string Description { get; set; }

        public string Name { get; set; }

        public string GroupTypePurpose { get; set; }

        public string InheritedGroupTypeGuid { get; set; }

        public string ParentGroupTypeId { get; set; }

        public bool? AllowWeeklySchedule { get; set; } = false;

        public bool? TakesAttendance { get; set; } = false;

        public bool? WeekendService { get; set; } = false;

        public bool? SelfReference { get; set; } = false;

        public bool? ShowInGroupList { get; set; } = true;

        public bool? ShowInNav { get; set; } = true;

        public LocationSelectionMode? LocationSelectionMode { get; set; } = null;

        public string LocationTypes { get; set; }

        public bool? AllowMultipleLocations { get; set; } = false;

        public bool? IsSchedulingEnabled { get; set; } = false;

        public bool? EnableLocationSchedules { get; set; } = false;

        public DateTime? CreatedDateTime { get; set; }

    }
}