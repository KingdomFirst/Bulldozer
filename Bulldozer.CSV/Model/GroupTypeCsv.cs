using CsvHelper.Configuration;
using System;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class GroupTypeCsv
    {
        private string _locationSelectionMode = string.Empty;
        private LocationSelectionMode _locationSelectionModeEnum = CSV.CSVInstance.LocationSelectionMode.None;
        private bool _isValidLocationSelectionMode = false;

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

        public string LocationSelectionMode
        {
            get
            {
                return _locationSelectionMode;
            }
            set
            {
                _locationSelectionMode = value;
                _isValidLocationSelectionMode = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _locationSelectionModeEnum );
            }
        }

        public LocationSelectionMode? LocationSelectionModeEnum
    {
            get
            {
                return _locationSelectionModeEnum;
            }
            set
            {
                _locationSelectionModeEnum = value.Value;
                _locationSelectionMode = _locationSelectionModeEnum.ToString();
            }
        }

        public bool IsValidLocationSelectionMode
        {
            get
            {
                return _isValidLocationSelectionMode;
            }
        }

        public string LocationTypes { get; set; }

        public bool? AllowMultipleLocations { get; set; } = false;

        public bool? IsSchedulingEnabled { get; set; } = false;

        public bool? EnableLocationSchedules { get; set; } = false;

        public DateTime? CreatedDateTime { get; set; }

    }
}