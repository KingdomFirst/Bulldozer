﻿using CsvHelper.Configuration;
using System;

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

        public DateTime? CreatedDateTime { get; set; }

    }
}