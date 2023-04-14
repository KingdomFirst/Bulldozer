using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bulldozer.Model
{
    public class GroupTypeCsv
    {
        public string Id { get; set; }

        public string Description { get; set; }

        public string Name { get; set; }

        public bool? IsCheckinPurpose { get; set; } = false;

        public Guid? InheritedGroupTypeGuid { get; set; }

        public string ParentGroupTypeId { get; set; }

        public bool? AllowWeeklySchedule { get; set; } = false;

        public bool? TakesAttendance { get; set; } = false;

        public bool? WeekendService { get; set; } = false;

        public bool? SelfReference { get; set; } = false;

        public bool? ShowInGroupList { get; set; } = true;

        public bool? ShowInNav { get; set; } = true;

    }

    public class GroupTypeCsvMap : ClassMap<GroupTypeCsv>
    {
        public GroupTypeCsvMap()
        {
            Map( m => m.Id );
            Map( m => m.Description ).Optional();
            Map( m => m.Name );
            Map( m => m.IsCheckinPurpose ).Optional();
            Map( m => m.InheritedGroupTypeGuid ).Optional();
            Map( m => m.TakesAttendance ).Optional();
            Map( m => m.AllowWeeklySchedule ).Optional();
            Map( m => m.ShowInGroupList ).Optional();
            Map( m => m.ShowInNav ).Optional();
            Map( m => m.ParentGroupTypeId ).Optional();
        }
    }
}