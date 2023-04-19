using System;

namespace Bulldozer.Model
{
    public class GroupImport
    {
        public int? GroupForeignId { get; set; }

        public string GroupForeignKey { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        public int? Order { get; set; } = -1;

        public int? ParentGroupForeignId { get; set; }

        public string ParentGroupForeignKey { get; set; }

        public int GroupTypeId { get; set; }

        public int? CampusId { get; set; }

        public int? Capacity { get; set; }

        public DateTime? CreatedDate { get; set; }

        public string MeetingDay { get; set; }

        public string MeetingTime { get; set; }

        public bool IsActive { get; set; }

        public bool IsPublic { get; set; }
    }

}