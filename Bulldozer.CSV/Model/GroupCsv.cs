using System;

namespace Bulldozer.Model
{
    public class GroupCsv
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        public int? Order { get; set; }

        public string ParentGroupId { get; set; }

        public string GroupTypeId { get; set; }

        public string CampusId { get; set; }

        public int? Capacity { get; set; }

        public string MeetingDay { get; set; }

        public string MeetingTime { get; set; }

        public bool? IsPublic { get; set; }

        public bool? IsActive { get; set; }

        public DateTime? CreatedDate { get; set; }

    }
}