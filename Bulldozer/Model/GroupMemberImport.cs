using Rock.Model;
using System;

namespace Bulldozer.Model
{
    public class GroupMemberImport
    {
        public int? PersonId { get; set; }

        public int? GroupId { get; set; }

        public int? GroupTypeId { get; set; }

        public string RoleName { get; set; }

        public int? RoleId { get; set; }

        public string GroupMemberForeignKey { get; set; }

        public GroupMemberStatus GroupMemberStatus { get; set; }

        public DateTime? InactiveDateTime { get; set; }

        public DateTime? CreatedDate { get; set; }

        public DateTime? DateTimeAdded { get; set; }

        public string Note { get; set; }

    }
}