using Rock.Model;
using System;

namespace Bulldozer.Model
{
    public class GroupMemberCsv
    {
        public string PersonId { get; set; }

        public string GroupId { get; set; }

        public string Role { get; set; }

        public string GroupMemberId { get; set; }

        public GroupMemberStatus GroupMemberStatus { get; set; } = GroupMemberStatus.Active;

        public DateTime? CreatedDate { get; set; }

    }
}