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

        public string GroupMemberStatus { get; set; }

        public DateTime? CreatedDate { get; set; }

        public string Note { get; set; }

    }
}