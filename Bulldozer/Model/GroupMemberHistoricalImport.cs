using Rock.Model;
using System;
using System.Collections.Generic;

namespace Bulldozer.Model
{
    public class GroupMemberHistoricalImport
    {
        public DateTime? ArchivedDateTime { get; set; }

        public bool? IsLeader { get; set; } = false;

        public bool? IsArchived { get; set; }

        public int? GroupId { get; set; }

        public string GroupForeignKey { get; set; }

        public int? GroupMemberId { get; set; }

        public string GroupMemberForeignKey { get; set; }

        public GroupMember GroupMember { get; set; }

        public string GroupMemberHistoricalForeignKey { get; set; }

        public int? GroupTypeId { get; set; }

        public string PersonForeignKey { get; set; }

        public int? PersonId { get; set; }

        public string Role { get; set; }

        public int? RoleId { get; set; }

        public bool CurrentRowIndicator { get; set; }

        public DateTime ExpireDateTime { get; set; }

        public DateTime EffectiveDateTime { get; set; }

        public DateTime? InactiveDateTime { get; set; }

        public GroupMemberStatus GroupMemberStatus { get; set; }

    }
}