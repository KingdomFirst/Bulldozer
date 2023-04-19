using CsvHelper.Configuration;
using Rock.Model;
using System;

namespace Bulldozer.Model
{
    public class GroupMemberHistoricalCsv
    {
        public DateTime? ArchivedDateTime { get; set; } = null;

        public bool? IsLeader { get; set; } = false;

        public bool? IsArchived { get; set; } = false;

        public string GroupId { get; set; }

        public string GroupMemberId { get; set; } = null;

        public string PersonId { get; set; }

        public string Role { get; set; }

        public bool CurrentRowIndicator { get; set; } = false;

        public DateTime ExpireDateTime { get; set; }

        public DateTime EffectiveDateTime { get; set; }

        public DateTime? InactiveDateTime { get; set; } = null;

        public GroupMemberStatus? GroupMemberStatus { get; set; } = null;

    }
}