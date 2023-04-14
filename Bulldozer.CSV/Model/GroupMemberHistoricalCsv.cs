using CsvHelper.Configuration;
using Rock.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Bulldozer.CSV.CSVInstance;

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

    public class GroupMemberHistoricalCsvMap : ClassMap<GroupMemberHistoricalCsv>
    {
        public GroupMemberHistoricalCsvMap()
        {
            Map( m => m.ArchivedDateTime ).Optional();
            Map( m => m.CurrentRowIndicator );
            Map( m => m.EffectiveDateTime );
            Map( m => m.ExpireDateTime );
            Map( m => m.InactiveDateTime ).Optional();
            Map( m => m.IsLeader ).Optional();
            Map( m => m.IsArchived ).Optional();
            Map( m => m.GroupId );
            Map( m => m.GroupMemberId ).Optional();
            Map( m => m.GroupMemberStatus ).Optional();
            Map( m => m.PersonId );
            Map( m => m.Role );
        }
    }
}