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
    public class GroupMemberCsv
    {
        public string PersonId { get; set; }

        public string GroupId { get; set; }

        public string Role { get; set; }

        public string GroupMemberId { get; set; }

        public GroupMemberStatus GroupMemberStatus { get; set; } = GroupMemberStatus.Active;

        public DateTime? CreatedDate { get; set; }

    }

    public class GroupMemberCsvMap : ClassMap<GroupMemberCsv>
    {
        public GroupMemberCsvMap()
        {
            Map( m => m.PersonId );
            Map( m => m.GroupId );
            Map( m => m.Role );
            Map( m => m.GroupMemberId ).Optional();
            Map( m => m.GroupMemberStatus ).Optional();
            Map( m => m.CreatedDate ).Optional();
        }
    }
}