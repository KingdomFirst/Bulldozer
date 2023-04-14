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

        public List<GroupMemberCsv> GroupMembers { get; set; }

        public List<GroupAddressCsv> Addresses { get; set; }

        public List<BusinessPhoneCsv> PhoneNumbers { get; set; }

    }

    public class GroupCsvMap : ClassMap<GroupCsv>
    {
        public GroupCsvMap()
        {
            Map( m => m.Id );
            Map( m => m.Name );
            Map( m => m.Description );
            Map( m => m.Order ).Optional();
            Map( m => m.ParentGroupId );
            Map( m => m.GroupTypeId );
            Map( m => m.CampusId );
            Map( m => m.Capacity ).Optional();
            Map( m => m.CreatedDate ).Optional();
            Map( m => m.MeetingDay );
            Map( m => m.MeetingTime );
            Map( m => m.IsPublic );
            Map( m => m.IsActive );
        }
    }
}