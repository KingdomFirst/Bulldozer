using Rock.Model;
using System;
using System.Collections.Generic;

namespace Bulldozer.Model
{
    public class GroupMemberImport
    {
        public int? PersonForeignId { get; set; }

        public int? PersonId { get; set; }

        public int? GroupId { get; set; }

        public int? GroupTypeId { get; set; }

        public string PersonForeignKey { get; set; }

        public string RoleName { get; set; }

        public int? RoleId { get; set; }

        public string GroupMemberForeignKey { get; set; }

        public GroupMemberStatus GroupMemberStatus { get; set; }

        public DateTime? CreatedDate { get; set; }
    }
}