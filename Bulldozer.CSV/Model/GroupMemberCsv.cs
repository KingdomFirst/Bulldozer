using Rock.Model;
using System;

namespace Bulldozer.Model
{
    public class GroupMemberCsv
    {
        private string _groupMemberStatus = string.Empty;
        private GroupMemberStatus _groupMemberStatusEnum = Rock.Model.GroupMemberStatus.Active;
        private bool _isValidGroupMemberStatus = false;

        public string PersonId { get; set; }

        public string GroupId { get; set; }

        public string Role { get; set; }

        public string GroupMemberId { get; set; }

        public string GroupMemberStatus
        {
            get
            {
                return _groupMemberStatus;
            }
            set
            {
                _groupMemberStatus = value;
                _isValidGroupMemberStatus = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _groupMemberStatusEnum );
            }
        }

        public GroupMemberStatus? GroupMemberStatusEnum
        {
            get
            {
                return _groupMemberStatusEnum;
            }
            set
            {
                _groupMemberStatusEnum = value.Value;
                _groupMemberStatus = _groupMemberStatusEnum.ToString();
            }
        }

        public bool IsValidGroupMemberStatus
        {
            get
            {
                return _isValidGroupMemberStatus;
            }
        }

        public DateTime? CreatedDate { get; set; }

        public DateTime? DateTimeAdded { get; set; }

        public string Note { get; set; }

    }
}