using CsvHelper.Configuration;
using Rock.Model;
using System;

namespace Bulldozer.Model
{
    public class GroupMemberHistoricalCsv
    {
        private string _groupMemberStatusHistorical = string.Empty;
        private GroupMemberStatus _groupMemberStatusHistoricalEnum = Rock.Model.GroupMemberStatus.Active;
        private bool _isValidHistoricalGroupMemberStatus = false;

        private string _groupMemberStatusCurrent = string.Empty;
        private GroupMemberStatus _groupMemberStatusCurrentEnum = Rock.Model.GroupMemberStatus.Inactive;
        private bool _isValidCurrentGroupMemberStatus = false;

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

        public string GroupMemberStatusHistorical
        {
            get
            {
                return _groupMemberStatusHistorical;
            }
            set
            {
                _groupMemberStatusHistorical = value;
                _isValidHistoricalGroupMemberStatus = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _groupMemberStatusHistoricalEnum );
            }
        }

        public GroupMemberStatus? GroupMemberStatusHistoricalEnum
        {
            get
            {
                return _groupMemberStatusHistoricalEnum;
            }
            set
            {
                _groupMemberStatusHistoricalEnum = value.Value;
                _groupMemberStatusHistorical = _groupMemberStatusHistoricalEnum.ToString();
            }
        }

        public bool IsValidHistoricalGroupMemberStatus
        {
            get
            {
                return _isValidHistoricalGroupMemberStatus;
            }
        }

        public string GroupMemberStatusCurrent
        {
            get
            {
                return _groupMemberStatusCurrent;
            }
            set
            {
                _groupMemberStatusCurrent = value;
                _isValidCurrentGroupMemberStatus = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _groupMemberStatusCurrentEnum );
            }
        }

        public GroupMemberStatus? GroupMemberStatusCurrentEnum
        {
            get
            {
                return _groupMemberStatusCurrentEnum;
            }
            set
            {
                _groupMemberStatusCurrentEnum = value.Value;
                _groupMemberStatusCurrent = _groupMemberStatusCurrentEnum.ToString();
            }
        }

        public bool IsValidCurrentGroupMemberStatus
        {
            get
            {
                return _isValidCurrentGroupMemberStatus;
            }
        }

    }
}