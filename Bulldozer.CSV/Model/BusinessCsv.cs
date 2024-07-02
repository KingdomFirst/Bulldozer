using CsvHelper.Configuration;
using Rock.Model;
using System;
using System.Collections.Generic;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class BusinessCsv
    {
        private string _recordStatus = string.Empty;
        private RecordStatus _recordStatusEnum = CSV.CSVInstance.RecordStatus.Active;
        private bool _isValidRecordStatus = false;

        private string _emailPreference = string.Empty;
        private EmailPreference _emailPreferenceEnum = Rock.Model.EmailPreference.EmailAllowed;
        private bool _isValidEmailPreference = false;

        public string Id { get; set; }

        public string Name { get; set; }

        public string Email { get; set; }

        public string RecordStatus
        {
            get
            {
                return _recordStatus;
            }
            set
            {
                _recordStatus = value;
                _isValidRecordStatus = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _recordStatusEnum );
            }
        }

        public RecordStatus? RecordStatusEnum
        {
            get
            {
                return _recordStatusEnum;
            }
            set
            {
                _recordStatusEnum = value.Value;
                _recordStatus = _recordStatusEnum.ToString();
            }
        }

        public bool IsValidRecordStatus
        {
            get
            {
                return _isValidRecordStatus;
            }
        }

        public string InactiveReasonNote { get; set; }

        public string InactiveReason { get; set; }

        public string EmailPreference
        {
            get
            {
                return _emailPreference;
            }
            set
            {
                _emailPreference = value;
                _isValidEmailPreference = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _emailPreferenceEnum );
            }
        }

        public EmailPreference? EmailPreferenceEnum
        {
            get
            {
                return _emailPreferenceEnum;
            }
            set
            {
                _emailPreferenceEnum = value.Value;
                _emailPreference = _emailPreferenceEnum.ToString();
            }
        }

        public bool IsValidEmailPreference
        {
            get
            {
                return _isValidEmailPreference;
            }
        }

        public DateTime? CreatedDateTime { get; set; }

        public DateTime? ModifiedDateTime { get; set; }

        public CampusCsv Campus { get; set; }

        public string Note { get; set; }

        public bool? IsEmailActive { get; set; }
    }
}