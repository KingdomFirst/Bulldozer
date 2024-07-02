using System;
using static Bulldozer.CSV.CSVInstance;

namespace Bulldozer.Model
{
    public class PersonSearchKeyCsv
    {
        private string _searchType = string.Empty;
        private PersonSearchKeyType _searchTypeEnum = CSV.CSVInstance.PersonSearchKeyType.Email;

        public string PersonId { get; set; }

        public string SearchValue { get; set; }

        public string SearchType
        {
            get
            {
                return _searchType;
            }
            set
            {
                _searchType = value;
                Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _searchTypeEnum );
            }
        }

        public PersonSearchKeyType? SearchTypeEnum
        {
            get
            {
                return _searchTypeEnum;
            }
            set
            {
                _searchTypeEnum = value.Value;
                _searchType = _searchTypeEnum.ToString();
            }
        }

    }
}