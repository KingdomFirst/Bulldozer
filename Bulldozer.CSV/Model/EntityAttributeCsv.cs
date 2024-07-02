using System;

namespace Bulldozer.Model
{
    public class EntityAttributeCsv
    {
        private string _attributeEntityType = string.Empty;
        private AttributeEntityType _attributeEntityTypeEnum = Model.AttributeEntityType.Person;
        private bool _isValidAttributeEntityType = false;

        public string Key { get; set; }

        public string Name { get; set; }

        public string FieldType { get; set; }

        public string Category { get; set; }

        public string DefinedTypeId { get; set; }

        public bool? DefinedTypeAllowMultiple { get; set; } = false;

        public string AttributeEntityType
        {
            get
            {
                return _attributeEntityType;
            }
            set
            {
                _attributeEntityType = value;
                _isValidAttributeEntityType = Enum.TryParse( value.Trim().Replace( " ", string.Empty ), true, out _attributeEntityTypeEnum );
            }
        }

        public AttributeEntityType? AttributeEntityTypeEnum
        {
            get
            {
                return _attributeEntityTypeEnum;
            }
            set
            {
                _attributeEntityTypeEnum = value.Value;
                _attributeEntityType = _attributeEntityTypeEnum.ToString();
            }
        }

        public bool IsValidAttributeEntityType
        {
            get
            {
                return _isValidAttributeEntityType;
            }
        }

        public string GroupTypeId { get; set; }

    }

    public enum AttributeEntityType
    {
        Person = 0,
        Business = 1,
        Family = 2,
        Group = 3
    }
}