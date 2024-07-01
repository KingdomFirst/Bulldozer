namespace Bulldozer.Model
{
    public class EntityAttributeCsv
    {
        public string Key { get; set; }

        public string Name { get; set; }

        public string FieldType { get; set; }

        public string Category { get; set; }

        public string DefinedTypeId { get; set; }

        public bool? DefinedTypeAllowMultiple { get; set; } = false;

        public AttributeEntityType? AttributeEntityType { get; set; } = Model.AttributeEntityType.Person;

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