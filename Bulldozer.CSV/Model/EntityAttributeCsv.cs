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

        public string AttributeEntityType { get; set; }

        public string GroupTypeId { get; set; }

    }
}