namespace Bulldozer.Model
{
    public class AttributeValueImport
    {
        public int AttributeId { get; set; }

        public string Value { get; set; }

        public string AttributeValueForeignKey { get; set; }

        public int? AttributeValueForeignId { get; set; }

        public int? EntityId { get; set; }
    }
}