namespace Bulldozer.Model
{
    public class PersonSearchKeyImport
    {
        public int PersonId { get; set; }

        public int PersonAliasId { get; set; }

        public string SearchValue { get; set; }

        public int SearchTypeDefinedValueId { get; set; }

        public string ForeignKey { get; set; }
    }
}