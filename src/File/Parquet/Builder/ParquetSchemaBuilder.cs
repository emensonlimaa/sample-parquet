using Parquet.Schema;

namespace File.Parquet.Builder;

public class ParquetSchemaBuilder
{
    private readonly List<DataField> _fields = new();

    public ParquetSchemaBuilder AddField<T>(string columnName)
    {
        _fields.Add(new DataField<T>(columnName));
        return this;
    }

    public ParquetSchema Build()
    {
        return new ParquetSchema(_fields);
    }
}
