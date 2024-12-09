using Parquet.Data;
using Parquet.Schema;

namespace File.Parquet.Builder;

public class DataColumnBuilder
{
    private readonly List<DataColumn> _columns = new();

    public DataColumnBuilder AddColumn<T>(string columnName, IEnumerable<T> data)
    {
        _columns.Add(new DataColumn(new DataField<T>(columnName), data.ToArray()));
        return this;
    }

    public IList<DataColumn> Build()
    {
        return _columns;
    }
}