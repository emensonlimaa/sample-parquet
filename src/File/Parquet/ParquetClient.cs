using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace File.Parquet;

public class ParquetClient
{
    public async Task WriteAsync<T>(string filePath, ParquetSchema schema, IList<DataColumn> columns)
    {
        try
        {
            using Stream fileStream = new BufferedStream(System.IO.File.OpenWrite($"{filePath}.parquet"));

            using ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(schema, fileStream);

            parquetWriter.CompressionMethod = CompressionMethod.Snappy;
            parquetWriter.CompressionLevel = System.IO.Compression.CompressionLevel.Fastest;

            var groupWriter = parquetWriter.CreateRowGroup();

            var columnWriteTasks = columns.Select(column =>
                WriteColumnAsync(groupWriter, column)
            ).ToArray();

            await Task.WhenAll(columnWriteTasks);
        }
        catch (Exception)
        {
            throw;
        }       
    }

    public async Task<IList<IList<T>>> ReadPageAsync<T>(string filePath, int pageIndex, int pageSize)
    {
        try
        {
            using var fileStream = System.IO.File.OpenRead($"{filePath}.parquet");

            var reader = await ParquetReader.CreateAsync(fileStream);

            var schema = reader.Schema;
            var rowGroupCount = reader.RowGroupCount;

            var allRows = new List<IList<T>>();
            int totalRowsRead = 0;
            int startRow = pageIndex * pageSize;  

            for (int rowGroupIndex = 0; rowGroupIndex < rowGroupCount; rowGroupIndex++)
            {
                var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);

                var columnReadTasks = schema.Fields.Select(field =>
                    ReadColumnAsync<T>(rowGroupReader, field, startRow, pageSize)
                ).ToArray();

                var columnsData = await Task.WhenAll(columnReadTasks);

                foreach (var columnData in columnsData)
                {
                    var rowsToAdd = columnData.Take(pageSize).ToList();
                    allRows.Add(rowsToAdd);
                    totalRowsRead += rowsToAdd.Count;

                    if (totalRowsRead >= pageSize)
                        break;
                }

                if (totalRowsRead >= pageSize)
                    break;
            }

            return allRows;
        }
        catch (Exception)
        {
            throw;
        }
    }

    private async Task<IList<T>> ReadColumnAsync<T>(ParquetRowGroupReader rowGroupReader, Field field, int startRow, int pageSize)
    {
        if (field is DataField dataField)
        {
            var columnData = await rowGroupReader.ReadColumnAsync(dataField);

            var dataList = columnData.Data.Cast<object>().ToList();

            return dataList.Skip(startRow).Take(pageSize)
                .Select(item => (T)Convert.ChangeType(item, typeof(T)))
                .ToList();
        }

        throw new InvalidCastException();
    }

    private async Task WriteColumnAsync(ParquetRowGroupWriter groupWriter, DataColumn column)
    {
        await groupWriter.WriteColumnAsync(column);
    }
}


