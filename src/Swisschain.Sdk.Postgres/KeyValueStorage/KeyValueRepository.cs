using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Npgsql;

namespace Swisschain.Sdk.Postgres.KeyValueStorage
{
    public class KeyValueRepository : IKeyValueRepository
    {
        private readonly string _connectionString;
        private readonly KeyValueRepositoryMappings _mappings;

        public KeyValueRepository(
            string connectionString,
            KeyValueRepositoryMappings mappings)
        {
            _connectionString = connectionString;
            _mappings = mappings;
        }

        public async Task InsertAsync<TValue>(string key, TValue value) 
            where TValue : class
        {
            var tableName = _mappings.GetTableName<TValue>();
            var serializedValue = JsonConvert.SerializeObject(value);

            await using var connection = new NpgsqlConnection(_connectionString);

            await connection.OpenAsync();

            await using var command = connection.CreateCommand();

            command.CommandText = $"insert into {tableName} (key, value) values (@key, @value)";

            command.Parameters.Add(new NpgsqlParameter("@key", DbType.String) {Value = key});
            command.Parameters.Add(new NpgsqlParameter("@value", DbType.String) {Value = serializedValue});

            await command.ExecuteNonQueryAsync();
        }

        public async Task InsertOrReplaceAsync<TValue>(string key, TValue value) 
            where TValue : class
        {
            var tableName = _mappings.GetTableName<TValue>();
            var serializedValue = JsonConvert.SerializeObject(value);

            await using var connection = new NpgsqlConnection(_connectionString);

            await connection.OpenAsync();

            await using var command = connection.CreateCommand();

            command.CommandText = $"insert into {tableName} (key, value) values (@key, @value) on conflict (key) do update set value = @value where {tableName}.key = @key";

            command.Parameters.Add(new NpgsqlParameter("@key", DbType.String) {Value = key});
            command.Parameters.Add(new NpgsqlParameter("@value", DbType.String) {Value = serializedValue});

            await command.ExecuteNonQueryAsync();
        }

        public async Task InsertOrIgnoreAsync<TValue>(string key, TValue value) 
            where TValue : class
        {
            var tableName = _mappings.GetTableName<TValue>();
            var serializedValue = JsonConvert.SerializeObject(value);

            await using var connection = new NpgsqlConnection(_connectionString);

            await connection.OpenAsync();

            await using var command = connection.CreateCommand();

            command.CommandText = $"insert into {tableName} (key, value) values (@key, @value) on conflict (key) do nothing";

            command.Parameters.Add(new NpgsqlParameter("@key", DbType.String) {Value = key});
            command.Parameters.Add(new NpgsqlParameter("@value", DbType.String) {Value = serializedValue});

            await command.ExecuteNonQueryAsync();
        }

        public async Task UpdateAsync<TValue>(string key, TValue value, bool throwOnNotFound = true) 
            where TValue : class
        {
            var tableName = _mappings.GetTableName<TValue>();
            var serializedValue = JsonConvert.SerializeObject(value);

            await using var connection = new NpgsqlConnection(_connectionString);

            await connection.OpenAsync();

            await using var command = connection.CreateCommand();

            command.CommandText = $"update {tableName} set value = @value where key = @key";

            command.Parameters.Add(new NpgsqlParameter("@key", DbType.String) {Value = key});
            command.Parameters.Add(new NpgsqlParameter("@value", DbType.String) {Value = serializedValue});

            if (await command.ExecuteNonQueryAsync() != 1 && throwOnNotFound)
            {
                throw new KeyValueStorageUpdateException(typeof(TValue), tableName, key, "Record with specified key was not found to update");
            }
        }

        public async Task DeleteAsync<TValue>(string key, bool throwOnNotFound = false) 
            where TValue : class
        {
            var tableName = _mappings.GetTableName<TValue>();

            await using var connection = new NpgsqlConnection(_connectionString);

            await connection.OpenAsync();

            await using var command = connection.CreateCommand();

            command.CommandText = $"delete from {tableName} where key = @key";

            command.Parameters.Add(new NpgsqlParameter("@key", DbType.String) {Value = key});

            if (await command.ExecuteNonQueryAsync() != 1 && throwOnNotFound)
            {
                throw new KeyValueStorageUpdateException(typeof(TValue), tableName, key, "Record with specified key was not found to delete");
            }
        }

        public async Task<TValue> GetOrDefaultAsync<TValue>(string key) 
            where TValue : class
        {
            var tableName = _mappings.GetTableName<TValue>();

            await using var connection = new NpgsqlConnection(_connectionString);

            await connection.OpenAsync();

            await using var command = connection.CreateCommand();

            command.CommandText = $"select key, value from {tableName} where key = @key";

            command.Parameters.Add(new NpgsqlParameter("@key", DbType.String) {Value = key});

            await using var reader = await command.ExecuteReaderAsync();

            if (await reader.ReadAsync())
            {
                var serializedValue = reader.GetString("value");

                return JsonConvert.DeserializeObject<TValue>(serializedValue);
            }

            return default;
        }

        public async Task<TValue> GetOrAsync<TValue>(string key)
            where TValue : class
        {
            var value = await GetOrDefaultAsync<TValue>(key);

            if (value == default)
            {
                var tableName = _mappings.GetTableName<TValue>();

                throw new KeyValueStorageUpdateException(typeof(TValue), tableName, key, "Record with specified key was not found");
            }

            return value;
        }

        public async Task<IReadOnlyCollection<TValue>> QueryAllAsync<TValue>() 
            where TValue : class
        {
            var tableName = _mappings.GetTableName<TValue>();

            await using var connection = new NpgsqlConnection(_connectionString);

            await connection.OpenAsync();

            await using var command = connection.CreateCommand();

            command.CommandText = $"select key, value from {tableName}";

            await using var reader = await command.ExecuteReaderAsync();

            var result = new List<TValue>();
            
            while(await reader.ReadAsync())
            {
                var serializedValue = reader.GetString("value");
                var value = JsonConvert.DeserializeObject<TValue>(serializedValue);

                result.Add(value);
            }

            return result;
        }

        public async Task<IReadOnlyCollection<TValue>> QueryAsync<TValue>(string startingAfter, string endingBefore, int limit, bool ascending) 
            where TValue : class
        {
            if (limit < 1 || limit > 1000)
            {
                throw new ArgumentOutOfRangeException(nameof(limit), limit, "Should be in range 1..1000");
            }

            var tableName = _mappings.GetTableName<TValue>();

            await using var connection = new NpgsqlConnection(_connectionString);

            await connection.OpenAsync();

            await using var command = connection.CreateCommand();

            var commandText = new StringBuilder();

            commandText.Append($"select key, value from {tableName}");

            if (startingAfter != default)
            {
                commandText.Append($" where {tableName}.key > @startingAfter");

                command.Parameters.Add(new NpgsqlParameter("@startingAfter", DbType.String) {Value = startingAfter});
            }

            if (endingBefore != default)
            {
                commandText.Append(startingAfter == default ? " where" : " and");
                commandText.Append($" {tableName}.key < @endingBefore");

                command.Parameters.Add(new NpgsqlParameter("@endingBefore", DbType.String) {Value = endingBefore});
            }

            commandText.Append(ascending ? $" order by {tableName}.key asc" : $" order by {tableName}.key desc");
            commandText.Append($" limit {limit}");

            command.CommandText = commandText.ToString();
            

            await using var reader = await command.ExecuteReaderAsync();

            var result = new List<TValue>();

            while(await reader.ReadAsync())
            {
                var serializedValue = reader.GetString("value");
                var value = JsonConvert.DeserializeObject<TValue>(serializedValue);

                result.Add(value);
            }

            return result;
        }
    }
}
