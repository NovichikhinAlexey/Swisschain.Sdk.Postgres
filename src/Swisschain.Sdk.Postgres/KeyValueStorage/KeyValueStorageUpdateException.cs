using System;

namespace Swisschain.Sdk.Postgres.KeyValueStorage
{
    public class KeyValueStorageUpdateException : Exception
    {
        public KeyValueStorageUpdateException(Type type, string table, string key, string errorMessage) :
            base($"Failed to update record of type '{type}' in table '{table}' with key '{key}': {errorMessage}")
        {
            Type = type;
            Table = table;
            Key = key;
            ErrorMessage = errorMessage;
        }

        public Type Type { get; }
        public string Table { get; }
        public string Key { get; }
        public string ErrorMessage { get; }
    }
}