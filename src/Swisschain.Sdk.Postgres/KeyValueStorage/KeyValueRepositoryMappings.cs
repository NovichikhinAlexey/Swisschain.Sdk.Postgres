using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Swisschain.Sdk.Postgres.KeyValueStorage
{
    public sealed class KeyValueRepositoryMappings
    {
        private ImmutableDictionary<Type, string> _mappings;

        public KeyValueRepositoryMappings()
        {
            _mappings = new Dictionary<Type, string>().ToImmutableDictionary();
        }

        public KeyValueRepositoryMappings Map<TValue>(string tableName)
        {
            _mappings = _mappings.Add(typeof(TValue), tableName);

            return this;
        }

        public string GetTableName<TValue>()
        {
            if (_mappings.TryGetValue(typeof(TValue), out var tableName))
            {
                return tableName;
            }

            throw new InvalidOperationException($"Table name mapping for type {typeof(TValue)} is not found. Use KeyValueRepositoryMappings.Map<TValue>(tableName) to register mapping.");
        }
    }
}
