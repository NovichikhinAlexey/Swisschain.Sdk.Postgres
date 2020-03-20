using System.Collections.Generic;
using System.Threading.Tasks;

namespace Swisschain.Sdk.Postgres.KeyValueStorage
{
    public interface IKeyValueRepository
    {
        Task InsertAsync<TValue>(string key, TValue value) where TValue : class;
        
        Task InsertOrReplaceAsync<TValue>(string key, TValue value) where TValue : class;
        
        Task InsertOrIgnoreAsync<TValue>(string key, TValue value) where TValue : class;
        
        /// <exception cref="KeyValueStorageUpdateException">Record was not found to update</exception>
        Task UpdateAsync<TValue>(string key, TValue value, bool throwOnNotFound = true) where TValue : class;
        
        /// <exception cref="KeyValueStorageUpdateException">Record was not found to delete</exception>
        Task DeleteAsync<TValue>(string key, bool throwOnNotFound = false) where TValue : class;
        
        Task<TValue> GetOrDefaultAsync<TValue>(string key) where TValue : class;
        
        /// <exception cref="KeyValueStorageUpdateException">Record was not found</exception>
        Task<TValue> GetOrAsync<TValue>(string key) where TValue : class;
        
        Task<IReadOnlyCollection<TValue>> QueryAllAsync<TValue>() where TValue : class;
        
        Task<IReadOnlyCollection<TValue>> QueryAsync<TValue>(string startingAfter, string endingBefore, int limit, bool ascending) where TValue : class;
    }
}