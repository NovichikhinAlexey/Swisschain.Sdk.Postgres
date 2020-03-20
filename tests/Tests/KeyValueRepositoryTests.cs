using System.Linq;
using System.Threading.Tasks;
using Shouldly;
using Swisschain.Sdk.Postgres.KeyValueStorage;
using Xunit;

namespace Tests
{

    // TODO: Refactor
    // TODO: Implement all cases
    // TODO: Run tests against Postgres container
    public class KeyValueRepositoryTests
    {
        private const string ConnectionString = "secret";

        public class Doc
        {
            public string A { get; set; }
            public long B { get; set; }
        }

        [Fact(Skip = "manual")]
        public async Task CanQueryAll()
        {
            var mappings = new KeyValueRepositoryMappings()
                .Map<Doc>("brokerage.docs");

            var repo = new KeyValueRepository(ConnectionString, mappings);

            foreach (var i in Enumerable.Range(1, 10))
            {
                await repo.DeleteAsync<Doc>(i.ToString("D5"));

                await repo.InsertAsync(i.ToString("D5"), new Doc
                {
                    A = i.ToString("D5"),
                    B = 123
                });
            }

            var all = await repo.QueryAllAsync<Doc>();

            all.Count.ShouldBe(10);
        }

        [Fact(Skip = "manual")]
        public async Task CanQueryWithLimit()
        {
            var mappings = new KeyValueRepositoryMappings()
                .Map<Doc>("brokerage.docs");

            var repo = new KeyValueRepository(ConnectionString, mappings);

            foreach (var i in Enumerable.Range(1, 10))
            {
                await repo.DeleteAsync<Doc>(i.ToString("D5"));

                await repo.InsertAsync(i.ToString("D5"), new Doc
                {
                    A = i.ToString("D5"),
                    B = 123
                });
            }

            var all = (await repo.QueryAsync<Doc>(default, default, 5, true)).ToArray();

            all.Length.ShouldBe(5);

            foreach (var i in Enumerable.Range(1, 5))
            {
                all[i - 1].A.ShouldBe(i.ToString("D5"));
            }
        }

        [Fact(Skip = "manual")]
        public async Task CanQueryWithLimitDescending()
        {
            var mappings = new KeyValueRepositoryMappings()
                .Map<Doc>("brokerage.docs");

            var repo = new KeyValueRepository(ConnectionString, mappings);

            foreach (var i in Enumerable.Range(1, 10))
            {
                await repo.DeleteAsync<Doc>(i.ToString("D5"));

                await repo.InsertAsync(i.ToString("D5"), new Doc
                {
                    A = i.ToString("D5"),
                    B = 123
                });
            }

            var all = (await repo.QueryAsync<Doc>(default, default, 5, false)).Reverse().ToArray();

            all.Length.ShouldBe(5);

            foreach (var i in Enumerable.Range(6, 5))
            {
                all[i - 6].A.ShouldBe(i.ToString("D5"));
            }
        }

        [Fact(Skip = "manual")]
        public async Task CanQueryWithStartingAfterAndLimit()
        {
            var mappings = new KeyValueRepositoryMappings()
                .Map<Doc>("brokerage.docs");

            var repo = new KeyValueRepository(ConnectionString, mappings);

            foreach (var i in Enumerable.Range(1, 10))
            {
                await repo.DeleteAsync<Doc>(i.ToString("D5"));

                await repo.InsertAsync(i.ToString("D5"), new Doc
                {
                    A = i.ToString("D5"),
                    B = 123
                });
            }

            var all = (await repo.QueryAsync<Doc>("00003", default, 5, true)).ToArray();

            all.Length.ShouldBe(5);

            foreach (var i in Enumerable.Range(4, 5))
            {
                all[i - 4].A.ShouldBe(i.ToString("D5"));
            }
        }

        [Fact(Skip = "manual")]
        public async Task CanQueryWithStartingAfterDescendingAndLimit()
        {
            var mappings = new KeyValueRepositoryMappings()
                .Map<Doc>("brokerage.docs");

            var repo = new KeyValueRepository(ConnectionString, mappings);

            foreach (var i in Enumerable.Range(1, 10))
            {
                await repo.DeleteAsync<Doc>(i.ToString("D5"));

                await repo.InsertAsync(i.ToString("D5"), new Doc
                {
                    A = i.ToString("D5"),
                    B = 123
                });
            }

            var all = (await repo.QueryAsync<Doc>("00003", default, 5, false)).Reverse().ToArray();

            all.Length.ShouldBe(5);

            foreach (var i in Enumerable.Range(4, 5))
            {
                all[i - 4].A.ShouldBe(i.ToString("D5"));
            }
        }

        [Fact(Skip = "manual")]
        public async Task CanQueryWithStartingAfterAndEndingBefore()
        {
            var mappings = new KeyValueRepositoryMappings()
                .Map<Doc>("brokerage.docs");

            var repo = new KeyValueRepository(ConnectionString, mappings);

            foreach (var i in Enumerable.Range(1, 10))
            {
                await repo.DeleteAsync<Doc>(i.ToString("D5"));

                await repo.InsertAsync(i.ToString("D5"), new Doc
                {
                    A = i.ToString("D5"),
                    B = 123
                });
            }

            var all = (await repo.QueryAsync<Doc>("00003", "00008", 5, true)).ToArray();

            all.Length.ShouldBe(4);

            foreach (var i in Enumerable.Range(4, 4))
            {
                all[i - 4].A.ShouldBe(i.ToString("D5"));
            }
        }

        [Fact(Skip = "manual")]
        public async Task CanQueryWithStartingAfterAndEndingBeforeDescending()
        {
            var mappings = new KeyValueRepositoryMappings()
                .Map<Doc>("brokerage.docs");

            var repo = new KeyValueRepository(ConnectionString, mappings);

            foreach (var i in Enumerable.Range(1, 10))
            {
                await repo.DeleteAsync<Doc>(i.ToString("D5"));

                await repo.InsertAsync(i.ToString("D5"), new Doc
                {
                    A = i.ToString("D5"),
                    B = 123
                });
            }

            var all = (await repo.QueryAsync<Doc>("00003", "00008", 5, false)).Reverse().ToArray();

            all.Length.ShouldBe(4);

            foreach (var i in Enumerable.Range(4, 4))
            {
                all[i - 4].A.ShouldBe(i.ToString("D5"));
            }
        }

        [Fact(Skip = "manual")]
        public async Task CanQueryWithEndingBefore()
        {
            var mappings = new KeyValueRepositoryMappings()
                .Map<Doc>("brokerage.docs");

            var repo = new KeyValueRepository(ConnectionString, mappings);

            foreach (var i in Enumerable.Range(1, 10))
            {
                await repo.DeleteAsync<Doc>(i.ToString("D5"));

                await repo.InsertAsync(i.ToString("D5"), new Doc
                {
                    A = i.ToString("D5"),
                    B = 123
                });
            }

            var all = (await repo.QueryAsync<Doc>(default, "00008", 10, true)).ToArray();

            all.Length.ShouldBe(7);

            foreach (var i in Enumerable.Range(1, 7))
            {
                all[i - 1].A.ShouldBe(i.ToString("D5"));
            }
        }

        [Fact(Skip = "manual")]
        public async Task CanQueryWithEndingBeforeDescending()
        {
            var mappings = new KeyValueRepositoryMappings()
                .Map<Doc>("brokerage.docs");

            var repo = new KeyValueRepository(ConnectionString, mappings);

            foreach (var i in Enumerable.Range(1, 10))
            {
                await repo.DeleteAsync<Doc>(i.ToString("D5"));

                await repo.InsertAsync(i.ToString("D5"), new Doc
                {
                    A = i.ToString("D5"),
                    B = 123
                });
            }

            var all = (await repo.QueryAsync<Doc>(default, "00008", 10, false)).Reverse().ToArray();

            all.Length.ShouldBe(2);

            foreach (var i in Enumerable.Range(9, 2))
            {
                all[i - 9].A.ShouldBe(i.ToString("D5"));
            }
        }

        [Fact(Skip = "manual")]
        public async Task Test1()
        {
            var mappings = new KeyValueRepositoryMappings()
                .Map<Doc>("brokerage.docs");

            var repo = new KeyValueRepository(ConnectionString, mappings);

            await repo.InsertAsync("12345", new Doc
            {
                A = "xxx",
                B = 123
            });

            await repo.InsertOrReplaceAsync("123456", new Doc
            {
                A = "xxx",
                B = 123
            });

            await repo.InsertOrReplaceAsync("123456", new Doc
            {
                A = "yyy",
                B = 1234
            });

            await repo.InsertOrIgnoreAsync("1234567", new Doc
            {
                A = "xxx",
                B = 123
            });

            await repo.InsertOrIgnoreAsync("1234567", new Doc
            {
                A = "yyy",
                B = 1234
            });

            await repo.InsertAsync("1", new Doc
            {
                A = "xxx",
                B = 123
            });

            await repo.DeleteAsync<Doc>("1", true);


            var r1 = await repo.GetOrDefaultAsync<Doc>("12345");
            var r2 = await repo.GetOrDefaultAsync<Doc>("123456");
            var r3 = await repo.GetOrDefaultAsync<Doc>("1234567");
        }
    }
}
