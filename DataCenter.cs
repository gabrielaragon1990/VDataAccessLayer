using System;
using System.Collections.Generic;
using System.Linq;

namespace VDataAccessLayer
{
    public static class DataCenter
    {
        private readonly static Dictionary<Type, IConnectionProvider> Providers = new Dictionary<Type, IConnectionProvider>();
        private readonly static Dictionary<int, DataContext> DataContextPool = new Dictionary<int, DataContext>();

        public static void Start(params IConnectionProvider[] connectionProviders)
        {
            if ((connectionProviders?.Length ?? 0) == 0)
                return;
            foreach (var prov in connectionProviders)
                Providers.Add(prov.GetType(), prov);
        }

        public static void AddConnectionProvider<TProv>() where TProv : IConnectionProvider, new()
        {
            AddConnectionProvider(new TProv());
        }

        public static void AddConnectionProvider<TProv>(TProv connectionProvider) where TProv : IConnectionProvider
        {
            if (Providers.ContainsKey(typeof(TProv)))
                Providers[typeof(TProv)] = connectionProvider;
            else Providers.Add(typeof(TProv), connectionProvider);
        }

        public static DataContext CreateContext<TProv>() where TProv : IConnectionProvider
        {
            if (!Providers.ContainsKey(typeof(TProv)))
                throw new InvalidOperationException("Provider was not found in the data center");
            //Returns context:
            return new DataContext(
                Providers[typeof(TProv)].CreateConnection()
            );
        }

        public static int AddNewContextToPool<TProv>() where TProv : IConnectionProvider
        {
            var dataContext = CreateContext<TProv>();
            int key = GetNewPoolKey();
            DataContextPool.Add(key, dataContext);
            return key;
        }

        public static int AddContextToPool(DataContext dataContext)
        {
            int key = GetNewPoolKey();
            DataContextPool.Add(key, dataContext);
            return key;
        }

        public static DataContext GetContext(int contextKey)
        {
            if (!DataContextPool.ContainsKey(contextKey))
                throw new IndexOutOfRangeException("The context key doesn't exists");
            return DataContextPool[contextKey];
        }

        public static bool RemoveContext(int contextKey)
        {
            bool exists;
            if (exists = DataContextPool.ContainsKey(contextKey))
                DataContextPool.Remove(contextKey);
            return exists;
        }

        #region Private Methods

        private static int GetNewPoolKey() => DataContextPool.Keys.Max() + 1;

        #endregion
    }
}
