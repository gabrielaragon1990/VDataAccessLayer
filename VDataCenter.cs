using System;
using System.Linq;
using VCollections;

namespace VDataAccessLayer
{
    public static class VDataCenter
    {
        private readonly static VDictionary<Type, IConnectionProvider> Providers = new VDictionary<Type, IConnectionProvider>();
        private readonly static VDictionary<int, VDataContext> DataContextPool = new VDictionary<int, VDataContext>();
        internal readonly static VDictionary<string, VDataContext> DataContextsDAL = new VDictionary<string, VDataContext>();

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

        public static VDataContext CreateContext<TProv>() where TProv : IConnectionProvider
        {
            if (!Providers.ContainsKey(typeof(TProv)))
                throw new InvalidOperationException("Provider was not found in the data center");
            //Returns context:
            return new VDataContext(
                Providers[typeof(TProv)].CreateConnection()
            );
        }

        #region DAL Collection

        public static void AddNewContextToDAL<TProv>(string key) where TProv : IConnectionProvider, new()
        {
            AddNewContextToDAL(key, new TProv());
        }

        public static void AddNewContextToDAL<TProv>(string key, TProv connectionProvider) where TProv : IConnectionProvider
        {
            var context = new VDataContext(connectionProvider.CreateConnection());
            if (!DataContextsDAL.ContainsKey(key))
                DataContextsDAL.Add(key, context);
            DataContextsDAL[key] = context;
        }

        public static VDataContext GetContextFromDAL(string key)
        {
            if (!DataContextsDAL.ContainsKey(key))
                throw new IndexOutOfRangeException("The context key doesn't exists");
            return DataContextsDAL[key];
        }

        public static bool VerifyExistenceOfContextFromDAL(string key)
        {
            return DataContextsDAL.ContainsKey(key);
        }

        public static bool RemoveContextFromDAL(string key)
        {
            bool exists;
            if (exists = DataContextsDAL.ContainsKey(key))
                DataContextsDAL.Remove(key);
            return exists;
        }

        #endregion

        #region DC Pool

        public static int AddNewContextToPool<TProv>() where TProv : IConnectionProvider
        {
            var dataContext = CreateContext<TProv>();
            int key = GetNewPoolKey();
            DataContextPool.Add(key, dataContext);
            return key;
        }

        public static int AddContextToPool(VDataContext dataContext)
        {
            int key = GetNewPoolKey();
            DataContextPool.Add(key, dataContext);
            return key;
        }

        public static VDataContext GetContextFromPool(int contextKey)
        {
            if (!DataContextPool.ContainsKey(contextKey))
                throw new IndexOutOfRangeException("The context key doesn't exists");
            return DataContextPool[contextKey];
        }

        public static bool VerifyExistenceOfContextFromPool(int contextKey)
        {
            return DataContextPool.ContainsKey(contextKey);
        }

        public static bool RemoveContextFromPool(int contextKey)
        {
            bool exists;
            if (exists = DataContextPool.ContainsKey(contextKey))
                DataContextPool.Remove(contextKey);
            return exists;
        }

        #endregion

        #region Private Methods

        private static int GetNewPoolKey() => DataContextPool.Keys.Max() + 1;

        #endregion
    }
}
