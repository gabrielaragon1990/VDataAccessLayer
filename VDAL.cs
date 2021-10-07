using VCollections;

namespace VDataAccessLayer
{
    public static class VDAL
    {
        public static VDictionary<string, VDataContext> Contexts => VDataCenter.DataContextsDAL;
    }
}