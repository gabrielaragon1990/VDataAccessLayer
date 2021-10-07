using System.Data;
using System.Diagnostics;

namespace VDataAccessLayer
{
    internal static class Helper
    {
        internal static void WriteSQLQuery(string query, int rowsAffected = 0) => 
            ShowInDebug($"\nExecuted Query:\n\n{query}\n\n-- {rowsAffected} row(s) affected\n");

        internal static void WriteSQLQuery(string query, IDbDataParameter[] parameters, int rowsAffected = 0)
        {
            var paramsStr = "";
            if (parameters?.Length > 0)
                paramsStr = $"\n{string.Join<IDbDataParameter>("\n", parameters)}\n";
            ShowInDebug($"\nExecuted Query:\n\n{query}\n{paramsStr}\n-- {rowsAffected} row(s) affected\n");
        }

        internal static void ShowInDebug(string message) => Debug.WriteLine(message);
    }
}
