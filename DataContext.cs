using System;
using System.Collections.Generic;
using System.Data;
using VCollections;
using VCollections.Tables;

namespace VDataAccessLayer
{
    public delegate void VReadRow(VRow row);
    public delegate bool VLogicWhere(VRow row);

    public class DataContext : IDisposable
    {
        #region Atributes

        private readonly object _objLock = new object();
        private readonly bool defaultThrowException = true;
        private IDbTransaction currentTransaction = null;

        #endregion

        protected IDbConnection _dbConnection;

        public bool IsOpen => _dbConnection?.State == ConnectionState.Open;

        public DataContext(IDbConnection dbConnection)
        {
            _dbConnection = dbConnection;
            /* Abre conexión en caso de no estarlo */
            Open();
        }

        #region Public Methods

        public void Open()
        {
            if (!IsOpen) _dbConnection.Open();
        }

        public void Close()
        {
            if (IsOpen) _dbConnection.Close();
        }

        /*SELECT QUERIES*/

        public bool ExecuteSelectQuery(string sqlQuery, VReadRow readEachRow) => 
            ExecuteSelectQuery(sqlQuery, null, readEachRow);

        public bool ExecuteSelectQuery(string sqlQuery, IDbDataParameter[] parameters, VReadRow readEachRow) =>
            ExecuteSelectQuery(sqlQuery, parameters, null, readEachRow);

        public bool ExecuteSelectQuery(string sqlQuery, IDbDataParameter[] parameters, VLogicWhere logicWhere, VReadRow readEachRow) =>
            _ExecuteSelectQuery(readEachRow, sqlQuery, parameters, logicWhere, defaultThrowException);

        public bool ExecuteSelectQuery(bool throwException, string sqlQuery, VReadRow readEachRow) =>
            ExecuteSelectQuery(throwException, sqlQuery, null, readEachRow);

        public bool ExecuteSelectQuery(bool throwException, string sqlQuery, IDbDataParameter[] parameters, VReadRow readEachRow) =>
            ExecuteSelectQuery(throwException, sqlQuery, parameters, null, readEachRow);

        public bool ExecuteSelectQuery(bool throwException, string sqlQuery, IDbDataParameter[] parameters, VLogicWhere logicWhere, VReadRow readEachRow) =>
            _ExecuteSelectQuery(readEachRow, sqlQuery, parameters, logicWhere, throwException);

        /*GET A FIELD VALUE*/

        public VCell GetFieldValue(string sqlQuery, params IDbDataParameter[] parameters) =>
            GetFieldValue(0, sqlQuery, parameters);

        public VCell GetFieldValue(int fieldIndex, string sqlQuery, params IDbDataParameter[] parameters) =>
            GetFieldValue(defaultThrowException, fieldIndex, sqlQuery, parameters);

        public VCell GetFieldValue(bool throwException, string sqlQuery, params IDbDataParameter[] parameters) =>
            GetFieldValue(throwException, 0, sqlQuery, parameters);

        public VCell GetFieldValue(bool throwException, int fieldIndex, string sqlQuery, params IDbDataParameter[] parameters)
        {
            VCell cell = null;
            ExecuteSelectQuery(throwException, sqlQuery, parameters)
                .GetFirstRow(r => cell = r[fieldIndex]);
            return cell;
        }

        public VCell GetFieldValue(string fieldName, string sqlQuery, params IDbDataParameter[] parameters) =>
            GetFieldValue(defaultThrowException, fieldName, sqlQuery, parameters);

        public VCell GetFieldValue(bool throwException, string fieldName, string sqlQuery, params IDbDataParameter[] parameters)
        {
            VCell cell = null;
            ExecuteSelectQuery(throwException, sqlQuery, parameters)
                .GetFirstRow(r => cell = r[fieldName]);
            return cell;
        }

        /*TRY GET A FIELD VALUE*/

        public bool TryGetFieldValue(out VCell cell, string sqlQuery, params IDbDataParameter[] parameters) =>
            TryGetFieldValue(out cell, 0, sqlQuery, parameters);

        public bool TryGetFieldValue(out VCell cell, int fieldIndex, string sqlQuery, params IDbDataParameter[] parameters) =>
            TryGetFieldValue(out cell, defaultThrowException, fieldIndex, sqlQuery, parameters);

        public bool TryGetFieldValue(out VCell cell, bool throwException, string sqlQuery, params IDbDataParameter[] parameters) =>
            TryGetFieldValue(out cell, throwException, 0, sqlQuery, parameters);

        public bool TryGetFieldValue(out VCell cell, bool throwException, int fieldIndex, string sqlQuery, params IDbDataParameter[] parameters)
        {
            VCell _cell = null;
            var got = ExecuteSelectQuery(throwException, sqlQuery, parameters)
                .GetFirstRow(r => _cell = r[fieldIndex]);
            cell = _cell;
            return got;
        }

        public bool TryGetFieldValue(out VCell cell, string fieldName, string sqlQuery, params IDbDataParameter[] parameters) =>
            TryGetFieldValue(out cell, defaultThrowException, fieldName, sqlQuery, parameters);

        public bool TryGetFieldValue(out VCell cell, bool throwException, string fieldName, string sqlQuery, params IDbDataParameter[] parameters)
        {
            VCell _cell = null;
            var got = ExecuteSelectQuery(throwException, sqlQuery, parameters)
                .GetFirstRow(r => _cell = r[fieldName]);
            cell = _cell;
            return got;
        }

        /*SELECT QUERIES WITH VTable RESULT*/

        public VTable ExecuteSelectQuery(string sqlQuery, params IDbDataParameter[] parameters) =>
            ExecuteSelectQuery(sqlQuery, null, parameters);

        public VTable ExecuteSelectQuery(string sqlQuery, VLogicWhere logicWhere, params IDbDataParameter[] parameters) =>
            _ExcecuteSelectQuery(sqlQuery, parameters, logicWhere, defaultThrowException);

        public VTable ExecuteSelectQuery(bool throwException, string sqlQuery, params IDbDataParameter[] parameters) =>
            ExecuteSelectQuery(throwException, sqlQuery, null, parameters);

        public VTable ExecuteSelectQuery(bool throwException, string sqlQuery, VLogicWhere logicWhere, params IDbDataParameter[] parameters) =>
            _ExcecuteSelectQuery(sqlQuery, parameters, logicWhere, throwException);

        /*NON QUERIES*/

        public int ExecuteNonQuery(string nonQuery, params IDbDataParameter[] parameters) =>
            ExecuteNonQuery(defaultThrowException, nonQuery, parameters);

        public int ExecuteNonQuery(bool throwException, string nonQuery, params IDbDataParameter[] parameters) =>
            _ExecuteNonQuery(nonQuery, parameters, throwException);

        /*STORE PROCEDURES*/

        public Dictionary<string, VCell> ExecuteStoredProcedure(string storeProcedureName, params IDbDataParameter[] parameters) =>
            ExecuteStoredProcedure(defaultThrowException, storeProcedureName, parameters);

        public Dictionary<string, VCell> ExecuteStoredProcedure(bool throwException, string storeProcedureName, params IDbDataParameter[] parameters)
        {
            var results = new Dictionary<string, VCell>();
            int _rowsAffected = 0;
            IDbCommand command = null;
            try
            {
                command = CreateCommand(storeProcedureName, parameters);
                command.CommandType = CommandType.StoredProcedure;
                _rowsAffected = command.ExecuteNonQuery();
                //Obtiene los resultados del Store
                if (VCollection.Contains(ref parameters, p => p.Direction != ParameterDirection.Input))
                    foreach (IDbDataParameter p in command.Parameters)
                        if (p.Direction != ParameterDirection.Input) 
                            results.Add(p.ParameterName, new VCell(p.Value));
            }
            catch (Exception e)
            {
                ShowExceptionMessageInDebug(e);
                if (throwException) _ThrowException(
                    $"Error executing Store Procedure '{storeProcedureName}', Inner: {e.Message}", e);
            }
            finally
            {
                if (command != null) command.Dispose();
            }
            Helper.WriteSQLQuery($"[STORED PROCEDURE]: {storeProcedureName}", parameters, _rowsAffected);
            return results;
        }

        /*TRANSACTIONS*/

        public void ExecuteTransaction(Action transaction) => 
            ExecuteTransaction(false, transaction);

        public void ExecuteTransaction(Action transaction, Action<Exception> onError) =>
            ExecuteTransaction(false, transaction, onError);

        public void ExecuteTransaction(Action transaction, IsolationLevel isolationLevel) => 
            ExecuteTransaction(false, transaction, isolationLevel);

        public void ExecuteTransaction(Action transaction, Action<Exception> onError, IsolationLevel isolationLevel) =>
            ExecuteTransaction(false, transaction, onError, isolationLevel);

        public void ExecuteTransaction(bool throwException, Action transaction) => 
            ExecuteTransaction(throwException, transaction, IsolationLevel.Unspecified);

        public void ExecuteTransaction(bool throwException, Action transaction, Action<Exception> onError) =>
            ExecuteTransaction(throwException, transaction, onError, IsolationLevel.Unspecified);

        public void ExecuteTransaction(bool throwException, Action transaction, IsolationLevel isolationLevel) =>
            ExecuteTransaction(throwException, transaction, null, isolationLevel);

        public void ExecuteTransaction(bool throwException, Action transaction, Action<Exception> onError, IsolationLevel isolationLevel)
        {
            if (transaction == null) return;
            try
            {
                BeginTranstraction(isolationLevel);
                transaction();
                Commit();
            }
            catch (Exception e)
            {
                ShowExceptionMessageInDebug(e);
                Rollback();
                onError?.Invoke(e);
                if (throwException)
                    _ThrowException($"Error trying to begin sql transaction, Inner: {e.Message}", e);
            }
            GC.Collect();
        }

        /*ACTIONS*/

        public void BeginTranstraction(IsolationLevel isolationLevel = IsolationLevel.Unspecified) => ProtectedAction(() =>
        {
            if (!IsOpen)
                throw new InvalidOperationException("The connections isn't open");
            if (currentTransaction != null) return;

            try
            {
                currentTransaction = _dbConnection.BeginTransaction(isolationLevel);
            }
            catch (Exception x)
            {
                ShowExceptionMessageInDebug(x);
                currentTransaction = null;
                throw new Exception("Error trying to begin sql transaction, See inner exception for more details.", x);
            }
        });

        public void Commit() => ProtectedAction(() =>
        {
            if (currentTransaction == null)
                throw new InvalidOperationException("There haven't an open transaction");
            try
            {
                currentTransaction.Commit();
                currentTransaction.Dispose();
                currentTransaction = null;
            }
            catch (Exception x)
            {
                ShowExceptionMessageInDebug(x);
            }
        });

        public void Rollback() => ProtectedAction(() =>
        {
            if (currentTransaction == null)
                throw new InvalidOperationException("There haven't an open transaction");
            try
            {
                currentTransaction.Rollback();
                currentTransaction.Dispose();
                currentTransaction = null;
            }
            catch (Exception x)
            {
                ShowExceptionMessageInDebug(x);
            }
        });

        public IDbCommand CreateCommand()
        {
            var cmd = _dbConnection.CreateCommand();
            cmd.Connection = _dbConnection;
            //Si hay una transacción dandose en el hilo actual
            ProtectedAction(() =>
            {
                if (currentTransaction != null)
                    cmd.Transaction = currentTransaction;
            });
            return cmd;
        }

        public IDbCommand CreateCommand(string sql, IDbDataParameter[] parameters)
        {
            var command = CreateCommand();
            command.CommandText = sql;
            if (parameters != null && parameters.Length > 0)
                VCollection.Foreach(ref parameters, p => command.Parameters.Add(p));
            return command;
        }

        public void Dispose()
        {
            if (currentTransaction != null)
                Rollback();
            Close();
            _dbConnection.Dispose();
            _dbConnection = null;
        }

        #endregion

        #region Private Methods

        private int _ExecuteNonQuery(string nonQuery, IDbDataParameter[] parameters, bool throwException)
        {
            int rowsAffected = 0;
            IDbCommand command = null;
            try
            {
                command = CreateCommand(nonQuery, parameters);
                rowsAffected = command.ExecuteNonQuery();
                Helper.WriteSQLQuery(nonQuery, parameters, rowsAffected);
            }
            catch (Exception e)
            {
                ShowExceptionMessageInDebug(e);
                if (throwException) _ThrowException(
                    $"An error occurred while trying to execute the Non Query statement, Inner: {e.Message}", e);
            }
            finally
            {
                if (command != null) command.Dispose();
            }
            return rowsAffected;
        }

        private VTable _ExcecuteSelectQuery(string sqlQuery, IDbDataParameter[] parameters, VLogicWhere logicWhere, bool throwException)
        {
            var table = new VTable();
            _ExecuteSelectQuery(throwException, sqlQuery, parameters, reader =>
            { //Obtiene los nombres de columna
                var _columnNames = new string[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++)
                    _columnNames[i] = reader.GetName(i);
                //Crea columnas en la tabla
                foreach (var c in _columnNames)
                    table.CreateColumn(c);
                //Obtiene cada fila:
                var _rowValues = new VList<object>();
                while (reader.Read())
                {
                    foreach (string col in _columnNames) 
                        _rowValues.Add(reader[col] ?? DBNull.Value);
                    table.Insert(_rowValues.ToArray());
                    _rowValues.Clear();
                }
                //En caso de haberse obtenido valores:
                if (table.RowCount > 0)
                    //Aplica el filtro:
                    table.Delete(row => !(logicWhere?.Invoke(row) ?? true));
                //
                _columnNames = null;
            });
            return table;
        }

        private bool _ExecuteSelectQuery(VReadRow readEachRow, string sqlQuery, IDbDataParameter[] parameters, VLogicWhere logicWhere, bool throwException)
        {
            if (readEachRow == null) return false;
            var isOK = false;
            _ExecuteSelectQuery(throwException, sqlQuery, parameters, reader =>
            {
                var columnNames = new string[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++)
                    columnNames[i] = reader.GetName(i);
                VRow row = null;
                var _cells = new VList<VCell>();
                while (reader.Read())
                {
                    foreach (string col in columnNames)
                        _cells.Add(new VCell(reader[col]));
                    row = new VRow(columnNames, _cells.ToArray());
                    //Valida si se cumple con el WHERE lógico, si es así, entonces manda a que sea leído:
                    if (logicWhere?.Invoke(row) ?? true) readEachRow(row);
                    //
                    _cells.Clear();
                }
                VCollection.CleanResource(ref columnNames);
                isOK = true;
            });
            return isOK;
        }

        private void _ExecuteSelectQuery(bool throwException, string selectQuery, IDbDataParameter[] parameters, Action<IDataReader> actionWithReader)
        {
            if (actionWithReader == null) return;

            var isOK = false;
            IDataReader reader = null;
            IDbCommand command = null;
            Helper.WriteSQLQuery(selectQuery, parameters, 0);
            try
            {
                command = CreateCommand(selectQuery, parameters);
                reader = command.ExecuteReader();
                isOK = true;
            }
            catch (Exception e)
            {
                ShowExceptionMessageInDebug(e);
                if (throwException) _ThrowException(
                    $"An error occurred while trying to execute the Select Query statement, Inner: {e.Message}", e);
                isOK = false;
            }
            finally
            {
                if (isOK)
                {
                    try
                    {
                        actionWithReader(reader);
                        reader.Dispose();
                        command.Dispose();
                    }
                    catch (Exception e)
                    {
                        ShowExceptionMessageInDebug(e);
                        if (throwException) _ThrowException(
                            $"An error occurred while trying to execute the Select Query statement, Inner: {e.Message}", e);
                    }
                }
            }
        }

        private void ProtectedAction(Action action)
        {
            lock (_objLock) action?.Invoke();
        }

        private void _ThrowException(string message, Exception innerException = null) => 
            throw new Exception(message ?? "Error", innerException);

        private void ShowExceptionMessageInDebug(Exception e)
        {
            do
            {
                Helper.ShowInDebug($"ERROR!! - InnerException: Message: {e.Message}\nStack: {e.StackTrace}");
                e = e.InnerException;
            }
            while (e != null);
        }

        #endregion
    }
}
