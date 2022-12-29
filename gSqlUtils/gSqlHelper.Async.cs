using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;
using gpower2.gSqlUtils.Extensions;

namespace gpower2.gSqlUtils
{
    /// <summary>
    /// This is a wrapper class for SqlHeperStatic class, in order to allow
    /// for persistent SQL Connections, DataReader functionality and easier
    /// consuming from users 
    /// </summary>
    public partial class gSqlHelper
    {
        #region "TransactionAsyncHelpers"

        public async Task BeginTransactionAsync()
        {
            await _semaphoreSlim.LockAsync();
            try
            {
                if (_SqlConnection.State == System.Data.ConnectionState.Closed)
                {
                    await _SqlConnection.OpenAsync();
                    Debug.WriteLine(string.Format("{0}[BeginTransactionAsync] Opened connection...", GetNowString()));
                }
                _SqlTransaction = _SqlConnection.BeginTransaction();
                Debug.WriteLine(string.Format("{0} Beginned transaction...", GetNowString()));
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }

        public async Task CommitTransactionAsync()
        {
            await _semaphoreSlim.LockAsync();
            try
            {
                if (_SqlTransaction == null)
                {
                    throw new Exception("There was no transaction to commit!");
                }
                _SqlTransaction.Commit();
                Debug.WriteLine(string.Format("{0}[CommitTransaction] Transaction was committed...", GetNowString()));
                _SqlTransaction = null;
                if (_SqlConnection.State == ConnectionState.Open)
                {
                    CloseConnection();
                }
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }

        public async Task RollbackTransactionAsync()
        {
            await _semaphoreSlim.LockAsync();
            try
            {
                if (_SqlTransaction == null)
                {
                    throw new Exception("There was no transaction to rollback!");
                }
                _SqlTransaction.Rollback();
                Debug.WriteLine(string.Format("{0}[RollbackTransaction] Transaction was rollbacked...", GetNowString()));
                _SqlTransaction = null;
                if (_SqlConnection.State == ConnectionState.Open)
                {
                    CloseConnection();
                }
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }

        #endregion

        #region "CloseConnectionAsync"

        public async Task CloseConnectionAsync()
        {
            await _semaphoreSlim.LockAsync();
            try
            {
                if (_SqlConnection != null && _SqlConnection.State != System.Data.ConnectionState.Closed && _SqlCommands.Count == 0)
                {
                    _SqlConnection.Close();
                    Debug.WriteLine(string.Format("{0}[CloseConnectionAsync] Closed connection...", GetNowString()));
                }
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }

        #endregion

        #region "ExecuteSqlAsync"

        /// <summary>
        /// Executes a non scalar SQL statement.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// It returns the number of rows affected.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <returns>The number of rows affected</returns>
        public Task<int> ExecuteSqlAsync(string argSqlCode)
        {
            return ExecuteSqlAsync(argSqlCode, 120, false, null);
        }

        /// <summary>
        /// Executes a non scalar SQL statement.
        /// It doesn't use a transaction.
        /// It returns the number of rows affected.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>The number of rows affected</returns>
        public Task<int> ExecuteSqlAsync(string argSqlCode, int argTimeout)
        {
            return ExecuteSqlAsync(argSqlCode, argTimeout, false, null);
        }

        /// <summary>
        /// Executes a non scalar SQL statement.
        /// It sets a default timeout of 120 seconds.
        /// It returns the number of rows affected.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction">The SQL transaction to use</param>
        /// <returns>The number of rows affected</returns>
        public Task<int> ExecuteSqlAsync(string argSqlCode, bool argUseTransaction)
        {
            return ExecuteSqlAsync(argSqlCode, 120, argUseTransaction, null);
        }

        /// <summary>
        /// Executes a non scalar SQL statement.
        /// It returns the number of rows affected.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argUseTransaction">The SQL transaction to use</param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns>The number of rows affected</returns>
        public async Task<int> ExecuteSqlAsync(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            int currentCommand;
            // Add a new entry in the SqlCommands Dictionary
            await _semaphoreSlim.LockAsync();
            try
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
            }
            finally
            {
                _semaphoreSlim.Release();
            }

            try
            {
                int resultValue = await SqlHelperStatic.ExecuteSqlAsync(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                return resultValue;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                _LastOperationException = SqlHelperStatic.LastOperationException;
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;

                await _semaphoreSlim.LockAsync();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[ExecuteSqlAsync] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[ExecuteSqlAsync] Closing connection...");
                    await CloseConnectionAsync();
                }
            }
        }

        #endregion

        #region "GetDataTableAsync"

        /// <summary>
        /// Returns a DataTable that contains the results of the execution of an SQL statement.
        /// If the results are a DataSet, then only the first DataTable is returned.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <returns>The DataTable that contains the results</returns>
        public Task<DataTable> GetDataTableAsync(string argSqlCode)
        {
            return GetDataTableAsync(argSqlCode, 120, false, null);
        }

        /// <summary>
        /// Returns a DataTable that contains the results of the execution of an SQL statement.
        /// If the results are a DataSet, then only the first DataTable is returned.
        /// It doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>The DataTable that contains the results</returns>
        public Task<DataTable> GetDataTableAsync(string argSqlCode, int argTimeout)
        {
            return GetDataTableAsync(argSqlCode, argTimeout, false, null);
        }

        /// <summary>
        /// Returns a DataTable that contains the results of the execution of an SQL statement.
        /// If the results are a DataSet, then only the first DataTable is returned.
        /// It sets a default timeout of 120 seconds.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction">The SQL transaction to use</param>
        /// <returns>The DataTable that contains the results</returns>
        public Task<DataTable> GetDataTableAsync(string argSqlCode, bool argUseTransaction)
        {
            return GetDataTableAsync(argSqlCode, 120, argUseTransaction, null);
        }

        /// <summary>
        /// Returns a DataTable that contains the results of the execution of an SQL statement.
        /// If the results are a DataSet, then only the first DataTable is returned.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argUseTransaction">The SQL transaction to use</param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns>The DataTable that contains the results</returns>
        public async Task<DataTable> GetDataTableAsync(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            await _semaphoreSlim.LockAsync();
            try
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
            }
            finally
            {
                _semaphoreSlim.Release();
            }

            try
            {
                DataTable resultValue = await SqlHelperStatic.GetDataTableAsync(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                return resultValue;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                _LastOperationException = SqlHelperStatic.LastOperationException;
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;

                await _semaphoreSlim.LockAsync();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataTableAsync] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataTableAsync] Closing connection...");
                    await CloseConnectionAsync();
                }
            }
        }

        #endregion

        #region "GetDataSetAsync"

        /// <summary>
        /// Returns a DataSet that contains the results of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <returns>The DataSet that contains the results</returns>
        public Task<DataSet> GetDataSetAsync(string argSqlCode)
        {
            return GetDataSetAsync(argSqlCode, 120, false, null);
        }

        /// <summary>
        /// Returns a DataSet that contains the results of the execution of an SQL statement.
        /// It doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>The DataSet that contains the results</returns>
        public Task<DataSet> GetDataSetAsync(string argSqlCode, int argTimeout)
        {
            return GetDataSetAsync(argSqlCode, argTimeout, false, null);
        }

        /// <summary>
        /// Returns a DataSet that contains the results of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction">The SQL transaction to use</param>
        /// <returns>The DataSet that contains the results</returns>
        public Task<DataSet> GetDataSetAsync(string argSqlCode, bool argUseTransaction)
        {
            return GetDataSetAsync(argSqlCode, 120, argUseTransaction, null);
        }

        /// <summary>
        /// Returns a DataSet that contains the results of the execution of an SQL statement.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argUseTransaction">The SQL transaction to use</param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns>The DataSet that contains the results</returns>
        public async Task<DataSet> GetDataSetAsync(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            await _semaphoreSlim.LockAsync();
            try
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
            }
            finally
            {
                _semaphoreSlim.Release();
            }

            try
            {
                DataSet resultValue = await SqlHelperStatic.GetDataSetAsync(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                return resultValue;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                _LastOperationException = SqlHelperStatic.LastOperationException;
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                await _semaphoreSlim.LockAsync();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataSetAsync] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataSetAsync] Closing connection...");
                    await CloseConnectionAsync();
                }
            }
        }

        #endregion

        #region "GetDataValueAsync"

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <returns></returns>
        public Task<object> GetDataValueAsync(string argSqlCode)
        {
            return GetDataValueAsync(argSqlCode, 120, false, null);
        }

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns></returns>
        public Task<object> GetDataValueAsync(string argSqlCode, int argTimeout)
        {
            return GetDataValueAsync(argSqlCode, argTimeout, false, null);
        }

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction">Whether to use SQL Transaction</param>
        /// <returns></returns>
        public Task<object> GetDataValueAsync(string argSqlCode, bool argUseTransaction)
        {
            return GetDataValueAsync(argSqlCode, 120, argUseTransaction, null);
        }

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argUseTransaction">Whether to use SQL Transaction</param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns></returns>
        public async Task<object> GetDataValueAsync(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            await _semaphoreSlim.LockAsync();
            try
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
            }
            finally
            {
                _semaphoreSlim.Release();
            }

            try
            {
                object resultValue = await SqlHelperStatic.GetDataValueAsync(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                return resultValue;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                _LastOperationException = SqlHelperStatic.LastOperationException;
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                await _semaphoreSlim.LockAsync();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataValueAsync] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataValueAsync] Closing connection...");
                    await CloseConnectionAsync();
                }
            }
        }

        #endregion

        #region "GetDataValueAsync<>"

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <returns>If the result is DBNull, then it returns the default value for the Type provided</returns>
        public Task<T> GetDataValueAsync<T>(string argSqlCode) where T : IComparable, IConvertible, IEquatable<T>
        {
            return GetDataValueAsync<T>(argSqlCode, 120, false, null);
        }

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>If the result is DBNull, then it returns the default value for the Type provided</returns>
        public Task<T> GetDataValueAsync<T>(string argSqlCode, int argTimeout) where T : IComparable, IConvertible, IEquatable<T>
        {
            return GetDataValueAsync<T>(argSqlCode, argTimeout, false, null);
        }

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction">Whether to use SQL Transaction</param>
        /// <returns>If the result is DBNull, then it returns the default value for the Type provided</returns>
        public Task<T> GetDataValueAsync<T>(string argSqlCode, bool argUseTransaction) where T : IComparable, IConvertible, IEquatable<T>
        {
            return GetDataValueAsync<T>(argSqlCode, 120, argUseTransaction, null);
        }

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argUseTransaction">Whether to use SQL Transaction</param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns>If the result is DBNull, then it returns the default value for the Type provided</returns>
        public async Task<T> GetDataValueAsync<T>(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters) where T : IComparable, IConvertible, IEquatable<T>
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            await _semaphoreSlim.LockAsync();
            try
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
            }
            finally
            {
                _semaphoreSlim.Release();
            }

            try
            {
                T resultValue = await SqlHelperStatic.GetDataValueAsync<T>(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                return resultValue;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                _LastOperationException = SqlHelperStatic.LastOperationException;
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                await _semaphoreSlim.LockAsync();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataValueAsync<>] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataValueAsync<>] Closing connection...");
                    await CloseConnectionAsync();
                }
            }
        }

        #endregion

        #region "GetDataListAsync"

        /// <summary>
        /// Returns a List of objects of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns an empty List
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argObjectType">The object Type to map the data to</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <returns>The List of objects filled with data</returns>
        public Task<IEnumerable> GetDataListAsync(Type argObjectType, string argSqlCode)
        {
            return GetDataListAsync(argObjectType, argSqlCode, 120, false, null);
        }

        /// <summary>
        /// Returns a List of objects of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns an empty List
        /// It doesn't use a transaction.
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argObjectType">The object Type to map the data to</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>The List of objects filled with data</returns>
        public Task<IEnumerable> GetDataListAsync(Type argObjectType, string argSqlCode, int argTimeout)
        {
            return GetDataListAsync(argObjectType, argSqlCode, argTimeout, false, null);
        }

        /// <summary>
        /// Returns a List of objects of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns an empty List
        /// It sets a default timeout of 120 seconds.
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argObjectType">The object Type to map the data to</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction">Whether to use transaction or not</param>
        /// <returns>The List of objects filled with data</returns>
        public Task<IEnumerable> GetDataListAsync(Type argObjectType, string argSqlCode, bool argUseTransaction)
        {
            return GetDataListAsync(argObjectType, argSqlCode, 120, argUseTransaction, null);
        }

        /// <summary>
        /// Returns a List of objects of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns an empty List
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argObjectType">The object Type to map the data to</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argUseTransaction">Whether to use transaction or not</param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns>The List of objects filled with data</returns>
        public async Task<IEnumerable> GetDataListAsync(Type argObjectType, string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            await _semaphoreSlim.LockAsync();
            try
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
            }
            finally
            {
                _semaphoreSlim.Release();
            }

            try
            {
                IEnumerable results = await SqlHelperStatic.GetDataListAsync(argObjectType, argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                return results;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                _LastOperationException = SqlHelperStatic.LastOperationException;
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                await _semaphoreSlim.LockAsync();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataListAsync] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataListAsync] Closing connection...");
                    await CloseConnectionAsync();
                }
            }
        }

        #endregion

        #region "GetDataListAsync<>"

        /// <summary>
        /// Returns a List of objects of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns an empty List
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <returns>The List of objects filled with data</returns>
        public Task<IEnumerable<T>> GetDataListAsync<T>(string argSqlCode)
        {
            return GetDataListAsync<T>(argSqlCode, 120, false, null);
        }

        /// <summary>
        /// Returns a List of objects of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns an empty List
        /// It doesn't use a transaction.
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>The List of objects filled with data</returns>
        public Task<IEnumerable<T>> GetDataListAsync<T>(string argSqlCode, int argTimeout)
        {
            return GetDataListAsync<T>(argSqlCode, argTimeout, false, null);
        }

        /// <summary>
        /// Returns a List of objects of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns an empty List
        /// It sets a default timeout of 120 seconds.
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction">Whether to use transaction or not</param>
        /// <returns>The List of objects filled with data</returns>
        public Task<IEnumerable<T>> GetDataListAsync<T>(string argSqlCode, bool argUseTransaction)
        {
            return GetDataListAsync<T>(argSqlCode, 120, argUseTransaction, null);
        }

        /// <summary>
        /// Returns a List of objects of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns an empty List
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argUseTransaction">Whether to use transaction or not</param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns>The List of objects filled with data</returns>
        public async Task<IEnumerable<T>> GetDataListAsync<T>(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            await _semaphoreSlim.LockAsync();
            try
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
            }
            finally
            {
                _semaphoreSlim.Release();
            }

            try
            {
                IEnumerable<T> results = await SqlHelperStatic.GetDataListAsync<T>(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                return results;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                _LastOperationException = SqlHelperStatic.LastOperationException;
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                await _semaphoreSlim.LockAsync();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataListAsync<>] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataListAsync<>] Closing connection...");
                    await CloseConnectionAsync();
                }
            }
        }

        #endregion

        #region "GetDataObjectAsync<>"

        /// <summary>
        /// Returns a single object of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns the default object
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argObjectType">The object Type to map the data to</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <returns>The List of objects filled with data</returns>
        public Task<T> GetDataObjectAsync<T>(string argSqlCode)
        {
            return GetDataObjectAsync<T>(argSqlCode, 120, false, null);
        }

        /// <summary>
        /// Returns a single object of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns the default object
        /// It doesn't use a transaction.
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argObjectType">The object Type to map the data to</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>The List of objects filled with data</returns>
        public Task<T> GetDataObjectAsync<T>(string argSqlCode, int argTimeout)
        {
            return GetDataObjectAsync<T>(argSqlCode, argTimeout, false, null);
        }

        /// <summary>
        /// Returns a single object of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns the default object
        /// It sets a default timeout of 120 seconds.
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argObjectType">The object Type to map the data to</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction">Whether to use transaction or not</param>
        /// <returns>The List of objects filled with data</returns>
        public Task<T> GetDataObjectAsync<T>(string argSqlCode, bool argUseTransaction)
        {
            return GetDataObjectAsync<T>(argSqlCode, 120, argUseTransaction, null);
        }

        /// <summary>
        /// Returns a single object of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns the default object
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argObjectType">The object Type to map the data to</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argUseTransaction">Whether to use transaction or not</param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns>The List of objects filled with data</returns>
        public async Task<T> GetDataObjectAsync<T>(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            await _semaphoreSlim.LockAsync();
            try
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
            }
            finally
            {
                _semaphoreSlim.Release();
            }

            try
            {
                T results = await SqlHelperStatic.GetDataObjectAsync<T>(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                return results;
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                _LastOperationException = SqlHelperStatic.LastOperationException;
                _LastOperationTimeSpan = SqlHelperStatic.LastOperationEllapsedTime;
                await _semaphoreSlim.LockAsync();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataObjectAsync<>] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataObjectAsync<>] Closing connection...");
                    await CloseConnectionAsync();
                }
            }
        }

        #endregion
    }
}