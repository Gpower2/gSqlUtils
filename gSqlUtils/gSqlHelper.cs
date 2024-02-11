using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Threading;

namespace gpower2.gSqlUtils
{
    /// <summary>
    /// This is a wrapper class for SqlHeperStatic class, in order to allow
    /// for persistent SQL Connections, DataReader functionality and easier
    /// consuming from users 
    /// </summary>
    public partial class gSqlHelper
    {
        #region "Properties"

        private SqlConnection _SqlConnection = null;

        /// <summary>
        /// The SqlConnection of the object. It is read-only.
        /// </summary>
        public SqlConnection gSqlConnection
        {
            get { return _SqlConnection; }
        }

        private SqlTransaction _SqlTransaction = null;

        /// <summary>
        /// The SqlTransaction of the object.
        /// </summary>
        public SqlTransaction gSqlTransaction
        {
            get { return _SqlTransaction; }
        }

        private Exception _LastOperationException = null;

        public Exception LastOperationException
        {
            get { return _LastOperationException; }
        }

        private TimeSpan _LastOperationTimeSpan = new TimeSpan();

        public TimeSpan LastOperationTimeSpan
        {
            get { return _LastOperationTimeSpan; }
        }

        #endregion

        #region "Fields"

        private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1, 1);

        /// <summary>
        /// A list that contains all the pending SQL Commands' hash codes
        /// </summary>
        private readonly List<int> _SqlCommands = new List<int>();

        #endregion

        #region "Constructor"

        /// <summary>
        /// 
        /// </summary>
        public gSqlHelper() { }

        /// <summary>
        /// Constructor that initializes the object with an existing SqlConnection
        /// </summary>
        /// <param name="argSqlConnection"></param>
        public gSqlHelper(SqlConnection argSqlConnection)
        {
            _SqlConnection = argSqlConnection;
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, string argUserId, string argPassword)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, string argUserId, string argPassword, bool argPersistSecurityInfo)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argPersistSecurityInfo);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, string argUserId, string argPassword, bool argPersistSecurityInfo,
            bool argMultipleActiveResultSets)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argPersistSecurityInfo, argMultipleActiveResultSets);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, string argUserId, string argPassword, string argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argApplicationName);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, string argUserId, string argPassword, string argApplicationName,
            bool argPersistSecurityInfo, bool argMultipleActiveResultSets)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argApplicationName, argPersistSecurityInfo, argMultipleActiveResultSets);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, string argUserId, string argPassword, int argConnectTimeout)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argConnectTimeout);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, string argUserId, string argPassword, int argConnectTimeout, string argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argConnectTimeout, argApplicationName);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, string argUserId, string argPassword, int argConnectTimeout, int argPacketSize)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argConnectTimeout, argPacketSize);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, string argUserId, string argPassword, int argConnectTimeout, int argPacketSize, string argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argConnectTimeout, argPacketSize, argApplicationName);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, string argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argApplicationName);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, string argApplicationName, bool argMultipleActiveResultSets)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argApplicationName, argMultipleActiveResultSets);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, int argConnectTimeout)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, int argConnectTimeout, string argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, argApplicationName);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, int argConnectTimeout, int argPacketSize)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, argPacketSize);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, int argConnectTimeout, int argPacketSize, string argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, argPacketSize, argApplicationName);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, int argConnectTimeout,
            bool argIntegratedSecurity, int argPacketSize, string argUserId, string argPassword, string argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, argIntegratedSecurity, argPacketSize, argUserId, argPassword, argApplicationName, false, false);
        }

        public gSqlHelper(string argDataSource, string argInitialCatalog, int argConnectTimeout,
            bool argIntegratedSecurity, int argPacketSize, string argUserId, string argPassword, string argApplicationName,
            bool argMultipleActiveResultSets, bool argPersistSecurityInfo)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, argIntegratedSecurity, argPacketSize, argUserId, argPassword, argApplicationName, argMultipleActiveResultSets, argPersistSecurityInfo);
        }

        #endregion

        #region "CloseConnection"

        public void CloseConnection()
        {
            _semaphoreSlim.Wait();
            try
            {
                if (_SqlConnection != null && _SqlConnection.State != System.Data.ConnectionState.Closed && _SqlCommands.Count == 0)
                {
                    _SqlConnection.Close();
                    Debug.WriteLine(string.Format("{0}[CloseConnection] Closed connection...", GetNowString()));
                }
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }

        #endregion

        #region "TransactionHelpers"

        public void BeginTransaction()
        {
            _semaphoreSlim.Wait();
            try
            {
                if (_SqlConnection.State == System.Data.ConnectionState.Closed)
                {
                    _SqlConnection.Open();
                    Debug.WriteLine(string.Format("{0}[BeginTransaction] Opened connection...", GetNowString()));
                }
                _SqlTransaction = _SqlConnection.BeginTransaction();
                Debug.WriteLine(string.Format("{0} Beginned transaction...", GetNowString()));
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }

        public void CommitTransaction()
        {
            _semaphoreSlim.Wait();
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

        public void RollbackTransaction()
        {
            _semaphoreSlim.Wait();
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

        #region "IsNull"

        /// <summary>
        /// It checks if an object is null or equal to DBNull.Value
        /// and then it returns the user defined object for that case, 
        /// else it returns the source object.
        /// </summary>
        /// <param name="argSourceObject">The object to check</param>
        /// <param name="argNullValue">The object to return, if the source object is null or equals to DBNull.Value</param>
        /// <returns></returns>
        public object IsNull(object argSourceObject, object argNullValue)
        {
            return SqlHelperStatic.IsNull(argSourceObject, argNullValue);
        }

        #endregion

        #region "IsNullString"

        /// <summary>
        /// It checks if a string is null and then it returns NULL.
        /// Else, it replaces the escape characters ' and " and 
        /// returns the string single quoted eg. text => 'text'
        /// </summary>
        /// <param name="argSourceString">The source string to check for null</param>
        /// <returns></returns>
        public string IsNullString(string argSourceString)
        {
            return IsNullString(argSourceString, true, false, true);
        }

        /// <summary>
        /// It checks if a string is null and then it returns NULL.
        /// Else, according to user options, it either replaces the
        /// escape characters ' and " or not, it either replaces the
        /// wildcard characters % and _ or not, and it either single
        /// quotes the string or not eg. text => 'text'
        /// </summary>
        /// <param name="argSourceString">The source string to check for null</param>
        /// <param name="argEscapeString">The flag whether to replace the escape characters or not</param>
        /// <param name="argEscapeWildcards">The flag whether to replace the wildcard characters or not</param>
        /// <param name="argQuoteString">The flag whether to single quote the string or not</param>
        /// <returns></returns>
        public string IsNullString(string argSourceString, bool argEscapeString, bool argEscapeWildcards, bool argQuoteString)
        {
            return SqlHelperStatic.IsNullString(argSourceString, argEscapeString, argEscapeWildcards, argQuoteString);
        }

        #endregion

        #region "EscapeString"

        /// <summary>
        /// It escapes the string by replacing ' with ''.
        /// </summary>
        /// <param name="argSourceString"></param>
        /// <returns></returns>
        public string EscapeString(string argSourceString)
        {
            return EscapeString(argSourceString, false);
        }

        /// <summary>
        /// It escapes the string by replacing ' with ''.
        /// It also escapes the wildcard charactes % and _ 
        /// if the user specifies it.
        /// </summary>
        /// <param name="argSourceString">The source string to escape</param>
        /// <param name="argEscapeWildcards">The flag to whether escape the wildcard characters or not</param>
        /// <returns>The escaped string</returns>
        public string EscapeString(string argSourceString, bool argEscapeWildcards)
        {
            return SqlHelperStatic.EscapeString(argSourceString, argEscapeWildcards);
        }

        #endregion

        #region "ExecuteSql"

        /// <summary>
        /// Executes a non scalar SQL statement.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// It returns the number of rows affected.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <returns>The number of rows affected</returns>
        public int ExecuteSql(string argSqlCode)
        {
            return ExecuteSql(argSqlCode, 120, false, null);
        }

        /// <summary>
        /// Executes a non scalar SQL statement.
        /// It doesn't use a transaction.
        /// It returns the number of rows affected.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>The number of rows affected</returns>
        public int ExecuteSql(string argSqlCode, int argTimeout)
        {
            return ExecuteSql(argSqlCode, argTimeout, false, null);
        }

        /// <summary>
        /// Executes a non scalar SQL statement.
        /// It sets a default timeout of 120 seconds.
        /// It returns the number of rows affected.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction">The SQL transaction to use</param>
        /// <returns>The number of rows affected</returns>
        public int ExecuteSql(string argSqlCode, bool argUseTransaction)
        {
            return ExecuteSql(argSqlCode, 120, argUseTransaction, null);
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
        public int ExecuteSql(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            int currentCommand;
            // Add a new entry in the SqlCommands Dictionary
            _semaphoreSlim.Wait();
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
                int resultValue = SqlHelperStatic.ExecuteSql(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
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
                _semaphoreSlim.Wait();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[ExecuteSql] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[ExecuteSql] Closing connection...");
                    CloseConnection();
                }
            }
        }

        #endregion

        #region "GetDataTable"

        /// <summary>
        /// Returns a DataTable that contains the results of the execution of an SQL statement.
        /// If the results are a DataSet, then only the first DataTable is returned.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <returns>The DataTable that contains the results</returns>
        public DataTable GetDataTable(string argSqlCode)
        {
            return GetDataTable(argSqlCode, 120, false, null);
        }

        /// <summary>
        /// Returns a DataTable that contains the results of the execution of an SQL statement.
        /// If the results are a DataSet, then only the first DataTable is returned.
        /// It doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>The DataTable that contains the results</returns>
        public DataTable GetDataTable(string argSqlCode, int argTimeout)
        {
            return GetDataTable(argSqlCode, argTimeout, false, null);
        }

        /// <summary>
        /// Returns a DataTable that contains the results of the execution of an SQL statement.
        /// If the results are a DataSet, then only the first DataTable is returned.
        /// It sets a default timeout of 120 seconds.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction">The SQL transaction to use</param>
        /// <returns>The DataTable that contains the results</returns>
        public DataTable GetDataTable(string argSqlCode, bool argUseTransaction)
        {
            return GetDataTable(argSqlCode, 120, argUseTransaction, null);
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
        public DataTable GetDataTable(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            _semaphoreSlim.Wait();
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
                DataTable resultValue = SqlHelperStatic.GetDataTable(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
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
                _semaphoreSlim.Wait();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataTable] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataTable] Closing connection...");
                    CloseConnection();
                }
            }
        }

        #endregion

        #region "GetDataSet"

        /// <summary>
        /// Returns a DataSet that contains the results of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <returns>The DataSet that contains the results</returns>
        public DataSet GetDataSet(string argSqlCode)
        {
            return GetDataSet(argSqlCode, 120, false, null);
        }

        /// <summary>
        /// Returns a DataSet that contains the results of the execution of an SQL statement.
        /// It doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>The DataSet that contains the results</returns>
        public DataSet GetDataSet(string argSqlCode, int argTimeout)
        {
            return GetDataSet(argSqlCode, argTimeout, false, null);
        }

        /// <summary>
        /// Returns a DataSet that contains the results of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction">The SQL transaction to use</param>
        /// <returns>The DataSet that contains the results</returns>
        public DataSet GetDataSet(string argSqlCode, bool argUseTransaction)
        {
            return GetDataSet(argSqlCode, 120, argUseTransaction, null);
        }

        /// <summary>
        /// Returns a DataSet that contains the results of the execution of an SQL statement.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argUseTransaction">The SQL transaction to use</param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns>The DataSet that contains the results</returns>
        public DataSet GetDataSet(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            _semaphoreSlim.Wait();
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
                DataSet resultValue = SqlHelperStatic.GetDataSet(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
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
                _semaphoreSlim.Wait();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataSet] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataSet] Closing connection...");
                    CloseConnection();
                }
            }
        }

        #endregion

        #region "GetDataValue"

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <returns></returns>
        public object GetDataValue(string argSqlCode)
        {
            return GetDataValue(argSqlCode, 120, false, null);
        }

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns></returns>
        public object GetDataValue(string argSqlCode, int argTimeout)
        {
            return GetDataValue(argSqlCode, argTimeout, false, null);
        }

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction">Whether to use SQL Transaction</param>
        /// <returns></returns>
        public object GetDataValue(string argSqlCode, bool argUseTransaction)
        {
            return GetDataValue(argSqlCode, 120, argUseTransaction, null);
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
        public object GetDataValue(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            _semaphoreSlim.Wait();
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
                object resultValue = SqlHelperStatic.GetDataValue(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
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
                _semaphoreSlim.Wait();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataValue] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataValue] Closing connection...");
                    CloseConnection();
                }
            }
        }

        #endregion

        #region "GetDataValue<>"

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <returns>If the result is DBNull, then it returns the default value for the Type provided</returns>
        public T GetDataValue<T>(string argSqlCode) where T : IComparable, IConvertible, IEquatable<T>
        {
            return GetDataValue<T>(argSqlCode, 120, false, null);
        }

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>If the result is DBNull, then it returns the default value for the Type provided</returns>
        public T GetDataValue<T>(string argSqlCode, int argTimeout) where T : IComparable, IConvertible, IEquatable<T>
        {
            return GetDataValue<T>(argSqlCode, argTimeout, false, null);
        }

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction">Whether to use SQL Transaction</param>
        /// <returns>If the result is DBNull, then it returns the default value for the Type provided</returns>
        public T GetDataValue<T>(string argSqlCode, bool argUseTransaction) where T : IComparable, IConvertible, IEquatable<T>
        {
            return GetDataValue<T>(argSqlCode, 120, argUseTransaction, null);
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
        public T GetDataValue<T>(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters) where T : IComparable, IConvertible, IEquatable<T>
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            _semaphoreSlim.Wait();
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
                T resultValue = SqlHelperStatic.GetDataValue<T>(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
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
                _semaphoreSlim.Wait();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataValue<>] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataValue<>] Closing connection...");
                    CloseConnection();
                }
            }
        }

        #endregion

        #region "GetDataList"

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
        public IEnumerable GetDataList(Type argObjectType, string argSqlCode)
        {
            return GetDataList(argObjectType, argSqlCode, 120, false, null);
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
        public IEnumerable GetDataList(Type argObjectType, string argSqlCode, int argTimeout)
        {
            return GetDataList(argObjectType, argSqlCode, argTimeout, false, null);
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
        public IEnumerable GetDataList(Type argObjectType, string argSqlCode, bool argUseTransaction)
        {
            return GetDataList(argObjectType, argSqlCode, 120, argUseTransaction, null);
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
        public IEnumerable GetDataList(Type argObjectType, string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            _semaphoreSlim.Wait();
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
                IEnumerable results = SqlHelperStatic.GetDataList(argObjectType, argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
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
                _semaphoreSlim.Wait();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataList] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataList] Closing connection...");
                    CloseConnection();
                }
            }
        }

        #endregion

        #region "GetDataList<>"

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
        public IEnumerable<T> GetDataList<T>(string argSqlCode)
        {
            return GetDataList<T>(argSqlCode, 120, false, null);
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
        public IEnumerable<T> GetDataList<T>(string argSqlCode, int argTimeout)
        {
            return GetDataList<T>(argSqlCode, argTimeout, false, null);
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
        public IEnumerable<T> GetDataList<T>(string argSqlCode, bool argUseTransaction)
        {
            return GetDataList<T>(argSqlCode, 120, argUseTransaction, null);
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
        public IEnumerable<T> GetDataList<T>(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            _semaphoreSlim.Wait();
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
                IEnumerable<T> results = SqlHelperStatic.GetDataList<T>(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
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
                _semaphoreSlim.Wait();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataList<>] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataList<>] Closing connection...");
                    CloseConnection();
                }
            }
        }

        #endregion

        #region "GetDataObject<>"

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
        public T GetDataObject<T>(string argSqlCode)
        {
            return GetDataObject<T>(argSqlCode, 120, false, null);
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
        public T GetDataObject<T>(string argSqlCode, int argTimeout)
        {
            return GetDataObject<T>(argSqlCode, argTimeout, false, null);
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
        public T GetDataObject<T>(string argSqlCode, bool argUseTransaction)
        {
            return GetDataObject<T>(argSqlCode, 120, argUseTransaction, null);
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
        public T GetDataObject<T>(string argSqlCode, int argTimeout, bool argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            int currentCommand;
            _semaphoreSlim.Wait();
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
                T results = SqlHelperStatic.GetDataObject<T>(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
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
                _semaphoreSlim.Wait();
                try
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(string.Format("[GetDataObject<>] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                // When we don't use transaction, the connection is simply open and there are no other pending commands, we close the connection
                if (!argUseTransaction && _SqlConnection.State == ConnectionState.Open && _SqlCommands.Count == 0)
                {
                    Debug.WriteLine("[GetDataObject<>] Closing connection...");
                    CloseConnection();
                }
            }
        }

        #endregion

        #region "GetClipboardTest"

        public string GetClipboardTextFromValueObject(object argValue)
        {
            return argValue.GetClipboardText();
        }

        public string GetClipboardTextFromValueObject(object argValue, CultureInfo argCultureInfo)
        {
            return argValue.GetClipboardText(argCultureInfo);
        }

        public string GetClipboardText(object argList, CultureInfo argCultureInfo, bool argWithHeaders)
        {
            return argList.GetClipboardText(argCultureInfo, argWithHeaders);
        }

        public string GetClipboardText(object argList, bool argWithHeaders)
        {
            return argList.GetClipboardText(argWithHeaders);
        }

        public string GetClipboardText(object argList, bool argWithHeaders, string argCellSeparator)
        {
            return argList.GetClipboardText(argWithHeaders, argCellSeparator);
        }

        public string GetClipboardText(object argList, CultureInfo argCultureInfo, bool argWithHeaders, string argCellSeparator)
        {
            return argList.GetClipboardText(argCultureInfo, argWithHeaders, argCellSeparator);
        }

        #endregion

        #region "Debug Functions"

        /// <summary>
        /// Returns the current time as a string with format "[dd/MM/yyyy][hh:mm:ss.fff]"
        /// </summary>
        /// <returns></returns>
        public string GetNowString()
        {
            return DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]");
        }

        #endregion
    }
}
