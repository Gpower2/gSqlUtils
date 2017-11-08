using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;

namespace gpower2.gSqlUtils
{
    /// <summary>
    /// This is a wrapper class for SqlHeperStatic class, in order to allow
    /// for persistent SQL Connections, DataReader functionality and easier
    /// consuming from users 
    /// </summary>
    public class gSqlHelper
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

        /// <summary>
        /// A list that contains all the pending SQL Commands' hash codes
        /// </summary>
        private List<Int32> _SqlCommands = new List<Int32>();

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

        public gSqlHelper(String argDataSource, String argInitialCatalog, String argUserId, String argPassword)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, Boolean argPersistSecurityInfo)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argPersistSecurityInfo);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, Boolean argPersistSecurityInfo, 
            Boolean argMultipleActiveResultSets)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argPersistSecurityInfo, argMultipleActiveResultSets);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, String argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argApplicationName);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, String argApplicationName, 
            Boolean argPersistSecurityInfo, Boolean argMultipleActiveResultSets)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argApplicationName, argPersistSecurityInfo, argMultipleActiveResultSets);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, Int32 argConnectTimeout)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argConnectTimeout);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, Int32 argConnectTimeout, String argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argConnectTimeout, argApplicationName);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, Int32 argConnectTimeout, Int32 argPacketSize)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argConnectTimeout, argPacketSize);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, Int32 argConnectTimeout, Int32 argPacketSize, String argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argConnectTimeout, argPacketSize, argApplicationName);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, String argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argApplicationName);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, String argApplicationName, Boolean argMultipleActiveResultSets)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argApplicationName, argMultipleActiveResultSets);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout, String argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, argApplicationName);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout, Int32 argPacketSize)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, argPacketSize);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout, Int32 argPacketSize, String argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, argPacketSize, argApplicationName);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout,
            Boolean argIntegratedSecurity, Int32 argPacketSize, String argUserId, String argPassword, String argApplicationName)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, argIntegratedSecurity, argPacketSize, argUserId, argPassword, argApplicationName, false, false);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout,
            Boolean argIntegratedSecurity, Int32 argPacketSize, String argUserId, String argPassword, String argApplicationName,
            Boolean argMultipleActiveResultSets, Boolean argPersistSecurityInfo)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, argIntegratedSecurity, argPacketSize, argUserId, argPassword, argApplicationName, argMultipleActiveResultSets, argPersistSecurityInfo);
        }

        #endregion

        #region "CloseConnection"

        public void CloseConnection()
        {
            lock (this)
            {
                if (_SqlConnection != null && _SqlConnection.State != System.Data.ConnectionState.Closed && _SqlCommands.Count == 0)
                {
                    _SqlConnection.Close();
                    Debug.WriteLine(String.Format("{0}[CloseConnection] Closed connection...", GetNowString()));
                }
            }
        }

        #endregion

        #region "TransactionHelpers"

        public void BeginTransaction()
        {
            lock (this)
            {
                if (_SqlConnection.State == System.Data.ConnectionState.Closed)
                {
                    _SqlConnection.Open();
                    Debug.WriteLine(String.Format("{0}[BeginTransaction] Opened connection...", GetNowString()));
                }
                _SqlTransaction = _SqlConnection.BeginTransaction();
                Debug.WriteLine(String.Format("{0} Beginned transaction...", GetNowString()));
            }
        }

        public void CommitTransaction()
        {
            lock (this)
            {
                if (_SqlTransaction == null)
                {
                    throw new Exception("There was no transaction to commit!");
                }
                _SqlTransaction.Commit();
                Debug.WriteLine(String.Format("{0}[CommitTransaction] Transaction was committed...", GetNowString()));
                _SqlTransaction = null;
                if (_SqlConnection.State == ConnectionState.Open)
                {
                    CloseConnection();
                }
            }
        }

        public void RollbackTransaction()
        {
            lock (this)
            {
                if (_SqlTransaction == null)
                {
                    throw new Exception("There was no transaction to rollback!");
                }
                _SqlTransaction.Rollback();
                Debug.WriteLine(String.Format("{0}[RollbackTransaction] Transaction was rollbacked...", GetNowString()));
                _SqlTransaction = null;
                if (_SqlConnection.State == ConnectionState.Open)
                {
                    CloseConnection();
                }
            }
        }

        #endregion

        #region "IsNull"

        /// <summary>
        /// It checks if an Object is null or equal to DBNull.Value
        /// and then it returns the user defined Object for that case, 
        /// else it returns the source Object.
        /// </summary>
        /// <param name="argSourceObject">The Object to check</param>
        /// <param name="argNullValue">The Object to return, if the source Object is null or equals to DBNull.Value</param>
        /// <returns></returns>
        public Object IsNull(Object argSourceObject, Object argNullValue)
        {
            return SqlHelperStatic.IsNull(argSourceObject, argNullValue);
        }

        #endregion

        #region "IsNullString"

        /// <summary>
        /// It checks if a String is null and then it returns NULL.
        /// Else, it replaces the escape characters ' and " and 
        /// returns the String single quoted eg. text => 'text'
        /// </summary>
        /// <param name="argSourceString">The source String to check for null</param>
        /// <returns></returns>
        public String IsNullString(String argSourceString)
        {
            return IsNullString(argSourceString, true, false, true);
        }

        /// <summary>
        /// It checks if a String is null and then it returns NULL.
        /// Else, according to user options, it either replaces the
        /// escape characters ' and " or not, it either replaces the
        /// wildcard characters % and _ or not, and it either single
        /// quotes the String or not eg. text => 'text'
        /// </summary>
        /// <param name="argSourceString">The source String to check for null</param>
        /// <param name="argEscapeString">The flag whether to replace the escape characters or not</param>
        /// <param name="argEscapeWildcards">The flag whether to replace the wildcard characters or not</param>
        /// <param name="argQuoteString">The flag whether to single quote the String or not</param>
        /// <returns></returns>
        public String IsNullString(String argSourceString, Boolean argEscapeString, Boolean argEscapeWildcards, Boolean argQuoteString)
        {
            return SqlHelperStatic.IsNullString(argSourceString, argEscapeString, argEscapeWildcards, argQuoteString);
        }

        #endregion

        #region "EscapeString"

        /// <summary>
        /// It escapes the String by replacing ' with ''.
        /// </summary>
        /// <param name="argSourceString"></param>
        /// <returns></returns>
        public String EscapeString(String argSourceString)
        {
            return EscapeString(argSourceString, false);
        }

        /// <summary>
        /// It escapes the String by replacing ' with ''.
        /// It also escapes the wildcard charactes % and _ 
        /// if the user specifies it.
        /// </summary>
        /// <param name="argSourceString">The source String to escape</param>
        /// <param name="argEscapeWildcards">The flag to whether escape the wildcard characters or not</param>
        /// <returns>The escaped String</returns>
        public String EscapeString(String argSourceString, Boolean argEscapeWildcards)
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
        public Int32 ExecuteSql(String argSqlCode)
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
        public Int32 ExecuteSql(String argSqlCode, Int32 argTimeout)
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
        public Int32 ExecuteSql(String argSqlCode, Boolean argUseTransaction)
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
        public Int32 ExecuteSql(String argSqlCode, Int32 argTimeout, Boolean argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            Int32 currentCommand;
            // Add a new entry in the SqlCommands Dictionary
            lock (this)
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
            }
            try
            {
                Int32 resultValue = SqlHelperStatic.ExecuteSql(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
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
                lock (this)
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(String.Format("[ExecuteSql] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
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
        public DataTable GetDataTable(String argSqlCode)
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
        public DataTable GetDataTable(String argSqlCode, Int32 argTimeout)
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
        public DataTable GetDataTable(String argSqlCode, Boolean argUseTransaction)
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
        public DataTable GetDataTable(String argSqlCode, Int32 argTimeout, Boolean argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            Int32 currentCommand;
            lock (this)
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
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
                lock (this)
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(String.Format("[GetDataTable] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
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
        public DataSet GetDataSet(String argSqlCode)
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
        public DataSet GetDataSet(String argSqlCode, Int32 argTimeout)
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
        public DataSet GetDataSet(String argSqlCode, Boolean argUseTransaction)
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
        public DataSet GetDataSet(String argSqlCode, Int32 argTimeout, Boolean argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            Int32 currentCommand;
            lock (this)
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
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
                lock (this)
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(String.Format("[GetDataSet] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
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
        public Object GetDataValue(String argSqlCode)
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
        public Object GetDataValue(String argSqlCode, Int32 argTimeout)
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
        public Object GetDataValue(String argSqlCode, Boolean argUseTransaction)
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
        public Object GetDataValue(String argSqlCode, Int32 argTimeout, Boolean argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            Int32 currentCommand;
            lock (this)
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
            }
            try
            {
                Object resultValue = SqlHelperStatic.GetDataValue(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
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
                lock (this)
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(String.Format("[GetDataValue] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
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
        public T GetDataValue<T>(String argSqlCode) where T : IComparable, IConvertible, IEquatable<T>
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
        public T GetDataValue<T>(String argSqlCode, Int32 argTimeout) where T : IComparable, IConvertible, IEquatable<T>
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
        public T GetDataValue<T>(String argSqlCode, Boolean argUseTransaction) where T : IComparable, IConvertible, IEquatable<T>
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
        public T GetDataValue<T>(String argSqlCode, Int32 argTimeout, Boolean argUseTransaction, List<SqlParameter> argSqlParameters) where T : IComparable, IConvertible, IEquatable<T>
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            Int32 currentCommand;
            lock (this)
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
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
                lock (this)
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(String.Format("[GetDataValue<>] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
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
        public IList GetDataList(Type argObjectType, String argSqlCode)
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
        public IList GetDataList(Type argObjectType, String argSqlCode, Int32 argTimeout)
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
        public IList GetDataList(Type argObjectType, String argSqlCode, Boolean argUseTransaction)
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
        public IList GetDataList(Type argObjectType, String argSqlCode, Int32 argTimeout, Boolean argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            Int32 currentCommand;
            lock (this)
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
            }
            try
            {
                IList results = SqlHelperStatic.GetDataList(argObjectType, argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
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
                lock (this)
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(String.Format("[GetDataList] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
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
        public IList<T> GetDataList<T>(String argSqlCode)
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
        public IList<T> GetDataList<T>(String argSqlCode, Int32 argTimeout)
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
        public IList<T> GetDataList<T>(String argSqlCode, Boolean argUseTransaction)
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
        public IList<T> GetDataList<T>(String argSqlCode, Int32 argTimeout, Boolean argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            Int32 currentCommand;
            lock (this)
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
            }
            try
            {
                IList<T> results = SqlHelperStatic.GetDataList<T>(argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
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
                lock (this)
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(String.Format("[GetDataList<>] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
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
        public T GetDataObject<T>(String argSqlCode)
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
        public T GetDataObject<T>(String argSqlCode, Int32 argTimeout)
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
        public T GetDataObject<T>(String argSqlCode, Boolean argUseTransaction)
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
        public T GetDataObject<T>(String argSqlCode, Int32 argTimeout, Boolean argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            // Add a new entry in the SqlCommands Dictionary
            Int32 currentCommand;
            lock (this)
            {
                currentCommand = DateTime.Now.GetHashCode();
                _SqlCommands.Add(currentCommand);
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
                lock (this)
                {
                    // Remove current entry from the _SqlCommands Dictionary
                    _SqlCommands.Remove(currentCommand);
                    Debug.WriteLine(String.Format("[GetDataObject<>] Remaining SqlCommands Count: {0}", _SqlCommands.Count));
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
        public String GetClipboardTextFromValueObject(Object argValue)
        {
            return ClipboardHelper.GetClipboardTextFromValueObject(argValue);
        }

        public String GetClipboardTextFromValueObject(Object argValue, CultureInfo argCultureInfo)
        {
            return ClipboardHelper.GetClipboardTextFromValueObject(argValue, argCultureInfo);
        }

        public String GetClipboardText(Object argList, CultureInfo argCultureInfo, Boolean argWithHeaders)
        {
            return ClipboardHelper.GetClipboardText(argList, argCultureInfo, argWithHeaders);
        }

        public String GetClipboardText(Object argList, Boolean argWithHeaders) 
        {
            return ClipboardHelper.GetClipboardText(argList, argWithHeaders);
        }

        public String GetClipboardText(Object argList, Boolean argWithHeaders, String argCellSeparator) 
        {
            return ClipboardHelper.GetClipboardText(argList, argWithHeaders, argCellSeparator);
        }

        public String GetClipboardText(Object argList, CultureInfo argCultureInfo, Boolean argWithHeaders, String argCellSeparator) 
        {
            return ClipboardHelper.GetClipboardText(argList, argCultureInfo, argWithHeaders, argCellSeparator);
        }

        public String GetClipboardTextFromIList(IList argList, CultureInfo argCultureInfo, Boolean argWithHeaders, String argCellSeparator) 
        {
            return ClipboardHelper.GetClipboardTextFromIList(argList, argCultureInfo, argWithHeaders, argCellSeparator);
        }

        public String GetClipboardTextFromDataTable(DataTable argDataTable, CultureInfo argCultureInfo, Boolean argWithHeaders, String argCellSeparator) 
        {
            return ClipboardHelper.GetClipboardTextFromDataTable(argDataTable, argCultureInfo, argWithHeaders, argCellSeparator);
        }

        public String GetClipboardTextFromObject(Object argObject, CultureInfo argCultureInfo, String argCellSeparator) 
        {
            return ClipboardHelper.GetClipboardTextFromObject(argObject, argCultureInfo, argCellSeparator);
        }

        #endregion

        #region "Debug Functions"

        /// <summary>
        /// Returns the current time as a String with format "[dd/MM/yyyy][hh:mm:ss.fff]"
        /// </summary>
        /// <returns></returns>
        public String GetNowString()
        {
            return DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]");
        }

        #endregion

    }
}
