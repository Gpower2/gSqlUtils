using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Reflection;

namespace gSqlUtils
{
    /// <summary>
    /// This is a wrapper class for SqlHeperStatic class, in order to allow
    /// for persistent SQL Connections, DataReader functionality and easier
    /// consuming from users 
    /// </summary>
    public class gSqlHelper
    {
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

        public gSqlHelper(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, Int32 argConnectTimeout)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argConnectTimeout);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, Int32 argConnectTimeout, Int32 argPacketSize)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argUserId, argPassword, argConnectTimeout, argPacketSize);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout, Int32 argPacketSize)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, argPacketSize);
        }

        public gSqlHelper(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout,
            Boolean argIntegratedSecurity, Int32 argPacketSize, String argUserId, String argPassword)
        {
            _SqlConnection = SqlHelperStatic.CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, argIntegratedSecurity, argPacketSize, argUserId, argPassword);
        }

        #endregion

        #region "CloseConnection"

        public void CloseConnection()
        {
            if (_SqlConnection.State != System.Data.ConnectionState.Closed)
            {
                _SqlConnection.Close();
            }
        }

        #endregion

        #region "TransactionHelpers"

        public void BeginTransaction()
        {
            if (_SqlConnection.State == System.Data.ConnectionState.Closed)
            {
                _SqlConnection.Open();
            }
            _SqlConnection.BeginTransaction();
        }

        public void CommitTransaction()
        {
            if (_SqlTransaction == null)
            {
                throw new Exception("There was no transaction to commit!");
            }
            _SqlTransaction.Commit();
            _SqlTransaction = null;
        }

        public void RollbackTransaction()
        {
            if (_SqlTransaction == null)
            {
                throw new Exception("There was no transaction to rollback!");
            }
            _SqlTransaction.Rollback();
            _SqlTransaction = null;
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
        /// </summary>
        /// <param name="argObjectType">The object Type to map the data to</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argUseTransaction"></param>
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
        /// </summary>
        /// <param name="argObjectType">The object Type to map the data to</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argUseTransaction"></param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns>The List of objects filled with data</returns>
        public IList GetDataList(Type argObjectType, String argSqlCode, Int32 argTimeout, Boolean argUseTransaction, List<SqlParameter> argSqlParameters)
        {
            _LastOperationException = null;
            _LastOperationTimeSpan = new TimeSpan();
            try
            {
                IList results = SqlHelperStatic.GetDataList(argObjectType, argSqlCode, _SqlConnection, argTimeout, argUseTransaction ? _SqlTransaction : null, argSqlParameters);
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
            }
        }

        #endregion
    }
}
