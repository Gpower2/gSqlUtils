using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace gpower2.gSqlUtils
{
    /// <summary>
    /// A static class that provides methods for CRUD operations on MS SQL Server
    /// and also some helper functions for creating SQL statements.
    /// All the CRUD methods require an already initialized SQL connection.
    /// All the CRUD methods have a stopwatch for timing the excecution time, 
    /// but the results are shown only in Debug builds. The last operation's
    /// ellapsed time can be accessed through the public static property:
    /// LastOperationEllapsedTime
    /// All the CRUD methods support SQL transactions to be passed as arguments,
    /// in order to use it from outside environments in multiple calls.
    /// </summary>
    public static class SqlHelperStatic
	{
		#region "Properties"
		
		/// <summary>
		/// Returns last CRUD operation's ellapsed time
		/// </summary>
		public static TimeSpan LastOperationEllapsedTime
		{
			get { return _LastOperationEllapsedTime; }
		}

		/// <summary>
		/// Return last CRUD operation's exception
		/// If no exception occured, it returns null
		/// </summary>
		public static Exception LastOperationException
		{
			get { return _LastOperationException; }
		}

		#endregion

		#region "Private members"

		private static TimeSpan _LastOperationEllapsedTime;
		private static Exception _LastOperationException;

        private static string _ThreadAnchor = "ThreadAnchor";

		#endregion

		#region "ExecuteSql"

		/// <summary>
		/// Executes a non scalar SQL statement.
		/// It sets a default timeout of 120 seconds and doesn't use a transaction.
		/// It returns the number of rows affected.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <returns>The number of rows affected</returns>
		public static Int32 ExecuteSql(String argSqlCode, SqlConnection argSqlCon)
		{
			return ExecuteSql(argSqlCode, argSqlCon, 120, null, null);
		}

		/// <summary>
		/// Executes a non scalar SQL statement.
		/// It doesn't use a transaction.
		/// It returns the number of rows affected.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <param name="argTimeout">The timeout for the SQL command in seconds</param>
		/// <returns>The number of rows affected</returns>
		public static Int32 ExecuteSql(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout)
		{
			return ExecuteSql(argSqlCode, argSqlCon, argTimeout, null, null);
		}

		/// <summary>
		/// Executes a non scalar SQL statement.
		/// It sets a default timeout of 120 seconds.
		/// It returns the number of rows affected.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <param name="argSqlTransaction">The SQL transaction to use</param>
		/// <returns>The number of rows affected</returns>
		public static Int32 ExecuteSql(String argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction)
		{
			return ExecuteSql(argSqlCode, argSqlCon, 120, argSqlTransaction, null);
		}

		/// <summary>
		/// Executes a non scalar SQL statement.
		/// It returns the number of rows affected.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <param name="argTimeout">The timeout for the SQL command in seconds</param>
		/// <param name="argSqlTransaction">The SQL transaction to use</param>
		/// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
		/// <returns>The number of rows affected</returns>
		public static Int32 ExecuteSql(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters)
		{
			Stopwatch myStopWatch = new Stopwatch();
			Int32 rowsAffected = 0;
			_LastOperationException = null;
			try
			{
				if (argSqlCon == null)
				{
					throw new Exception("Null SQL connection!");
				}
				if (String.IsNullOrWhiteSpace(argSqlCode))
				{
					throw new Exception("Empty SQL code!");
				}
                lock (_ThreadAnchor)
                {
                    if (argSqlCon.State == System.Data.ConnectionState.Closed)
                    {
                        argSqlCon.Open();
                        Debug.WriteLine(String.Format("{0}[ExecuteSql] Opened connection...", GetNowString()));
                    }
                    // Wait for the connection to finish connecting
                    while (argSqlCon.State == ConnectionState.Connecting) { }
                }
                using (SqlCommand sqlCmd = new SqlCommand(argSqlCode, argSqlCon))
				{
					if (argSqlTransaction != null)
					{
						sqlCmd.Transaction = argSqlTransaction;
					}
					// if user supplied with command parameters, add them to the select command
					if (argSqlParameters != null)
					{
						foreach (SqlParameter sqlParam in argSqlParameters)
						{
							sqlCmd.Parameters.Add(sqlParam);
						}
					}
					sqlCmd.CommandTimeout = argTimeout;
                    Debug.WriteLine(String.Format("{0}[ExecuteSql] Starting to execute SQL code:", GetNowString()));
					Debug.WriteLine(GetSQLCommandString(sqlCmd));
					myStopWatch.Start();
					rowsAffected = sqlCmd.ExecuteNonQuery();
					myStopWatch.Stop();
                    _LastOperationEllapsedTime = myStopWatch.Elapsed;
                    Debug.WriteLine(String.Format("{0}[ExecuteSql] Finished executing SQL code (duration: {1})", GetNowString(), myStopWatch.Elapsed));
					return rowsAffected;
				}
			}
			catch (Exception ex)
			{
				myStopWatch.Stop();
                _LastOperationEllapsedTime = myStopWatch.Elapsed;
                _LastOperationException = ex;
                Debug.WriteLine(String.Format("{0}[ExecuteSql] Error executing SQL code! (duration: {1})", GetNowString(), myStopWatch.Elapsed));
				Debug.WriteLine(ex);
				throw;
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
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <returns>The DataTable that contains the results</returns>
		public static DataTable GetDataTable(String argSqlCode, SqlConnection argSqlCon)
		{
			return GetDataTable(argSqlCode, argSqlCon, 120, null, null);
		}

		/// <summary>
		/// Returns a DataTable that contains the results of the execution of an SQL statement.
		/// If the results are a DataSet, then only the first DataTable is returned.
		/// It doesn't use a transaction.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <param name="argTimeout">The timeout for the SQL command in seconds</param>
		/// <returns>The DataTable that contains the results</returns>
		public static DataTable GetDataTable(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout)
		{
			return GetDataTable(argSqlCode, argSqlCon, argTimeout, null, null);
		}

		/// <summary>
		/// Returns a DataTable that contains the results of the execution of an SQL statement.
		/// If the results are a DataSet, then only the first DataTable is returned.
		/// It sets a default timeout of 120 seconds.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <param name="argSqlTransaction">The SQL transaction to use</param>
		/// <returns>The DataTable that contains the results</returns>
		public static DataTable GetDataTable(String argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction)
		{
			return GetDataTable(argSqlCode, argSqlCon, 120, argSqlTransaction, null);
		}

		/// <summary>
		/// Returns a DataTable that contains the results of the execution of an SQL statement.
		/// If the results are a DataSet, then only the first DataTable is returned.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <param name="argTimeout">The timeout for the SQL command in seconds</param>
		/// <param name="argSqlTransaction">The SQL transaction to use</param>
		/// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
		/// <returns>The DataTable that contains the results</returns>
		public static DataTable GetDataTable(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters)
		{
			DataTable myDatatable = null;
			Stopwatch myStopWatch = new Stopwatch();
			_LastOperationException = null;
			try
			{
				if (argSqlCon == null)
				{
					throw new Exception("Null SQL connection!");
				}
                if (String.IsNullOrWhiteSpace(argSqlCode))
                {
                    throw new Exception("Empty SQL code!");
				}
                lock (_ThreadAnchor)
                {
                    if (argSqlCon.State == System.Data.ConnectionState.Closed)
                    {
                        argSqlCon.Open();
                        Debug.WriteLine(String.Format("{0}[GetDataTable] Opened connection...", GetNowString()));
                    }
                    // Wait for the connection to finish connecting
                    while (argSqlCon.State == ConnectionState.Connecting) { }
                }

                myDatatable = new DataTable();
				using (SqlDataAdapter myAdapter = new SqlDataAdapter(argSqlCode, argSqlCon))
				{
					myAdapter.SelectCommand.CommandTimeout = argTimeout;
					// if user supplied a transaction, use it
					if (argSqlTransaction != null)
					{
						myAdapter.SelectCommand.Transaction = argSqlTransaction;
					}
					// if user supplied with command parameters, add them to the select command
					if (argSqlParameters != null)
					{
						foreach (SqlParameter sqlParam in argSqlParameters)
						{
							myAdapter.SelectCommand.Parameters.Add(sqlParam);
						}
					}
                    Debug.WriteLine(String.Format("{0}[GetDataTable] Starting to execute SQL code:", GetNowString()));
					Debug.WriteLine(GetSQLCommandString(myAdapter.SelectCommand));
					myStopWatch.Start();
					myAdapter.Fill(myDatatable);
					myStopWatch.Stop();
                    _LastOperationEllapsedTime = myStopWatch.Elapsed;
                    Debug.WriteLine(String.Format("{0}[GetDataTable] Finished executing SQL code (duration: {1})", GetNowString(), myStopWatch.Elapsed));
				}
				return myDatatable;
			}
			catch (Exception ex)
			{
				myStopWatch.Stop();
                _LastOperationEllapsedTime = myStopWatch.Elapsed;
                _LastOperationException = ex;
                Debug.WriteLine(String.Format("{0}[GetDataTable] Error executing SQL code! (duration: {1})", GetNowString(), myStopWatch.Elapsed));
				if (myDatatable != null)
				{
					myDatatable.Dispose();
					myDatatable = null;
				}
				Debug.WriteLine(ex);
				throw;
			}
		}

		#endregion

		#region "GetDataSet"

		/// <summary>
		/// Returns a DataSet that contains the results of the execution of an SQL statement.
		/// It sets a default timeout of 120 seconds and doesn't use a transaction.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <returns>The DataSet that contains the results</returns>
		public static DataSet GetDataSet(String argSqlCode, SqlConnection argSqlCon)
		{
			return GetDataSet(argSqlCode, argSqlCon, 120, null, null);
		}

		/// <summary>
		/// Returns a DataSet that contains the results of the execution of an SQL statement.
		/// It doesn't use a transaction.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <param name="argTimeout">The timeout for the SQL command in seconds</param>
		/// <returns>The DataSet that contains the results</returns>
		public static DataSet GetDataSet(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout)
		{
			return GetDataSet(argSqlCode, argSqlCon, argTimeout, null, null);
		}

		/// <summary>
		/// Returns a DataSet that contains the results of the execution of an SQL statement.
		/// It sets a default timeout of 120 seconds.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <param name="argSqlTransaction">The SQL transaction to use</param>
		/// <returns>The DataSet that contains the results</returns>
		public static DataSet GetDataSet(String argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction)
		{
			return GetDataSet(argSqlCode, argSqlCon, 120, argSqlTransaction, null);
		}

		/// <summary>
		/// Returns a DataSet that contains the results of the execution of an SQL statement.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <param name="argTimeout">The timeout for the SQL command in seconds</param>
		/// <param name="argSqlTransaction">The SQL transaction to use</param>
		/// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
		/// <returns>The DataSet that contains the results</returns>
		public static DataSet GetDataSet(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters)
		{
			DataSet myDataset = null;
			Stopwatch myStopWatch = new Stopwatch();
			_LastOperationException = null;
			try
			{
				if (argSqlCon == null)
				{
					throw new Exception("Null SQL connection!");
				}
                if (String.IsNullOrWhiteSpace(argSqlCode))
                {
                    throw new Exception("Empty SQL code!");
				}
                lock (_ThreadAnchor)
                {
                    if (argSqlCon.State == System.Data.ConnectionState.Closed)
                    {
                        argSqlCon.Open();
                        Debug.WriteLine(String.Format("{0}[GetDataSet] Opened connection...", GetNowString()));
                    }
                    // Wait for the connection to finish connecting
                    while (argSqlCon.State == ConnectionState.Connecting) { }
                }
                myDataset = new DataSet();
				using (SqlDataAdapter myAdapter = new SqlDataAdapter(argSqlCode, argSqlCon))
				{
					myAdapter.SelectCommand.CommandTimeout = argTimeout;
					if (argSqlTransaction != null)
					{
						myAdapter.SelectCommand.Transaction = argSqlTransaction;
					}
					// if user supplied with command parameters, add them to the select command
					if (argSqlParameters != null)
					{
						foreach (SqlParameter sqlParam in argSqlParameters)
						{
							myAdapter.SelectCommand.Parameters.Add(sqlParam);
						}
					}
                    Debug.WriteLine(String.Format("{0}[GetDataSet] Starting to execute SQL code:", GetNowString()));
					Debug.WriteLine(GetSQLCommandString(myAdapter.SelectCommand));
					myStopWatch.Start();
					myAdapter.Fill(myDataset);
					myStopWatch.Stop();
                    _LastOperationEllapsedTime = myStopWatch.Elapsed;
                    Debug.WriteLine(String.Format("{0}[GetDataSet] Finished executing SQL code (duration: {1})", GetNowString(), myStopWatch.Elapsed));
				}
				return myDataset;
			}
			catch (Exception ex)
			{
				myStopWatch.Stop();
                _LastOperationEllapsedTime = myStopWatch.Elapsed;
                _LastOperationException = ex;
                Debug.WriteLine(String.Format("{0}[GetDataSet] Error executing SQL code! (duration: {1})", GetNowString(), myStopWatch.Elapsed));
				if (myDataset != null)
				{
					myDataset.Dispose();
					myDataset = null;
				}
				Debug.WriteLine(ex);
				throw;
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
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <returns></returns>
		public static Object GetDataValue(String argSqlCode, SqlConnection argSqlCon)
		{
			return GetDataValue(argSqlCode, argSqlCon, 120, null, null);
		}

		/// <summary>
		/// Returns the value of the first cell of the first DataTable from the results
		/// of the execution of an SQL statement.
		/// It doesn't use a transaction.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <param name="argTimeout">The timeout for the SQL command in seconds</param>
		/// <returns></returns>
		public static Object GetDataValue(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout)
		{
			return GetDataValue(argSqlCode, argSqlCon, argTimeout, null, null);
		}

		/// <summary>
		/// Returns the value of the first cell of the first DataTable from the results
		/// of the execution of an SQL statement.
		/// It sets a default timeout of 120 seconds.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <param name="argSqlTransaction">The SQL transaction to use</param>
		/// <returns></returns>
		public static Object GetDataValue(String argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction)
		{
			return GetDataValue(argSqlCode, argSqlCon, 120, argSqlTransaction, null);
		}

		/// <summary>
		/// Returns the value of the first cell of the first DataTable from the results
		/// of the execution of an SQL statement.
		/// </summary>
		/// <param name="argSqlCode">The SQL code to execute</param>
		/// <param name="argSqlCon">The SQL connection to use</param>
		/// <param name="argTimeout">The timeout for the SQL command in seconds</param>
		/// <param name="argSqlTransaction">The SQL transaction to use</param>
		/// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
		/// <returns></returns>
		public static Object GetDataValue(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters)
		{
			Stopwatch myStopWatch = new Stopwatch();
			_LastOperationException = null;
			try
			{
				if (argSqlCon == null)
				{
					throw new Exception("Null SQL connection!");
				}
                if (String.IsNullOrWhiteSpace(argSqlCode))
                {
                    throw new Exception("Empty SQL code!");
				}
                lock (_ThreadAnchor)
                {
                    if (argSqlCon.State == System.Data.ConnectionState.Closed)
                    {
                        argSqlCon.Open();
                        Debug.WriteLine(String.Format("{0}[GetDataValue] Opened connection...", GetNowString()));
                    }
                    // Wait for the connection to finish connecting
                    while (argSqlCon.State == ConnectionState.Connecting) { }
                }
                using (SqlCommand myCommand = new SqlCommand(argSqlCode, argSqlCon))
				{
					myCommand.CommandTimeout = argTimeout;
					if (argSqlTransaction != null)
					{
						myCommand.Transaction = argSqlTransaction;
					}
					// if user supplied with command parameters, add them to the select command
					if (argSqlParameters != null)
					{
						foreach (SqlParameter sqlParam in argSqlParameters)
						{
							myCommand.Parameters.Add(sqlParam);
						}
					}
                    Debug.WriteLine(String.Format("{0}[GetDataValue] Starting to execute SQL code:", GetNowString()));
					Debug.WriteLine(GetSQLCommandString(myCommand));
					myStopWatch.Start();
					Object resultObject = myCommand.ExecuteScalar();
					myStopWatch.Stop();
                    _LastOperationEllapsedTime = myStopWatch.Elapsed;
                    Debug.WriteLine(String.Format("{0}[GetDataValue] Finished executing SQL code (duration: {1})", GetNowString(), myStopWatch.Elapsed));
					return resultObject;
				}
			}
			catch (Exception ex)
			{
				myStopWatch.Stop();
                _LastOperationEllapsedTime = myStopWatch.Elapsed;
                _LastOperationException = ex;
                Debug.WriteLine(String.Format("{0}[GetDataValue] Error executing SQL code! (duration: {1})", GetNowString(), myStopWatch.Elapsed));
				Debug.WriteLine(ex);
				throw;
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
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <returns>If the result is DBNull, then it returns the default value for the Type provided</returns>
        public static T GetDataValue<T>(String argSqlCode, SqlConnection argSqlCon) where T : IComparable, IConvertible, IEquatable<T>
        {
            return GetDataValue<T>(argSqlCode, argSqlCon, 120, null, null);
        }

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>If the result is DBNull, then it returns the default value for the Type provided</returns>
        public static T GetDataValue<T>(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout) where T : IComparable, IConvertible, IEquatable<T>
        {
            return GetDataValue<T>(argSqlCode, argSqlCon, argTimeout, null, null);
        }

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argSqlTransaction">The SQL transaction to use</param>
        /// <returns>If the result is DBNull, then it returns the default value for the Type provided</returns>
        public static T GetDataValue<T>(String argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction) where T : IComparable, IConvertible, IEquatable<T>
        {
            return GetDataValue<T>(argSqlCode, argSqlCon, 120, argSqlTransaction, null);
        }

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argSqlTransaction">The SQL transaction to use</param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns>If the result is DBNull, then it returns the default value for the Type provided</returns>
        public static T GetDataValue<T>(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters) where T : IComparable, IConvertible, IEquatable<T>
        {
            Stopwatch myStopWatch = new Stopwatch();
            _LastOperationException = null;
            try
            {
                if (argSqlCon == null)
                {
                    throw new Exception("Null SQL connection!");
                }
                if (String.IsNullOrWhiteSpace(argSqlCode))
                {
                    throw new Exception("Empty SQL code!");
                }
                lock (_ThreadAnchor)
                {
                    if (argSqlCon.State == System.Data.ConnectionState.Closed)
                    {
                        argSqlCon.Open();
                        Debug.WriteLine(String.Format("{0}[GetDataValue<>] Opened connection...", GetNowString()));
                    }
                    // Wait for the connection to finish connecting
                    while (argSqlCon.State == ConnectionState.Connecting) { }
                }
                using (SqlCommand myCommand = new SqlCommand(argSqlCode, argSqlCon))
                {
                    myCommand.CommandTimeout = argTimeout;
                    if (argSqlTransaction != null)
                    {
                        myCommand.Transaction = argSqlTransaction;
                    }
                    // if user supplied with command parameters, add them to the select command
                    if (argSqlParameters != null)
                    {
                        foreach (SqlParameter sqlParam in argSqlParameters)
                        {
                            myCommand.Parameters.Add(sqlParam);
                        }
                    }
                    Debug.WriteLine(String.Format("{0}[GetDataValue<>] Starting to execute SQL code:", GetNowString()));
                    Debug.WriteLine(GetSQLCommandString(myCommand));
                    myStopWatch.Start();
                    // Excecute the SQL command
                    Object res = myCommand.ExecuteScalar();
                    myStopWatch.Stop();
                    _LastOperationEllapsedTime = myStopWatch.Elapsed;
                    Debug.WriteLine(String.Format("{0}[GetDataValue<>] Finished executing SQL code (duration: {1})", GetNowString(), myStopWatch.Elapsed));

                    // Check for DBNull
                    if (res == DBNull.Value)
                    {
                        // If DBNull, then return default value for T
                        return default(T);
                    }
                    else
                    {
                        // Check for Nullable<T> properties
                        if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(Nullable<>))
                        {
                            // If the type is Nullable<T> we change the value to the Nullable<T> equivalent
                            return (T)Convert.ChangeType(res, Nullable.GetUnderlyingType(typeof(T)));
                        }
                        else
                        {
                            return (T)Convert.ChangeType(res, typeof(T));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                myStopWatch.Stop();
                _LastOperationEllapsedTime = myStopWatch.Elapsed;
                _LastOperationException = ex;
                Debug.WriteLine(String.Format("{0}[GetDataValue<>] Error executing SQL code! (duration: {1})", GetNowString(), myStopWatch.Elapsed));
                Debug.WriteLine(ex);
                throw;
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
        /// <param name="argObjectType">The Type of the objects contained in the returned list</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <returns>The List of objects filled with data</returns>
        public static IList GetDataList(Type argObjectType, String argSqlCode, SqlConnection argSqlCon)
        {
            return GetDataList(argObjectType, argSqlCode, argSqlCon, 120, null, null);
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
        /// <param name="argObjectType">The Type of the objects contained in the returned list</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>The List of objects filled with data</returns>
        public static IList GetDataList(Type argObjectType, String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout)
        {
            return GetDataList(argObjectType, argSqlCode, argSqlCon, argTimeout, null, null);
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
        /// <param name="argObjectType">The Type of the objects contained in the returned list</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argSqlTransaction">The SQL transaction to use</param>
        /// <returns>The List of objects filled with data</returns>
        public static IList GetDataList(Type argObjectType, String argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction)
        {
            return GetDataList(argObjectType, argSqlCode, argSqlCon, 120, argSqlTransaction, null);
        }

        /// <summary>
        /// Fills a Dictionary that maps object properties to column index of a DataReader
        /// </summary>
        /// <param name="myReader"></param>
        /// <param name="mapDict"></param>
        /// <param name="objectProperties"></param>
        /// <param name="argRootPropertyName"></param>
        private static void FillMap(SqlDataReader myReader, Dictionary<PropertyInfo, Int32> mapDict, PropertyInfo[] objectProperties, String argRootPropertyName = "")
        {
            // for each property of the object, try to find a column of the same name
            foreach (PropertyInfo myProp in objectProperties)
            {
                // Only for properties that can be written to
                if (myProp.CanWrite)
                {
                    // Check for reference type
                    if (!myProp.PropertyType.IsValueType && myProp.PropertyType != typeof(String)
                        && !myProp.PropertyType.IsArray
                        && !(myProp.PropertyType.IsGenericType && myProp.PropertyType.GetGenericTypeDefinition() != typeof(Nullable<>)))
                    {
                        // Check if we have a property of the saqme declaring type
                        // In that case, we only support one depth level
                        if (myProp.PropertyType == myProp.DeclaringType && NumberOfOccurences(argRootPropertyName, ".") > 1)
                        {
                            continue;
                        }
                        // If reference type then try to find if we have columns for its properties
                        FillMap(myReader, mapDict, myProp.PropertyType.GetProperties(),
                            String.IsNullOrWhiteSpace(argRootPropertyName) ? myProp.Name : string.Format("{0}.{1}", argRootPropertyName, myProp.Name));
                        continue;
                    }

                    // try to find a column with the same property name
                    // Remove '_' character from column name
                    // Make the comparison case insensitive
                    for (Int32 curColumn = 0; curColumn < myReader.FieldCount; curColumn++)
                    {
                        // Check if column is already mapped
                        if (mapDict.ContainsValue(curColumn))
                        {
                            // continue to next column
                            continue;
                        }

                        // check column name with property name
                        if (myReader.GetName(curColumn).Replace("_", "").Replace(" ", "").Trim().ToLower().Equals( // Column Name
                            (String.IsNullOrWhiteSpace(argRootPropertyName) ? myProp.Name : string.Format("{0}_{1}", argRootPropertyName, myProp.Name)).Replace("_", "").ToLower()) // Property Name
                            )
                        {
                            // Add the map entry
                            mapDict.Add(myProp, curColumn);
                            // Exit the loop
                            break;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argProp"></param>
        /// <param name="argObject"></param>
        /// <param name="argValue"></param>
        private static void SetPropertyValueToObject(PropertyInfo argProp, Object argObject, Object argValue)
        {
            // Check for Nullable<T> properties
            if (argProp.PropertyType.IsGenericType && argProp.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                // If the type is Nullable<T> we change the value to the Nullable<T> equivalent
                argProp.SetValue(argObject, argValue == DBNull.Value ? null : Convert.ChangeType(argValue,
                    Nullable.GetUnderlyingType(argProp.PropertyType)), null);
            }
            else if (argProp.PropertyType.IsValueType)
            {
                // if type is value type, then it doesn't allow null, so we get the default value by using Activator
                argProp.SetValue(argObject, argValue == DBNull.Value ? Activator.CreateInstance(argProp.PropertyType) : Convert.ChangeType(argValue,
                    argProp.PropertyType), null);
            }
            else
            {
                // if type is not a value type and not a nullable type, we can assign null
                argProp.SetValue(argObject, argValue == DBNull.Value ? null : Convert.ChangeType(argValue, argProp.PropertyType), null);
            }
        }

        /// <summary>
        /// Returns a List of objects of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns an empty List
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argObjectType">The Type of the objects contained in the returned list</param>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argSqlTransaction">The SQL transaction to use</param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns>The List of objects filled with data</returns>
        public static IList GetDataList(Type argObjectType, String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters)
        {
            Stopwatch myStopWatch = new Stopwatch();
            _LastOperationException = null;
            try
            {
                // check for null connection
                if (argSqlCon == null)
                {
                    throw new Exception("Null SQL connection!");
                }
                // check for empty SQL query
                if (String.IsNullOrWhiteSpace(argSqlCode))
                {
                    throw new Exception("Empty SQL query!");
                }
                // Get the List<T> type for our object type
                Type genericListType = typeof(List<>).MakeGenericType(argObjectType);
                // Instantiate the List<T>
                IList objectList = (IList)Activator.CreateInstance(genericListType);
                lock (_ThreadAnchor)
                {                    
                    // Open the SQL connection in case it's closed
                    if (argSqlCon.State == System.Data.ConnectionState.Closed)
                    {
                        argSqlCon.Open();
                        Debug.WriteLine(String.Format("{0}[GetDataList] Opened connection...", GetNowString()));
                    }
                    // Wait for the connection to finish connecting
                    while (argSqlCon.State == ConnectionState.Connecting) { }
                }
                // Create the SQL command from the SQL query using object's connection
                using (SqlCommand sqlCmd = new SqlCommand(argSqlCode, argSqlCon))
                {
                    // Set the SQL command timeout
                    sqlCmd.CommandTimeout = argTimeout;
                    // Check if transaction is needed
                    if (argSqlTransaction != null)
                    {
                        sqlCmd.Transaction = argSqlTransaction;
                    }
                    // if user supplied with command parameters, add them to the select command
                    if (argSqlParameters != null)
                    {
                        foreach (SqlParameter sqlParam in argSqlParameters)
                        {
                            sqlCmd.Parameters.Add(sqlParam);
                        }
                    }
                    Debug.WriteLine(String.Format("{0}[GetDataList] Starting to execute SQL code:", GetNowString()));
                    Debug.WriteLine(GetSQLCommandString(sqlCmd));
                    myStopWatch.Start();
                    // Create the DataReader from our command
                    using (SqlDataReader myReader = sqlCmd.ExecuteReader())
                    {
                        // Check if there are rows
                        if (myReader.HasRows)
                        {
                            // Check if the Type requested is ValueType
                            if (argObjectType.IsValueType)
                            {
                                // If we have a Value Type, we don't need to create a properties map
                                // Begin reading
                                while (myReader.Read())
                                {
                                    // Instantiate a new object for filling it from datarow
                                    Object myObject = Activator.CreateInstance(argObjectType);

                                    // If we have a Value Type, then use the first column
                                    Object cellValue = myReader.GetValue(0);

                                    // Check for Nullable<T> properties
                                    if (argObjectType.IsGenericType && argObjectType.GetGenericTypeDefinition() == typeof(Nullable<>))
                                    {
                                        // If the type is Nullable<T> we change the value to the Nullable<T> equivalent
                                        myObject = (cellValue == DBNull.Value ? null : Convert.ChangeType(cellValue, Nullable.GetUnderlyingType(argObjectType)));
                                    }
                                    else
                                    {
                                        // If the type is a not Nullable Value Type, we use the default value when we have DBNull.Value
                                        myObject = (cellValue == DBNull.Value ? Activator.CreateInstance(argObjectType) : Convert.ChangeType(cellValue, argObjectType));
                                    }

                                    // Add the object to the list
                                    objectList.Add(myObject);
                                }
                            }
                            else
                            {
                                // If we have a Reference Type, then create the properties Map
                                // Make a map for properties <-> columns
                                Dictionary<PropertyInfo, Int32> mapDict = new Dictionary<PropertyInfo, Int32>();
                                Boolean mapCreated = false;

                                // Get the properties for the object
                                PropertyInfo[] objectProperties = argObjectType.GetProperties();

                                // Begin reading
                                while (myReader.Read())
                                {
                                    // Check if map is created
                                    if (!mapCreated)
                                    {
                                        FillMap(myReader, mapDict, objectProperties);
                                        mapCreated = true;
                                    }

                                    // Instantiate a new object for filling it from datarow
                                    Object rootObject = Activator.CreateInstance(argObjectType);
                                    Object curObject = rootObject;

                                    // If we have a Reference Type, use the properties Map to make the assignments
                                    // Make the assignment
                                    foreach (PropertyInfo mapProp in mapDict.Keys)
                                    {
                                        Object cellValue = myReader.GetValue(mapDict[mapProp]);

                                        if (mapProp.DeclaringType != argObjectType)
                                        {
                                            // Try to find the root object's property with the declaring type of the property
                                            PropertyInfo declareObjectProperty = objectProperties.FirstOrDefault(x => x.PropertyType == mapProp.DeclaringType);
                                            if (declareObjectProperty != null)
                                            {
                                                // If property was found, get it or instantiate it
                                                Object declareObject = declareObjectProperty.GetValue(rootObject, null);
                                                if (declareObject == null)
                                                {
                                                    declareObject = Activator.CreateInstance(mapProp.DeclaringType);
                                                    // Set the value to the property of the newly created object
                                                    SetPropertyValueToObject(declareObjectProperty, rootObject, declareObject);
                                                }

                                                curObject = declareObject;
                                            }
                                            else
                                            {
                                                // if we couldn't find the property, then ignore the cell value
                                                continue;
                                            }
                                        }
                                        else
                                        {
                                            curObject = rootObject;
                                        }

                                        SetPropertyValueToObject(mapProp, curObject, cellValue);
                                    }

                                    // Add the object to the list
                                    objectList.Add(rootObject);
                                }
                            }
                        }
                    }
                }
                myStopWatch.Stop();
                _LastOperationEllapsedTime = myStopWatch.Elapsed;
                Debug.WriteLine(String.Format("{0}[GetDataList] Finished executing SQL code (duration: {1})", GetNowString(), myStopWatch.Elapsed));
                // Return our list
                return objectList;
            }
            catch (Exception ex)
            {
                myStopWatch.Stop();
                _LastOperationEllapsedTime = myStopWatch.Elapsed;
                _LastOperationException = ex;
                Debug.WriteLine(String.Format("{0}[GetDataList] Error executing SQL code! (duration: {1})", GetNowString(), myStopWatch.Elapsed));
                Debug.WriteLine(ex);
                throw;
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
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <returns>The List of objects filled with data</returns>
        public static IList<T> GetDataList<T>(String argSqlCode, SqlConnection argSqlCon)
        {
            return GetDataList<T>(argSqlCode, argSqlCon, 120, null, null);
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
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>The List of objects filled with data</returns>
        public static IList<T> GetDataList<T>(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout)
        {
            return GetDataList<T>(argSqlCode, argSqlCon, argTimeout, null, null);
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
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argSqlTransaction">The SQL transaction to use</param>
        /// <returns>The List of objects filled with data</returns>
        public static IList<T> GetDataList<T>(String argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction)
        {
            return GetDataList<T>(argSqlCode, argSqlCon, 120, argSqlTransaction, null);
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
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argSqlTransaction">The SQL transaction to use</param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns>The List of objects filled with data</returns>
        public static IList<T> GetDataList<T>(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters)
        {
            // Return our list
            return (IList<T>)GetDataList(typeof(T), argSqlCode, argSqlCon, argTimeout, argSqlTransaction, argSqlParameters);
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
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <returns>The List of objects filled with data</returns>
        public static T GetDataObject<T>(String argSqlCode, SqlConnection argSqlCon)
        {
            return GetDataObject<T>(argSqlCode, argSqlCon, 120, null, null);
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
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>The List of objects filled with data</returns>
        public static T GetDataObject<T>(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout)
        {
            return GetDataObject<T>(argSqlCode, argSqlCon, argTimeout, null, null);
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
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argSqlTransaction">The SQL transaction to use</param>
        /// <returns>The List of objects filled with data</returns>
        public static T GetDataObject<T>(String argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction)
        {
            return GetDataObject<T>(argSqlCode, argSqlCon, 120, argSqlTransaction, null);
        }

        /// <summary>
        /// Returns a single object of the type specified, containing the data from 
        /// executing the SQL code provided.
        /// It maps each column name from the dataset to the same named property of the
        /// object, replacing '_' character in column name using case insensitive comparison.
        /// If the results are empty, it returns the default object
        /// WARNING! DBNull is mapped to null!
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <param name="argSqlTransaction">The SQL transaction to use</param>
        /// <param name="argSqlParameters">The SQL Parameters for the SQL command</param>
        /// <returns>The List of objects filled with data</returns>
        public static T GetDataObject<T>(String argSqlCode, SqlConnection argSqlCon, Int32 argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters)
        {
            Stopwatch myStopWatch = new Stopwatch();
            _LastOperationException = null;
            try
            {
                // check for null connection
                if (argSqlCon == null)
                {
                    throw new Exception("Null SQL connection!");
                }
                // check for empty SQL query
                if (String.IsNullOrWhiteSpace(argSqlCode))
                {
                    throw new Exception("Empty SQL query!");
                }
                lock (_ThreadAnchor)
                {
                    // Open the SQL connection in case it's closed
                    if (argSqlCon.State == System.Data.ConnectionState.Closed)
                    {
                        argSqlCon.Open();
                        Debug.WriteLine(String.Format("{0}[GetDataObject<>] Opened connection...", GetNowString()));
                    }
                    // Wait for the connection to finish connecting
                    while (argSqlCon.State == ConnectionState.Connecting) { }
                }
                // Instantiate a new object for filling it from datarow
                T myObject = default(T);
                // Create the SQL command from the SQL query using object's connection
                using (SqlCommand sqlCmd = new SqlCommand(argSqlCode, argSqlCon))
                {
                    // Set the SQL command timeout
                    sqlCmd.CommandTimeout = argTimeout;
                    // Check if transaction is needed
                    if (argSqlTransaction != null)
                    {
                        sqlCmd.Transaction = argSqlTransaction;
                    }
                    // if user supplied with command parameters, add them to the select command
                    if (argSqlParameters != null)
                    {
                        foreach (SqlParameter sqlParam in argSqlParameters)
                        {
                            sqlCmd.Parameters.Add(sqlParam);
                        }
                    }
                    Debug.WriteLine(String.Format("{0}[GetDataObject<>] Starting to execute SQL code:", GetNowString()));
                    Debug.WriteLine(GetSQLCommandString(sqlCmd));
                    myStopWatch.Start();
                    // Create the DataReader from our command
                    using (SqlDataReader myReader = sqlCmd.ExecuteReader())
                    {
                        // Check if there are rows
                        if (myReader.HasRows)
                        {
                            // Read the first row
                            if (myReader.Read())
                            {
                                // Instantiate the object
                                myObject = (T)Activator.CreateInstance(typeof(T));

                                // Check if the Type requested is ValueType
                                if (typeof(T).IsValueType)
                                {
                                    // If we have a Value Type, then use the first column
                                    Object cellValue = myReader.GetValue(0);

                                    // Check for Nullable<T> properties
                                    if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(Nullable<>))
                                    {
                                        // If the type is Nullable<T> we change the value to the Nullable<T> equivalent
                                        myObject = (T)(cellValue == DBNull.Value ? null : Convert.ChangeType(cellValue, Nullable.GetUnderlyingType(typeof(T))));
                                    }
                                    else
                                    {
                                        myObject = (T)(cellValue == DBNull.Value ? Activator.CreateInstance(typeof(T)) : Convert.ChangeType(cellValue, typeof(T)));
                                    }
                                }
                                else
                                {
                                    // If we have a Reference Type, then create the properties Map                                  
                                    // Make a map for properties <-> columns
                                    Dictionary<PropertyInfo, Int32> mapDict = new Dictionary<PropertyInfo, Int32>();

                                    // Get the properties for the object
                                    PropertyInfo[] objectProperties = typeof(T).GetProperties();

                                    // Create the map fro properties <-> columns
                                    FillMap(myReader, mapDict, objectProperties);

                                    // Instantiate a new object for filling it from datarow
                                    Object rootObject = myObject;
                                    Object curObject = rootObject;

                                    // If we have a Reference Type, use the properties Map to make the assignments
                                    // Make the assignment
                                    foreach (PropertyInfo mapProp in mapDict.Keys)
                                    {
                                        Object cellValue = myReader.GetValue(mapDict[mapProp]);

                                        if (mapProp.DeclaringType != typeof(T))
                                        {
                                            // Try to find the root object's property with the declaring type of the property
                                            PropertyInfo declareObjectProperty = objectProperties.FirstOrDefault(x => x.PropertyType == mapProp.DeclaringType);
                                            if (declareObjectProperty != null)
                                            {
                                                // If property was found, get it or instantiate it
                                                Object declareObject = declareObjectProperty.GetValue(rootObject, null);
                                                if (declareObject == null)
                                                {
                                                    declareObject = Activator.CreateInstance(mapProp.DeclaringType);
                                                    // Set the value to the property of the newly created object
                                                    SetPropertyValueToObject(declareObjectProperty, rootObject, declareObject);
                                                }

                                                curObject = declareObject;
                                            }
                                            else
                                            {
                                                // if we couldn't find the property, then ignore the cell value
                                                continue;
                                            }
                                        }
                                        else
                                        {
                                            curObject = rootObject;
                                        }

                                        SetPropertyValueToObject(mapProp, curObject, cellValue);
                                    }
                                }
                            }
                        }
                    }
                }
                myStopWatch.Stop();
                _LastOperationEllapsedTime = myStopWatch.Elapsed;
                Debug.WriteLine(String.Format("{0}[GetDataObject<>] Finished executing SQL code (duration: {1})", GetNowString(), myStopWatch.Elapsed));
                // Return our object
                return myObject;
            }
            catch (Exception ex)
            {
                myStopWatch.Stop();
                _LastOperationEllapsedTime = myStopWatch.Elapsed;
                _LastOperationException = ex;
                Debug.WriteLine(String.Format("{0}[GetDataObject<>] Error executing SQL code! (duration: {1})", GetNowString(), myStopWatch.Elapsed));
                Debug.WriteLine(ex);
                throw;
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
		public static Object IsNull(Object argSourceObject, Object argNullValue)
		{
			if (argSourceObject == null)
			{
				return argNullValue;
			}
			if (argSourceObject == DBNull.Value)
			{
				return argNullValue;
			}
			return argSourceObject;
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
		public static String IsNullString(String argSourceString)
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
		public static String IsNullString(String argSourceString, Boolean argEscapeString, Boolean argEscapeWildcards, Boolean argQuoteString)
		{
			if (argSourceString == null)
			{
				return "NULL";
			}
			if (argEscapeString)
			{
				argSourceString = EscapeString(argSourceString, argEscapeWildcards);
			}
			if (argQuoteString)
			{
				argSourceString = String.Format("N'{0}'", argSourceString);
			}
			return argSourceString;
		}

		#endregion

		#region "EscapeString"

		/// <summary>
		/// It escapes the String by replacing ' with ''
		/// </summary>
		/// <param name="argSourceString"></param>
		/// <returns></returns>
		public static String EscapeString(String argSourceString)
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
		public static String EscapeString(String argSourceString, Boolean argEscapeWildcards)
		{
            if (argSourceString == null)
            {
                return "";
            }
			argSourceString = argSourceString.Replace("'", "''");
            //argSourceString = argSourceString.Replace("\"", "\"\"");
			if (argEscapeWildcards)
			{
				argSourceString = argSourceString.Replace("%", "[%]");
				argSourceString = argSourceString.Replace("_", "[_]");
			}
			return argSourceString;
		}

		#endregion

		#region "CreateSqlConnection"

		/// <summary>
		/// 
		/// </summary>
		/// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
		/// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
		/// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: String.Empty)</param>
		/// <param name="argPassword">The password for the SQL Server account (Default: String.Empty)</param>
		/// <returns></returns>
		public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, String argUserId, String argPassword)
		{
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, false, 8000, argUserId, argPassword, String.Empty, false, false);
		}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: String.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: String.Empty)</param>
        /// <param name="argPersistSecurityInfo">The flag that indicates if security-sensitive information, such as the password, is not returned as part of the connection if the connection is open or has ever been in an open state (Default: false)</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, Boolean argPersistSecurityInfo)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, false, 8000, argUserId, argPassword, String.Empty, false, argPersistSecurityInfo);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: String.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: String.Empty)</param>
        /// <param name="argPersistSecurityInfo">The flag that indicates if security-sensitive information, such as the password, is not returned as part of the connection if the connection is open or has ever been in an open state (Default: false)</param>
        /// <param name="argMultipleActiveResultSets">The flag that indicates if an application can maintain multiple active result sets (MARS) (Default: false).</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, Boolean argPersistSecurityInfo, Boolean argMultipleActiveResultSets)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, false, 8000, argUserId, argPassword, String.Empty, argMultipleActiveResultSets, argPersistSecurityInfo);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: String.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: String.Empty)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, 
            String argApplicationName)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, false, 8000, argUserId, argPassword, argApplicationName, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: String.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: String.Empty)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <param name="argPersistSecurityInfo">The flag that indicates if security-sensitive information, such as the password, is not returned as part of the connection if the connection is open or has ever been in an open state (Default: false)</param>
        /// <param name="argMultipleActiveResultSets">The flag that indicates if an application can maintain multiple active result sets (MARS) (Default: false).</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, String argUserId, String argPassword,
            String argApplicationName, Boolean argPersistSecurityInfo, Boolean argMultipleActiveResultSets)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, false, 8000, argUserId, argPassword, argApplicationName, argMultipleActiveResultSets, argPersistSecurityInfo);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: String.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: String.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, 
            Int32 argConnectTimeout)
		{
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, false, 8000, argUserId, argPassword, String.Empty, false, false);
		}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: String.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: String.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, String argUserId, String argPassword,
            Int32 argConnectTimeout, String argApplicationName)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, false, 8000, argUserId, argPassword, argApplicationName, false, false);
        }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
		/// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
		/// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: String.Empty)</param>
		/// <param name="argPassword">The password for the SQL Server account (Default: String.Empty)</param>
		/// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
		/// <param name="argPacketSize">The size (in bytes) of the network packets used to communicate with an instance of SQL Server (Default: 8000)</param>
		/// <returns></returns>
		public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, 
            Int32 argConnectTimeout, Int32 argPacketSize)
		{
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, false, argPacketSize, argUserId, argPassword, String.Empty, false, false);
		}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: String.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: String.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <param name="argPacketSize">The size (in bytes) of the network packets used to communicate with an instance of SQL Server (Default: 8000)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, String argUserId, String argPassword,
            Int32 argConnectTimeout, Int32 argPacketSize, String argApplicationName)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, false, argPacketSize, argUserId, argPassword, argApplicationName, false, false);
        }

		/// <summary>
		/// 
		/// </summary>
		/// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
		/// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
		/// <returns></returns>
		public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog)
		{
			return CreateSqlConnection(argDataSource, argInitialCatalog, 15, true, 8000, String.Empty, String.Empty, String.Empty, false, false);
		}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, String argApplicationName)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, true, 8000, String.Empty, String.Empty, argApplicationName, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <param name="argMultipleActiveResultSets">The flag that indicates if an application can maintain multiple active result sets (MARS) (Default: false).</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, String argApplicationName, Boolean argMultipleActiveResultSets)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, true, 8000, String.Empty, String.Empty, argApplicationName, argMultipleActiveResultSets, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout)
		{
			return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, true, 8000, String.Empty, String.Empty, String.Empty, false, false);
		}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout, String argApplicationName)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, true, 8000, String.Empty, String.Empty, argApplicationName, false, false);
        }
        
        /// <summary>
		/// 
		/// </summary>
		/// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
		/// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
		/// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
		/// <param name="argPacketSize">The size (in bytes) of the network packets used to communicate with an instance of SQL Server (Default: 8000)</param>
		/// <returns></returns>
		public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout, Int32 argPacketSize)
		{
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, true, argPacketSize, String.Empty, String.Empty, String.Empty, false, false);
		}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <param name="argPacketSize">The size (in bytes) of the network packets used to communicate with an instance of SQL Server (Default: 8000)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout, Int32 argPacketSize, String argApplicationName)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, true, argPacketSize, String.Empty, String.Empty, argApplicationName, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <param name="argIntegratedSecurity">The flag that indicates whether User ID and Password are specified in the connection 
        /// or the current Windows account credentials are used for authentication (Default: false)</param>
        /// <param name="argPacketSize">The size (in bytes) of the network packets used to communicate with an instance of SQL Server (Default: 8000)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: String.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: String.Empty)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string (Default: ".Net SqlClient Data Provider")</param>
        /// <param name="argMultipleActiveResultSets">The flag that indicates if an application can maintain multiple active result sets (MARS) (Default: false).</param>
        /// <param name="argPersistSecurityInfo">The flag that indicates if security-sensitive information, such as the password, is not returned as part of the connection if the connection is open or has ever been in an open state (Default: false)</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout, 
			Boolean argIntegratedSecurity, Int32 argPacketSize, String argUserId, String argPassword, String argApplicationName,
            Boolean argMultipleActiveResultSets, Boolean argPersistSecurityInfo)
		{
            Stopwatch sw = new Stopwatch();
            _LastOperationException = null;
            _LastOperationEllapsedTime = new TimeSpan();
            try
            {
                SqlConnectionStringBuilder myBuilder = new SqlConnectionStringBuilder();

                // Default value: String.Empty
                // This property corresponds to the "Data Source", "server", "address", "addr", and "network address" keys within the connection string. 
                // Regardless of which of these values has been supplied within the supplied connection string, the connection string created by the 
                // SqlConnectionStringBuilder will use the well-known "Data Source" key.
                myBuilder.DataSource = argDataSource;

                // Default value: String.Empty
                // This property corresponds to the "Initial Catalog" and "database" keys within the connection string.
                myBuilder.InitialCatalog = argInitialCatalog;

                // Default value: 15
                // This property corresponds to the "Connect Timeout", "connection timeout", and "timeout" keys within the connection string.
                myBuilder.ConnectTimeout = argConnectTimeout;

                // Default value: false
                // This property corresponds to the "Integrated Security" and "trusted_connection" keys within the connection string.
                myBuilder.IntegratedSecurity = argIntegratedSecurity;

                // Default value: 8000 
                // This property corresponds to the "Packet Size" key within the connection string.
                myBuilder.PacketSize = argPacketSize;

                // Default value: ".Net SqlClient Data Provider"
                // This property corresponds to the "Application Name" key within the connection string.
                if (!String.IsNullOrWhiteSpace(argApplicationName))
                {
                    myBuilder.ApplicationName = argApplicationName;
                }

                // Default Value: 100
                // This property corresponds to the "Max Pool Size" key within the connection string.
                myBuilder.MaxPoolSize = 300;

                // Default Value: false
                // This property corresponds to the "MultipleActiveResultSets" key within the connection string.
                myBuilder.MultipleActiveResultSets = argMultipleActiveResultSets;

                // Check if integrated security is true and in that case, leave UserID and Password fields empty
                if (!argIntegratedSecurity)
                {
                    // Default value: String.Empty
                    // This property corresponds to the "User ID", "user", and "uid" keys within the connection string.
                    myBuilder.UserID = argUserId;

                    // Default value: String.Empty
                    // This property corresponds to the "Password" and "pwd" keys within the connection string.
                    myBuilder.Password = argPassword;

                    // Default Value: false
                    // This property corresponds to the "Persist Security Info" and "persistsecurityinfo" keys within the connection string.
                    myBuilder.PersistSecurityInfo = argPersistSecurityInfo;
                }

                Debug.WriteLine(String.Format("{0}[CreateSqlConnection] Starting to connect to SQL server:", GetNowString()));
                Debug.WriteLine(myBuilder.ConnectionString);

                // Return a new connection with the connection string of the builder
                sw.Start();
                SqlConnection myCon = null;
                lock (_ThreadAnchor)
                {
                    myCon = new SqlConnection(myBuilder.ConnectionString);
                }
                sw.Stop();

                _LastOperationEllapsedTime = sw.Elapsed;

                Debug.WriteLine(String.Format("{0}[CreateSqlConnection] Finished connecting to SQL server (duration: {1})", GetNowString(), sw.Elapsed));

                // Clears the Connection pool associated with this connection
                SqlConnection.ClearPool(myCon);

                return myCon;
            }
            catch (Exception ex)
            {
                sw.Stop();
                _LastOperationException = ex;
                _LastOperationEllapsedTime = sw.Elapsed;
                Debug.WriteLine(String.Format("{0}[CreateSqlConnection] Error connecting to SQL server! (duration: {1})", GetNowString(), sw.Elapsed));
                Debug.WriteLine(ex);
                throw;
            }
		}

		#endregion

		#region "Debug Functions"

        /// <summary>
        /// Returns the current time as a String with format "[dd/MM/yyyy][hh:mm:ss.fff]"
        /// </summary>
        /// <returns></returns>
        public static String GetNowString()
        {
            return DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]");
        }

		public static String GetSQLCommandString(SqlCommand argSqlCmd)
		{
			try
			{
				String cmdSqlCode = argSqlCmd.CommandText;
				if (argSqlCmd.Parameters != null)
				{
					foreach (SqlParameter sqlParam in argSqlCmd.Parameters)
					{
						try
						{
							// Check for NULL value
							if (((INullable)sqlParam.SqlValue).IsNull)
							{
								cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "NULL");
								continue;
							}
							// replace parameter value to the argSqlCode for debugging purposes
							byte[] ba = null;
							StringBuilder hex;
							switch (sqlParam.SqlDbType)
							{
								case SqlDbType.BigInt:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, ((Int64)sqlParam.Value).ToString(System.Globalization.CultureInfo.InvariantCulture));
									break;
								case SqlDbType.Binary:
									// Convert byte array to hex string
									ba = (byte[])sqlParam.Value;
									hex = new StringBuilder(ba.Length * 2);
									foreach (byte b in ba)
									{
										hex.AppendFormat("{0:x2}", b);
									}
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + hex.ToString() + "'");
									break;
								case SqlDbType.Bit:
									Boolean bValue = (Boolean)sqlParam.Value;
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, bValue ? "1" : "0");
									break;
								case SqlDbType.Char:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + EscapeString((String)sqlParam.Value) + "'");
									break;
								case SqlDbType.Date:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + ((DateTime)sqlParam.Value).ToString("dd/MM/yyyy") + "'");
									break;
								case SqlDbType.DateTime:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + ((DateTime)sqlParam.Value).ToString("dd/MM/yyyy HH:mm:ss.fff") + "'");
									break;
								case SqlDbType.DateTime2:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + ((DateTime)sqlParam.Value).ToString("dd/MM/yyyy HH:mm:ss.fffffff") + "'");
									break;
								case SqlDbType.DateTimeOffset:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + ((DateTime)sqlParam.Value).ToString("dd/MM/yyyy HH:mm:ss.fff zzz") + "'");
									break;
								case SqlDbType.Decimal:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, ((Decimal)sqlParam.Value).ToString(System.Globalization.CultureInfo.InvariantCulture));
									break;
								case SqlDbType.Float:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, ((Double)sqlParam.Value).ToString(System.Globalization.CultureInfo.InvariantCulture));
									break;
								case SqlDbType.Image:
									// Convert byte array to hex string
									ba = (byte[])sqlParam.Value;
									hex = new StringBuilder(ba.Length * 2);
									foreach (byte b in ba)
									{
										hex.AppendFormat("{0:x2}", b);
									}
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + hex.ToString() + "'");
									break;
								case SqlDbType.Int:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, ((Int32)sqlParam.Value).ToString());
									break;
								case SqlDbType.Money:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, ((Decimal)sqlParam.Value).ToString(System.Globalization.CultureInfo.InvariantCulture));
									break;
								case SqlDbType.NChar:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "N'" + EscapeString((String)sqlParam.Value) + "'");
									break;
								case SqlDbType.NText:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "N'" + EscapeString((String)sqlParam.Value) + "'");
									break;
								case SqlDbType.NVarChar:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "N'" + EscapeString((String)sqlParam.Value) + "'");
									break;
								case SqlDbType.Real:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, ((Single)sqlParam.Value).ToString(System.Globalization.CultureInfo.InvariantCulture));
									break;
								case SqlDbType.SmallDateTime:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + ((DateTime)sqlParam.Value).ToString("dd/MM/yyyy HH:mm") + "'");
									break;
								case SqlDbType.SmallInt:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, ((Int16)sqlParam.Value).ToString());
									break;
								case SqlDbType.SmallMoney:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, ((Decimal)sqlParam.Value).ToString(System.Globalization.CultureInfo.InvariantCulture));
									break;
								case SqlDbType.Structured:
									// TODO
									break;
								case SqlDbType.Text:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + EscapeString((String)sqlParam.Value) + "'");
									break;
								case SqlDbType.Time:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + ((DateTime)sqlParam.Value).ToString("HH:mm:ss.fffffff") + "'");
									break;
								case SqlDbType.Timestamp:
									// Convert byte array to hex string
									ba = (byte[])sqlParam.Value;
									hex = new StringBuilder(ba.Length * 2);
									foreach (byte b in ba)
									{
										hex.AppendFormat("{0:x2}", b);
									}
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + hex.ToString() + "'");
									break;
								case SqlDbType.TinyInt:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, Convert.ToInt16(sqlParam.Value).ToString(System.Globalization.CultureInfo.InvariantCulture));
									break;
								case SqlDbType.Udt:
									// TODO
									break;
								case SqlDbType.UniqueIdentifier:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + (Guid)sqlParam.Value + "'");
									break;
								case SqlDbType.VarBinary:
									// Convert byte array to hex string
									ba = (byte[])sqlParam.Value;
									hex = new StringBuilder(ba.Length * 2);
									foreach (byte b in ba)
									{
										hex.AppendFormat("{0:x2}", b);
									}
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + hex.ToString() + "'");
									break;
								case SqlDbType.VarChar:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + EscapeString((String)sqlParam.Value) + "'");
									break;
								case SqlDbType.Variant:
									break;
								case SqlDbType.Xml:
									cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + EscapeString((String)sqlParam.Value) + "'");
									break;
								default:
									break;
							}
						}
						catch (Exception ex)
						{
							Debug.WriteLine(ex);
							return cmdSqlCode;
						}
					}
				}
				return cmdSqlCode;
			}
			catch (Exception ex)
			{
				Debug.WriteLine(ex);
				throw;
			}
		}

		private static String ReplaceWholeWord(String original, String oldWord, String newWord)
		{
			return Regex.Replace(original,
								String.Format(@"\b{0}\b", oldWord),
								newWord,
                                RegexOptions.IgnoreCase);
		}

        #endregion

        public static Int32 NumberOfOccurences(string argSource, string argSearch)
        {
            // Check for empty source or search string
            if (String.IsNullOrEmpty(argSource) || String.IsNullOrEmpty(argSearch))
            {
                return 0;
            }
            // Check if search string is longer than the source string
            if (argSearch.Length > argSource.Length)
            {
                return 0;
            }

            Int32 occurences = 0, currentSourceIndex = 0, sourceLength = argSource.Length, searchLength = argSearch.Length, i = 0;
            bool foundOccurence = true;
            while (true)
            {
                foundOccurence = true;
                for (i = 0; i < searchLength; i++)
                {
                    if (argSource[currentSourceIndex + i] != argSearch[i])
                    {
                        foundOccurence = false;
                        i++;
                        break;
                    }
                }
                if (foundOccurence)
                {
                    occurences++;
                }
                currentSourceIndex += i;
                if (currentSourceIndex > sourceLength - 1)
                {
                    break;
                }
            }
            return occurences;
        }
    }
}
