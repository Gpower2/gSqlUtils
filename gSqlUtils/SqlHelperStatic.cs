using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Data;
using System.Diagnostics;
using System.Data.SqlTypes;
using System.Text.RegularExpressions;

namespace gSqlUtils
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
				if (argSqlCode.Trim().Length == 0)
				{
					throw new Exception("Empty SQL code!");
				}
				if (argSqlCon.State == System.Data.ConnectionState.Closed)
				{
					argSqlCon.Open();
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
					Debug.WriteLine(DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]") + " Starting to execute SQL code:");
					Debug.WriteLine(GetSQLCommandString(sqlCmd));
					myStopWatch.Start();
					rowsAffected = sqlCmd.ExecuteNonQuery();
					myStopWatch.Stop();
					_LastOperationEllapsedTime = new TimeSpan(myStopWatch.ElapsedTicks);
					Debug.WriteLine(DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]") + " Finished executing SQL code (duration: " + myStopWatch.Elapsed.ToString() + ")");
					sqlCmd.Dispose();
					return rowsAffected;
				}
			}
			catch (Exception ex)
			{
				myStopWatch.Stop();
				_LastOperationEllapsedTime = new TimeSpan(myStopWatch.ElapsedTicks);
				_LastOperationException = ex;
				Debug.WriteLine(DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]") + " Error executing SQL code!");
				Debug.WriteLine(ex);
				GC.Collect();
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
				if (argSqlCode.Trim().Length == 0)
				{
					throw new Exception("Empty SQL code!");
				}
				if (argSqlCon.State == System.Data.ConnectionState.Closed)
				{
					argSqlCon.Open();
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
					Debug.WriteLine(DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]") + " Starting to execute SQL code:");
					Debug.WriteLine(GetSQLCommandString(myAdapter.SelectCommand));
					myStopWatch.Start();
					myAdapter.Fill(myDatatable);
					myStopWatch.Stop();
					_LastOperationEllapsedTime = new TimeSpan(myStopWatch.ElapsedTicks);
					Debug.WriteLine(DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]") + " Finished executing SQL code (duration: " + myStopWatch.Elapsed.ToString() + ")");
				}
				GC.Collect();
				return myDatatable;
			}
			catch (Exception ex)
			{
				myStopWatch.Stop();
				_LastOperationEllapsedTime = new TimeSpan(myStopWatch.ElapsedTicks);
				_LastOperationException = ex;
				Debug.WriteLine(DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]") + " Error executing SQL code!");
				if (myDatatable != null)
				{
					myDatatable.Dispose();
					myDatatable = null;
				}
				Debug.WriteLine(ex);
				GC.Collect();
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
				if (argSqlCode.Trim().Length == 0)
				{
					throw new Exception("Empty SQL code!");
				}
				if (argSqlCon.State == System.Data.ConnectionState.Closed)
				{
					argSqlCon.Open();
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
					Debug.WriteLine(DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]") + " Starting to execute SQL code:");
					Debug.WriteLine(GetSQLCommandString(myAdapter.SelectCommand));
					myStopWatch.Start();
					myAdapter.Fill(myDataset);
					myStopWatch.Stop();
					_LastOperationEllapsedTime = new TimeSpan(myStopWatch.ElapsedTicks);
					Debug.WriteLine(DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]") + " Finished executing SQL code (duration: " + myStopWatch.Elapsed.ToString() + ")");
				}
				GC.Collect();
				return myDataset;
			}
			catch (Exception ex)
			{
				myStopWatch.Stop();
				_LastOperationEllapsedTime = new TimeSpan(myStopWatch.ElapsedTicks);
				_LastOperationException = ex;
				Debug.WriteLine(DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]") + " Error executing SQL code!");
				if (myDataset != null)
				{
					myDataset.Dispose();
					myDataset = null;
				}
				Debug.WriteLine(ex);
				GC.Collect();
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
				if (argSqlCode.Trim().Length == 0)
				{
					throw new Exception("Empty SQL code!");
				}
				if (argSqlCon.State == System.Data.ConnectionState.Closed)
				{
					argSqlCon.Open();
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
					Debug.WriteLine(DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]") + " Starting to execute SQL code:");
					Debug.WriteLine(GetSQLCommandString(myCommand));
					myStopWatch.Start();
					Object resultObject = myCommand.ExecuteScalar();
					myStopWatch.Stop();
					_LastOperationEllapsedTime = new TimeSpan(myStopWatch.ElapsedTicks);
					Debug.WriteLine(DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]") + " Finished executing SQL code (duration: " + myStopWatch.Elapsed.ToString() + ")");
					GC.Collect();
					return resultObject;
				}
			}
			catch (Exception ex)
			{
				myStopWatch.Stop();
				_LastOperationEllapsedTime = new TimeSpan(myStopWatch.ElapsedTicks);
				_LastOperationException = ex;
				Debug.WriteLine(DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]") + " Error executing SQL code!");
				Debug.WriteLine(ex);
				GC.Collect();
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
				argSourceString = String.Format("'{0}'", argSourceString);
			}
			return argSourceString;
		}

		#endregion

		#region "EscapeString"

		/// <summary>
		/// It escapes the String by replacing ' with '' and " with "".
		/// </summary>
		/// <param name="argSourceString"></param>
		/// <returns></returns>
		public static String EscapeString(String argSourceString)
		{
			return EscapeString(argSourceString, false);
		}

		/// <summary>
		/// It escapes the String by replacing ' with ''
		/// and " with "".
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
				return "NULL";
			}
			argSourceString = argSourceString.Replace("'", "''");
			argSourceString = argSourceString.Replace("\"", "\"\"");
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
			return CreateSqlConnection(argDataSource, argInitialCatalog, 15, false, 8000, argUserId, argPassword);
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
		public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, Int32 argConnectTimeout)
		{
			return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, false, 8000, argUserId, argPassword);
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
		public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, String argUserId, String argPassword, Int32 argConnectTimeout, Int32 argPacketSize)
		{
			return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, false, argPacketSize, argUserId, argPassword);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: String.Empty)</param>
		/// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: String.Empty)</param>
		/// <returns></returns>
		public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog)
		{
			return CreateSqlConnection(argDataSource, argInitialCatalog, 15, true, 8000, String.Empty, String.Empty);
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
			return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, true, 8000, String.Empty, String.Empty);
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
			return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, true, argPacketSize, String.Empty, String.Empty);
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
		/// <returns></returns>
		public static SqlConnection CreateSqlConnection(String argDataSource, String argInitialCatalog, Int32 argConnectTimeout, 
			Boolean argIntegratedSecurity, Int32 argPacketSize, String argUserId, String argPassword)
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

			// Check if integrated security is true and in that case, leave UserID and Password fields empty
			if (!argIntegratedSecurity)
			{
				// Default value: String.Empty
				// This property corresponds to the "User ID", "user", and "uid" keys within the connection string.
				myBuilder.UserID = argUserId;

				// Default value: String.Empty
				// This property corresponds to the "Password" and "pwd" keys within the connection string.
				myBuilder.Password = argPassword;
			}

			// Return a new connection with the connection string of the builder
			return new SqlConnection(myBuilder.ConnectionString);
		}

		#endregion

		#region "Debug Functions"

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
								newWord);
		}

		#endregion
	}
}
