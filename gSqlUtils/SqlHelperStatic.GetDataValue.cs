using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace gpower2.gSqlUtils
{
    public static partial class SqlHelperStatic
    {
        #region "GetDataValue"

        /// <summary>
        /// Returns the value of the first cell of the first DataTable from the results
        /// of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <returns></returns>
        public static object GetDataValue(string argSqlCode, SqlConnection argSqlCon)
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
        public static object GetDataValue(string argSqlCode, SqlConnection argSqlCon, int argTimeout)
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
        public static object GetDataValue(string argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction)
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
        public static object GetDataValue(string argSqlCode, SqlConnection argSqlCon, int argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters)
        {
            Stopwatch myStopWatch = new Stopwatch();
            _LastOperationException = null;
            try
            {
                if (argSqlCon == null)
                {
                    throw new Exception("Null SQL connection!");
                }
                if (string.IsNullOrWhiteSpace(argSqlCode))
                {
                    throw new Exception("Empty SQL code!");
                }
                lock (_ThreadAnchor)
                {
                    if (argSqlCon.State == System.Data.ConnectionState.Closed)
                    {
                        argSqlCon.Open();
                        Debug.WriteLine(string.Format("{0}[GetDataValue] Opened connection...", GetNowString()));
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
                    Debug.WriteLine(string.Format("{0}[GetDataValue] Starting to execute SQL code:", GetNowString()));
                    Debug.WriteLine(GetSQLCommandString(myCommand));
                    myStopWatch.Start();
                    object resultObject = myCommand.ExecuteScalar();
                    myStopWatch.Stop();
                    _LastOperationEllapsedTime = myStopWatch.Elapsed;
                    Debug.WriteLine(string.Format("{0}[GetDataValue] Finished executing SQL code (duration: {1})", GetNowString(), myStopWatch.Elapsed));
                    return resultObject;
                }
            }
            catch (Exception ex)
            {
                myStopWatch.Stop();
                _LastOperationEllapsedTime = myStopWatch.Elapsed;
                _LastOperationException = ex;
                Debug.WriteLine(string.Format("{0}[GetDataValue] Error executing SQL code! (duration: {1})", GetNowString(), myStopWatch.Elapsed));
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
        public static T GetDataValue<T>(string argSqlCode, SqlConnection argSqlCon) where T : IComparable, IConvertible, IEquatable<T>
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
        public static T GetDataValue<T>(string argSqlCode, SqlConnection argSqlCon, int argTimeout) where T : IComparable, IConvertible, IEquatable<T>
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
        public static T GetDataValue<T>(string argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction) where T : IComparable, IConvertible, IEquatable<T>
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
        public static T GetDataValue<T>(string argSqlCode, SqlConnection argSqlCon, int argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters) where T : IComparable, IConvertible, IEquatable<T>
        {
            Stopwatch myStopWatch = new Stopwatch();
            _LastOperationException = null;
            try
            {
                if (argSqlCon == null)
                {
                    throw new Exception("Null SQL connection!");
                }
                if (string.IsNullOrWhiteSpace(argSqlCode))
                {
                    throw new Exception("Empty SQL code!");
                }
                lock (_ThreadAnchor)
                {
                    if (argSqlCon.State == System.Data.ConnectionState.Closed)
                    {
                        argSqlCon.Open();
                        Debug.WriteLine(string.Format("{0}[GetDataValue<>] Opened connection...", GetNowString()));
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
                    Debug.WriteLine(string.Format("{0}[GetDataValue<>] Starting to execute SQL code:", GetNowString()));
                    Debug.WriteLine(GetSQLCommandString(myCommand));
                    myStopWatch.Start();
                    // Excecute the SQL command
                    object res = myCommand.ExecuteScalar();
                    myStopWatch.Stop();
                    _LastOperationEllapsedTime = myStopWatch.Elapsed;
                    Debug.WriteLine(string.Format("{0}[GetDataValue<>] Finished executing SQL code (duration: {1})", GetNowString(), myStopWatch.Elapsed));

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
                Debug.WriteLine(string.Format("{0}[GetDataValue<>] Error executing SQL code! (duration: {1})", GetNowString(), myStopWatch.Elapsed));
                Debug.WriteLine(ex);
                throw;
            }
        }

        #endregion
    }
}
