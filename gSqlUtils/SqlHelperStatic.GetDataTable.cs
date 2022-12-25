using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace gpower2.gSqlUtils
{
    public static partial class SqlHelperStatic
    {
        #region "GetDataTable"

        /// <summary>
        /// Returns a DataTable that contains the results of the execution of an SQL statement.
        /// If the results are a DataSet, then only the first DataTable is returned.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <returns>The DataTable that contains the results</returns>
        public static DataTable GetDataTable(string argSqlCode, SqlConnection argSqlCon)
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
        public static DataTable GetDataTable(string argSqlCode, SqlConnection argSqlCon, int argTimeout)
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
        public static DataTable GetDataTable(string argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction)
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
        public static DataTable GetDataTable(string argSqlCode, SqlConnection argSqlCon, int argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters)
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
                if (string.IsNullOrWhiteSpace(argSqlCode))
                {
                    throw new Exception("Empty SQL code!");
                }
                lock (_ThreadAnchor)
                {
                    if (argSqlCon.State == System.Data.ConnectionState.Closed)
                    {
                        argSqlCon.Open();
                        Debug.WriteLine(string.Format("{0}[GetDataTable] Opened connection...", GetNowString()));
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
                    Debug.WriteLine(string.Format("{0}[GetDataTable] Starting to execute SQL code:", GetNowString()));
                    Debug.WriteLine(GetSQLCommandString(myAdapter.SelectCommand));
                    myStopWatch.Start();
                    myAdapter.Fill(myDatatable);
                    myStopWatch.Stop();
                    _LastOperationEllapsedTime = myStopWatch.Elapsed;
                    Debug.WriteLine(string.Format("{0}[GetDataTable] Finished executing SQL code (duration: {1})", GetNowString(), myStopWatch.Elapsed));
                }
                return myDatatable;
            }
            catch (Exception ex)
            {
                myStopWatch.Stop();
                _LastOperationEllapsedTime = myStopWatch.Elapsed;
                _LastOperationException = ex;
                Debug.WriteLine(string.Format("{0}[GetDataTable] Error executing SQL code! (duration: {1})", GetNowString(), myStopWatch.Elapsed));
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
    }
}
