using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace gpower2.gSqlUtils
{
    public static partial class SqlHelperStatic
    {
        #region "GetDataSetAsync"

        /// <summary>
        /// Returns a DataSet that contains the results of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <returns>The DataSet that contains the results</returns>
        public static Task<DataSet> GetDataSetAsync(string argSqlCode, SqlConnection argSqlCon)
        {
            return GetDataSetAsync(argSqlCode, argSqlCon, 120, null, null);
        }

        /// <summary>
        /// Returns a DataSet that contains the results of the execution of an SQL statement.
        /// It doesn't use a transaction.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argTimeout">The timeout for the SQL command in seconds</param>
        /// <returns>The DataSet that contains the results</returns>
        public static Task<DataSet> GetDataSetAsync(string argSqlCode, SqlConnection argSqlCon, int argTimeout)
        {
            return GetDataSetAsync(argSqlCode, argSqlCon, argTimeout, null, null);
        }

        /// <summary>
        /// Returns a DataSet that contains the results of the execution of an SQL statement.
        /// It sets a default timeout of 120 seconds.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <param name="argSqlTransaction">The SQL transaction to use</param>
        /// <returns>The DataSet that contains the results</returns>
        public static Task<DataSet> GetDataSetAsync(string argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction)
        {
            return GetDataSetAsync(argSqlCode, argSqlCon, 120, argSqlTransaction, null);
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
        public static async Task<DataSet> GetDataSetAsync(string argSqlCode, SqlConnection argSqlCon, int argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters)
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
                if (string.IsNullOrWhiteSpace(argSqlCode))
                {
                    throw new Exception("Empty SQL code!");
                }
                #if NET40
                _connectionSemaphore.Wait();
                #else
                await _connectionSemaphore.WaitAsync();
                #endif
                {
                    if (argSqlCon.State == System.Data.ConnectionState.Closed)
                    {
                        await argSqlCon.OpenAsync();
                        Debug.WriteLine(string.Format("{0}[GetDataSetAsync] Opened connection...", GetNowString()));
                    }
                    // Wait for the connection to finish connecting
                    while (argSqlCon.State == ConnectionState.Connecting) { }
                }
                _connectionSemaphore.Release();

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
                    Debug.WriteLine(string.Format("{0}[GetDataSetAsync] Starting to execute SQL code:", GetNowString()));
                    Debug.WriteLine(GetSQLCommandString(myAdapter.SelectCommand));
                    myStopWatch.Start();
                    myAdapter.Fill(myDataset);
                    myStopWatch.Stop();
                    _LastOperationEllapsedTime = myStopWatch.Elapsed;
                    Debug.WriteLine(string.Format("{0}[GetDataSetAsync] Finished executing SQL code (duration: {1})", GetNowString(), myStopWatch.Elapsed));
                }
                return myDataset;
            }
            catch (Exception ex)
            {
                myStopWatch.Stop();
                _LastOperationEllapsedTime = myStopWatch.Elapsed;
                _LastOperationException = ex;
                Debug.WriteLine(string.Format("{0}[GetDataSetAsync] Error executing SQL code! (duration: {1})", GetNowString(), myStopWatch.Elapsed));
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
    }
}
