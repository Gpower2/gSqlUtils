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
        #region "ExecuteSqlAsync"

        /// <summary>
        /// Executes a non scalar SQL statement.
        /// It sets a default timeout of 120 seconds and doesn't use a transaction.
        /// It returns the number of rows affected.
        /// </summary>
        /// <param name="argSqlCode">The SQL code to execute</param>
        /// <param name="argSqlCon">The SQL connection to use</param>
        /// <returns>The number of rows affected</returns>
        public static Task<int> ExecuteSqlAsync(string argSqlCode, SqlConnection argSqlCon)
		{
			return ExecuteSqlAsync(argSqlCode, argSqlCon, 120, null, null);
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
		public static Task<int> ExecuteSqlAsync(string argSqlCode, SqlConnection argSqlCon, int argTimeout)
		{
			return ExecuteSqlAsync(argSqlCode, argSqlCon, argTimeout, null, null);
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
		public static Task<int> ExecuteSqlAsync(string argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction)
		{
			return ExecuteSqlAsync(argSqlCode, argSqlCon, 120, argSqlTransaction, null);
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
		public static async Task<int> ExecuteSqlAsync(string argSqlCode, SqlConnection argSqlCon, int argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters)
		{
			Stopwatch myStopWatch = new Stopwatch();
			int rowsAffected = 0;
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
                await Task.Factory.StartNew(() => _connectionSemaphore.Wait());
				#else
                await _connectionSemaphore.WaitAsync();
				#endif
				try
				{
					if (argSqlCon.State == System.Data.ConnectionState.Closed)
					{
						await argSqlCon.OpenAsync();
						Debug.WriteLine(string.Format("{0}[ExecuteSqlAsync] Opened connection...", GetNowString()));
					}
					// Wait for the connection to finish connecting
					while (argSqlCon.State == ConnectionState.Connecting) { }
				}
				finally
				{
					_connectionSemaphore.Release();
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
					Debug.WriteLine(string.Format("{0}[ExecuteSqlAsync] Starting to execute SQL code:", GetNowString()));
					Debug.WriteLine(GetSQLCommandString(sqlCmd));
					myStopWatch.Start();
					rowsAffected = await sqlCmd.ExecuteNonQueryAsync();
					myStopWatch.Stop();
					_LastOperationEllapsedTime = myStopWatch.Elapsed;
					Debug.WriteLine(string.Format("{0}[ExecuteSqlAsync] Finished executing SQL code (duration: {1})", GetNowString(), myStopWatch.Elapsed));
					return rowsAffected;
				}
			}
			catch (Exception ex)
			{
				myStopWatch.Stop();
				_LastOperationEllapsedTime = myStopWatch.Elapsed;
				_LastOperationException = ex;
				Debug.WriteLine(string.Format("{0}[ExecuteSqlAsync] Error executing SQL code! (duration: {1})", GetNowString(), myStopWatch.Elapsed));
				Debug.WriteLine(ex);
				throw;
			}
		}

		#endregion
	}
}
