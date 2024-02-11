using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

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
    public static partial class SqlHelperStatic
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

        private static readonly object _ThreadAnchor = new object();

        private static readonly SemaphoreSlim _connectionSemaphore = new SemaphoreSlim(1, 1);

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
		public static object IsNull(object argSourceObject, object argNullValue)
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
        /// It checks if a string is null and then it returns NULL.
        /// Else, it replaces the escape characters ' and " and 
        /// returns the string single quoted eg. text => 'text'
        /// </summary>
        /// <param name="argSourceString">The source string to check for null</param>
        /// <returns></returns>
        public static string IsNullString(string argSourceString)
        {
            return IsNullString(argSourceString, true, false, true);
        }

        /// <summary>
        /// It checks if a string is null and then it returns NULL.
        /// Else, according to user options, it either replaces the
        /// escape characters ' and " or not, it either replaces the
        /// wildcard characters % and _ and [ or not, and it either single
        /// quotes the string or not eg. text => 'text'
        /// </summary>
        /// <param name="argSourceString">The source string to check for null</param>
        /// <param name="argEscapeString">The flag whether to replace the escape characters or not</param>
        /// <param name="argEscapeWildcards">The flag whether to replace the wildcard characters or not</param>
        /// <param name="argQuoteString">The flag whether to single quote the string or not</param>
        /// <returns></returns>
        public static string IsNullString(string argSourceString, bool argEscapeString, bool argEscapeWildcards, bool argQuoteString)
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
                argSourceString = string.Format("N'{0}'", argSourceString);
            }
            return argSourceString;
        }

        #endregion

        #region "EscapeString"

        /// <summary>
        /// It escapes the string by replacing ' with ''
        /// </summary>
        /// <param name="argSourceString"></param>
        /// <returns></returns>
        public static string EscapeString(string argSourceString)
        {
            return EscapeString(argSourceString, false);
        }

        /// <summary>
        /// It escapes the string by replacing ' with ''.
        /// It also escapes the wildcard charactes % and _ and [
        /// if the user specifies it.
        /// </summary>
        /// <param name="argSourceString">The source string to escape</param>
        /// <param name="argEscapeWildcards">The flag to whether escape the wildcard characters or not</param>
        /// <returns>The escaped string</returns>
        public static string EscapeString(string argSourceString, bool argEscapeWildcards)
        {
            if (argSourceString == null)
            {
                return "";
            }
            argSourceString = argSourceString.Replace("'", "''");
            //argSourceString = argSourceString.Replace("\"", "\"\"");
            if (argEscapeWildcards)
            {
                MatchCollection openPar = Regex.Matches(argSourceString, @"\[");
                List<Match> allMatches = new List<Match>(openPar.Cast<Match>());
                allMatches = allMatches.OrderBy(m => m.Index).ToList();

                int offset = 0;
                foreach (Match m in allMatches)
                {
                    argSourceString = argSourceString.Remove(m.Index + offset, 1);
                    if (m.Value == "[")
                    {
                        argSourceString = argSourceString.Insert(m.Index + offset, "[[]");
                    }
                    else
                    {
                        continue;
                    }
                    offset += 2;
                }

                argSourceString = argSourceString.Replace("%", "[%]");
                argSourceString = argSourceString.Replace("_", "[_]");
            }
            return argSourceString;
        }

        #endregion

        #region "Debug Functions"

        /// <summary>
        /// Returns the current time as a string with format "[dd/MM/yyyy][hh:mm:ss.fff]"
        /// </summary>
        /// <returns></returns>
        public static string GetNowString()
        {
            return DateTime.Now.ToString("[dd/MM/yyyy][hh:mm:ss.fff]");
        }

        public static string GetSQLCommandString(SqlCommand argSqlCmd)
        {
            try
            {
                string cmdSqlCode = argSqlCmd.CommandText;
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
                                    bool bValue = (bool)sqlParam.Value;
                                    cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, bValue ? "1" : "0");
                                    break;
                                case SqlDbType.Char:
                                    cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + EscapeString((string)sqlParam.Value) + "'");
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
                                    cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, ((int)sqlParam.Value).ToString());
                                    break;
                                case SqlDbType.Money:
                                    cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, ((Decimal)sqlParam.Value).ToString(System.Globalization.CultureInfo.InvariantCulture));
                                    break;
                                case SqlDbType.NChar:
                                    cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "N'" + EscapeString((string)sqlParam.Value) + "'");
                                    break;
                                case SqlDbType.NText:
                                    cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "N'" + EscapeString((string)sqlParam.Value) + "'");
                                    break;
                                case SqlDbType.NVarChar:
                                    cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "N'" + EscapeString((string)sqlParam.Value) + "'");
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
                                    cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + EscapeString((string)sqlParam.Value) + "'");
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
                                    cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + EscapeString((string)sqlParam.Value) + "'");
                                    break;
                                case SqlDbType.Variant:
                                    break;
                                case SqlDbType.Xml:
                                    cmdSqlCode = ReplaceWholeWord(cmdSqlCode, sqlParam.ParameterName, "'" + EscapeString((string)sqlParam.Value) + "'");
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

        private static string ReplaceWholeWord(string original, string oldWord, string newWord)
        {
            return Regex.Replace(original,
                                string.Format(@"\b{0}\b", oldWord),
                                newWord,
                                RegexOptions.IgnoreCase);
        }

        #endregion

        public static int NumberOfOccurences(string argSource, string argSearch)
        {
            // Check for empty source or search string
            if (string.IsNullOrEmpty(argSource) || string.IsNullOrEmpty(argSearch))
            {
                return 0;
            }
            // Check if search string is longer than the source string
            if (argSearch.Length > argSource.Length)
            {
                return 0;
            }

            int occurences = 0, currentSourceIndex = 0, sourceLength = argSource.Length, searchLength = argSearch.Length, i = 0;
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
