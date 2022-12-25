using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace gpower2.gSqlUtils
{
    public static partial class SqlHelperStatic
    {
        #region "GetDataListAsync"

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
        public static Task<IEnumerable> GetDataListAsync(Type argObjectType, string argSqlCode, SqlConnection argSqlCon)
        {
            return GetDataListAsync(argObjectType, argSqlCode, argSqlCon, 120, null, null);
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
        public static Task<IEnumerable> GetDataListAsync(Type argObjectType, string argSqlCode, SqlConnection argSqlCon, int argTimeout)
        {
            return GetDataListAsync(argObjectType, argSqlCode, argSqlCon, argTimeout, null, null);
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
        public static Task<IEnumerable> GetDataListAsync(Type argObjectType, string argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction)
        {
            return GetDataListAsync(argObjectType, argSqlCode, argSqlCon, 120, argSqlTransaction, null);
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
        public static async Task<IEnumerable> GetDataListAsync(Type argObjectType, string argSqlCode, SqlConnection argSqlCon, int argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters)
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
                if (string.IsNullOrWhiteSpace(argSqlCode))
                {
                    throw new Exception("Empty SQL query!");
                }
                // Get the List<T> type for our object type
                Type genericListType = typeof(List<>).MakeGenericType(argObjectType);
                // Instantiate the List<T>
                IList objectList = (IList)Activator.CreateInstance(genericListType);

                #if NET40
                _connectionSemaphore.Wait();
                #else
                await _connectionSemaphore.WaitAsync();
                #endif
                {
                    // Open the SQL connection in case it's closed
                    if (argSqlCon.State == System.Data.ConnectionState.Closed)
                    {
                        await argSqlCon.OpenAsync();
                        Debug.WriteLine(string.Format("{0}[GetDataListAsync] Opened connection...", GetNowString()));
                    }
                    // Wait for the connection to finish connecting
                    while (argSqlCon.State == ConnectionState.Connecting) { }
                }
                _connectionSemaphore.Release();

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
                    Debug.WriteLine(string.Format("{0}[GetDataListAsync] Starting to execute SQL code:", GetNowString()));
                    Debug.WriteLine(GetSQLCommandString(sqlCmd));
                    myStopWatch.Start();
                    // Create the DataReader from our command
                    using (SqlDataReader myReader = await sqlCmd.ExecuteReaderAsync())
                    {
                        // Check if there are rows
                        if (myReader.HasRows)
                        {
                            // Check if the Type requested is ValueType
                            if (argObjectType.IsValueType || argObjectType == typeof(string))
                            {
                                // If we have a Value Type, we don't need to create a properties map
                                // Begin reading
                                while (
                                    #if NET40
                                    myReader.Read()
                                    #else
                                    await myReader.ReadAsync()
                                    #endif
                                    )
                                {
                                    // Instantiate a new object for filling it from datarow
                                    object myObject;
                                    if (argObjectType == typeof(string))
                                    {
                                        myObject = null;
                                    }
                                    else
                                    {
                                        myObject = Activator.CreateInstance(argObjectType);
                                    }

                                    // If we have a Value Type, then use the first column
                                    object cellValue = myReader.GetValue(0);

                                    // Check for Nullable<T> properties
                                    if (argObjectType.IsGenericType && argObjectType.GetGenericTypeDefinition() == typeof(Nullable<>))
                                    {
                                        // If the type is Nullable<T> we change the value to the Nullable<T> equivalent
                                        myObject = (cellValue == DBNull.Value ? null : Convert.ChangeType(cellValue, Nullable.GetUnderlyingType(argObjectType)));
                                    }
                                    else
                                    {
                                        // If the type is a not Nullable Value Type, we use the default value when we have DBNull.Value
                                        myObject = (cellValue == DBNull.Value ?
                                            argObjectType == typeof(string) ? null : Activator.CreateInstance(argObjectType)
                                            : Convert.ChangeType(cellValue, argObjectType));
                                    }

                                    // Add the object to the list
                                    objectList.Add(myObject);
                                }
                            }
                            else
                            {
                                // If we have a Reference Type, then create the properties Map
                                // Make a map for properties <-> columns
                                Dictionary<int, Tuple<PropertyInfo, string>> mapDict = new Dictionary<int, Tuple<PropertyInfo, string>>();
                                bool mapCreated = false;

                                // Get the properties for the object
                                PropertyInfo[] objectProperties = argObjectType.GetProperties();

                                // Begin reading
                                while (
                                    #if NET40
                                    myReader.Read()
                                    #else
                                    await myReader.ReadAsync()
                                    #endif
                                    )
                                {
                                    // Check if map is created
                                    if (!mapCreated)
                                    {
                                        FillMap(myReader, mapDict, objectProperties);
                                        mapCreated = true;
                                    }

                                    // Instantiate a new object for filling it from datarow
                                    object rootObject = Activator.CreateInstance(argObjectType);
                                    object curObject = rootObject;

                                    // If we have a Reference Type, use the properties Map to make the assignments
                                    // Make the assignment
                                    foreach (int columnIndex in mapDict.Keys)
                                    {
                                        PropertyInfo mapProp = mapDict[columnIndex].Item1;
                                        object cellValue = myReader.GetValue(columnIndex);

                                        // Don't assign null value
                                        if (cellValue == null || cellValue == DBNull.Value)
                                        {
                                            continue;
                                        }

                                        if (mapProp.DeclaringType != argObjectType && mapProp.DeclaringType != argObjectType.BaseType)
                                        {
                                            // Get Property Path
                                            string propertyPath = mapDict[columnIndex].Item2;

                                            // Sanity check
                                            if (string.IsNullOrWhiteSpace(propertyPath))
                                            {
                                                // The path should not be empty when we don't have the same type in the property and the Declaring Type
                                                // We should check the FillMap!
                                                continue;
                                            }

                                            // Get the parent property array
                                            string[] paths = propertyPath.Split(new string[] { "." }, StringSplitOptions.None);

                                            // Set the current Parent object, which is the root object
                                            object currentParent = rootObject;

                                            for (int i = 0; i < paths.Length; i++)
                                            {
                                                // Try to find the root object's property with the declaring type of the property
                                                PropertyInfo declareObjectProperty = currentParent.GetType().GetProperties().FirstOrDefault(x => x.Name == paths[i]);
                                                if (declareObjectProperty != null)
                                                {
                                                    // If property was found, get it or instantiate it
                                                    object declareObject = declareObjectProperty.GetValue(currentParent, null);
                                                    if (declareObject == null)
                                                    {
                                                        declareObject = Activator.CreateInstance(declareObjectProperty.PropertyType);
                                                        // Set the value to the property of the newly created object
                                                        SetPropertyValueToObject(declareObjectProperty, currentParent, declareObject);
                                                    }
                                                    // Set the current object to the property's object
                                                    curObject = declareObject;
                                                    // Set the parent to the current object
                                                    currentParent = declareObject;
                                                }
                                                else
                                                {
                                                    // if we couldn't find the property, then ignore the cell value
                                                    continue;
                                                }
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
                Debug.WriteLine(string.Format("{0}[GetDataListAsync] Finished executing SQL code (duration: {1})", GetNowString(), myStopWatch.Elapsed));
                // Return our list
                return objectList;
            }
            catch (Exception ex)
            {
                myStopWatch.Stop();
                _LastOperationEllapsedTime = myStopWatch.Elapsed;
                _LastOperationException = ex;
                Debug.WriteLine(string.Format("{0}[GetDataListAsync] Error executing SQL code! (duration: {1})", GetNowString(), myStopWatch.Elapsed));
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
        public static Task<IEnumerable<T>> GetDataListAsync<T>(string argSqlCode, SqlConnection argSqlCon)
        {
            return GetDataListAsync<T>(argSqlCode, argSqlCon, 120, null, null);
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
        public static Task<IEnumerable<T>> GetDataListAsync<T>(string argSqlCode, SqlConnection argSqlCon, int argTimeout)
        {
            return GetDataListAsync<T>(argSqlCode, argSqlCon, argTimeout, null, null);
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
        public static Task<IEnumerable<T>> GetDataListAsync<T>(string argSqlCode, SqlConnection argSqlCon, SqlTransaction argSqlTransaction)
        {
            return GetDataListAsync<T>(argSqlCode, argSqlCon, 120, argSqlTransaction, null);
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
        public static async Task<IEnumerable<T>> GetDataListAsync<T>(string argSqlCode, SqlConnection argSqlCon, int argTimeout, SqlTransaction argSqlTransaction, List<SqlParameter> argSqlParameters)
        {
            // Return our list
            return (IEnumerable<T>)await GetDataListAsync(typeof(T), argSqlCode, argSqlCon, argTimeout, argSqlTransaction, argSqlParameters);
        }

#endregion
    }
}
