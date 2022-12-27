using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace gpower2.gSqlUtils
{
    public static partial class SqlHelperStatic
    {
        /// <summary>
        /// Fills a Dictionary that maps object properties to column index of a DataReader
        /// </summary>
        /// <param name="myReader"></param>
        /// <param name="mapDict"></param>
        /// <param name="objectProperties"></param>
        /// <param name="argRootPropertyName"></param>
        private static void FillMap(SqlDataReader myReader, Dictionary<int, Tuple<PropertyInfo, string>> mapDict, PropertyInfo[] objectProperties, string argRootPropertyName = "")
        {
            // for each property of the object, try to find a column of the same name
            foreach (PropertyInfo myProp in objectProperties)
            {
                // Only for properties that can be written to
                if (myProp.CanWrite)
                {
                    // Check for reference type
                    if (!myProp.PropertyType.IsValueType && myProp.PropertyType != typeof(string)
                        && !myProp.PropertyType.IsArray
                        && !(myProp.PropertyType.IsGenericType && myProp.PropertyType.GetGenericTypeDefinition() != typeof(Nullable<>)))
                    {
                        // Check if we have a property of the same declaring type
                        // In that case, we only support one depth level
                        if (myProp.PropertyType == myProp.DeclaringType && NumberOfOccurences(argRootPropertyName, ".") > 1)
                        {
                            continue;
                        }
                        // If reference type then try to find if we have columns for its properties
                        FillMap(myReader, mapDict, myProp.PropertyType.GetProperties(),
                            string.IsNullOrWhiteSpace(argRootPropertyName) ? myProp.Name : string.Format("{0}.{1}", argRootPropertyName, myProp.Name));
                        continue;
                    }

                    // try to find a column with the same property name
                    // Remove '_' character from column name
                    // Make the comparison case insensitive
                    for (int curColumn = 0; curColumn < myReader.FieldCount; curColumn++)
                    {
                        // Check if column is already mapped
                        if (mapDict.Keys.Contains(curColumn))
                        {
                            // continue to next column
                            continue;
                        }

                        // check column name with property name
                        if (myReader.GetName(curColumn).Replace("_", "").Replace(" ", "").Trim().ToLower().Equals( // Column Name
                            (string.IsNullOrWhiteSpace(argRootPropertyName) ? myProp.Name : string.Format("{0}_{1}", argRootPropertyName, myProp.Name)).Replace("_", "").ToLower()) // Property Name
                            )
                        {
                            // Add the map entry
                            mapDict.Add(curColumn, new Tuple<PropertyInfo, string>(myProp, argRootPropertyName));
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
        private static void SetPropertyValueToObject(PropertyInfo argProp, object argObject, object argValue)
        {
            // Check for Nullable<T> properties
            if (argProp.PropertyType.IsGenericType && argProp.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                // If the type is Nullable<T> we change the value to the Nullable<T> equivalent
                argProp.SetValue(argObject, argValue == DBNull.Value ? null : Convert.ChangeType(argValue,
                    Nullable.GetUnderlyingType(argProp.PropertyType)), null);
            }
            else if (argProp.PropertyType.IsValueType || argProp.PropertyType == typeof(string))
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
    }
}
