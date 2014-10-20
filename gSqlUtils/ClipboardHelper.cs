using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Globalization;
using System.Reflection;
using System.Data;

namespace gSqlUtils
{
    public class ClipboardHelper
    {
        public static String GetClipboardTextFromValueObject(Object argValue)
        {
            return GetClipboardTextFromValueObject(argValue, CultureInfo.InstalledUICulture);
        }

        public static String GetClipboardTextFromValueObject(Object argValue, CultureInfo argCultureInfo)
        {
            if (argValue == null)
            {
                return String.Empty;
            }
            else
            {
                String rawString;
                if (argValue.GetType() == typeof(String))
                {
                    rawString = (String)argValue;
                }
                else if (argValue.GetType() == typeof(Char))
                {
                    rawString = Convert.ToString((Char)argValue);
                }
                else if (argValue.GetType() == typeof(Int16)
                    || argValue.GetType() == typeof(Int32)
                    || argValue.GetType() == typeof(Int64)
                    )
                {
                    rawString = Convert.ToInt64(argValue).ToString(argCultureInfo);
                }
                else if (argValue.GetType() == typeof(UInt16)
                    || argValue.GetType() == typeof(UInt32)
                    || argValue.GetType() == typeof(UInt64)
                    )
                {
                    rawString = Convert.ToUInt64(argValue).ToString(argCultureInfo);
                }
                else if (argValue.GetType() == typeof(Single)
                    || argValue.GetType() == typeof(Double)
                    )
                {
                    rawString = Convert.ToDouble(argValue).ToString(argCultureInfo);
                }
                else if (argValue.GetType() == typeof(Decimal)
                    )
                {
                    rawString = ((Decimal)argValue).ToString(argCultureInfo);
                }
                else if (argValue.GetType() == typeof(DateTime))
                {
                    rawString = ((DateTime)argValue).ToString(argCultureInfo.DateTimeFormat.ShortDatePattern);
                }
                else if (argValue.GetType() == typeof(Boolean))
                {
                    rawString = ((Boolean)argValue).ToString(argCultureInfo);
                }
                else if (argValue.GetType() == typeof(Byte))
                {
                    rawString = ((Byte)argValue).ToString(argCultureInfo);
                }
                else if (argValue is Enum)
                {
                    rawString = Enum.GetName(argValue.GetType(), argValue);
                }
                else if (argValue is IEnumerable)
                {
                    rawString = String.Format("{{ {0} }}", GetClipboardText(argValue, argCultureInfo, false, ","));
                }
                else
                {
                    rawString = String.Format("{{ {0} }}", GetClipboardTextFromObject(argValue, argCultureInfo, ","));
                }
                return rawString
                    .Replace("\t", String.Empty)
                    .Replace("\r", String.Empty)
                    .Replace("\n", String.Empty)
                    .Replace(";", String.Empty);
            }
        }

        public static String GetClipboardText(Object argList, CultureInfo argCultureInfo, Boolean argWithHeaders)
        {
            return GetClipboardText(argList, argCultureInfo, argWithHeaders, "\t");
        }

        public static String GetClipboardText(Object argList, Boolean argWithHeaders)
        {
            return GetClipboardText(argList, CultureInfo.InstalledUICulture, argWithHeaders, "\t");
        }

        public static String GetClipboardText(Object argList, Boolean argWithHeaders, String argCellSeparator)
        {
            return GetClipboardText(argList, CultureInfo.InstalledUICulture, argWithHeaders, argCellSeparator);
        }

        public static String GetClipboardText(Object argList, CultureInfo argCultureInfo, Boolean argWithHeaders, String argCellSeparator)
        {
            // Check the object's type
            if (argList is IList)
            {
                return GetClipboardTextFromIList((IList)argList, argCultureInfo, argWithHeaders, argCellSeparator);
            }
            else if (argList is DataTable)
            {
                return GetClipboardTextFromDataTable((DataTable)argList, argCultureInfo, argWithHeaders, argCellSeparator);
            }
            else if (argList.GetType().IsValueType)
            {
                return GetClipboardTextFromValueObject(argList);
            }
            else
            {
                return String.Empty;
            }
        }

        public static String GetClipboardTextFromIList(IList argList, CultureInfo argCultureInfo, Boolean argWithHeaders, String argCellSeparator)
        {
            // Create the StringBuilder
            StringBuilder finalBuilder = new StringBuilder();
            if (argWithHeaders)
            {
                foreach (PropertyInfo prop in argList.GetType().GetGenericArguments()[0].GetProperties())
                {
                    finalBuilder.AppendFormat("{0}{1}", GetClipboardTextFromValueObject(prop.Name, argCultureInfo).Replace(argCellSeparator, String.Empty), argCellSeparator);
                }
                if (finalBuilder.Length > argCellSeparator.Length - 1)
                {
                    finalBuilder.Length -= argCellSeparator.Length; //remove cell separator character
                }
                finalBuilder.Append("\r\n"); // add \r\n
            }
            foreach (Object item in argList)
            {
                finalBuilder.AppendFormat("{0}\r\n", GetClipboardTextFromObject(item, argCultureInfo, argCellSeparator)); // add \r\n
            }
            if (finalBuilder.Length > 1)
            {
                finalBuilder.Length -= 2; //remove \r\n character
            }
            return finalBuilder.ToString();
        }

        public static String GetClipboardTextFromDataTable(DataTable argDataTable, CultureInfo argCultureInfo, Boolean argWithHeaders, String argCellSeparator)
        {
            // Create the StringBuilder
            StringBuilder finalBuilder = new StringBuilder();
            if (argWithHeaders)
            {
                foreach (DataColumn column in argDataTable.Columns)
                {
                    finalBuilder.AppendFormat("{0}{1}", GetClipboardTextFromValueObject(column.ColumnName, argCultureInfo).Replace(argCellSeparator, String.Empty), argCellSeparator);
                }
                if (finalBuilder.Length > argCellSeparator.Length - 1)
                {
                    finalBuilder.Length -= argCellSeparator.Length; //remove cell separator character
                }
                finalBuilder.Append("\r\n"); // add \r\n
            }
            foreach (DataRow drItem in argDataTable.Rows)
            {
                finalBuilder.AppendFormat("{0}\r\n", GetClipboardTextFromObject(drItem, argCultureInfo, argCellSeparator)); // add \r\n
            }
            if (finalBuilder.Length > 1)
            {
                finalBuilder.Length -= 2; //remove \r\n character
            }
            return finalBuilder.ToString();
        }

        public static String GetClipboardTextFromObject(Object argObject, CultureInfo argCultureInfo, String argCellSeparator)
        {
            StringBuilder finalBuilder = new StringBuilder();
            if (argObject is DataRow)
            {
                foreach (Object obj in ((DataRow)argObject).ItemArray)
                {
                    finalBuilder.AppendFormat("{0}{1}", GetClipboardTextFromValueObject((obj == DBNull.Value) ? null : obj, argCultureInfo).Replace(argCellSeparator, String.Empty), argCellSeparator);
                }
            }
            else
            {
                foreach (PropertyInfo prop in argObject.GetType().GetProperties())
                {
                    finalBuilder.AppendFormat("{0}{1}", GetClipboardTextFromValueObject(prop.GetValue(argObject, null), argCultureInfo).Replace(argCellSeparator, String.Empty), argCellSeparator);
                }
            }
            if (finalBuilder.Length > argCellSeparator.Length - 1)
            {
                finalBuilder.Length -= argCellSeparator.Length; //remove \t character
            }
            return finalBuilder.ToString();
        }
    }
}
