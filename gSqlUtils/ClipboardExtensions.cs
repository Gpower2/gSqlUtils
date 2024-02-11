using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;

namespace gpower2.gSqlUtils
{
    public static class ClipboardExtensions
    {
        public static string GetClipboardText(this object argValue)
        {
            return GetClipboardTextFromValueObject(argValue, CultureInfo.InstalledUICulture);
        }

        public static string GetClipboardText(this object argValue, CultureInfo argCultureInfo)
        {
            return GetClipboardTextFromValueObject(argValue, argCultureInfo);
        }

        public static string GetClipboardText(this object argList, CultureInfo argCultureInfo, bool argWithHeaders)
        {
            return GetClipboardText(argList, argCultureInfo, argWithHeaders, "\t");
        }

        public static string GetClipboardText(this object argList, bool argWithHeaders)
        {
            return GetClipboardText(argList, CultureInfo.InstalledUICulture, argWithHeaders, "\t");
        }

        public static string GetClipboardText(this object argList, bool argWithHeaders, string argCellSeparator)
        {
            return GetClipboardText(argList, CultureInfo.InstalledUICulture, argWithHeaders, argCellSeparator);
        }

        public static string GetClipboardText(this object argList, CultureInfo argCultureInfo, bool argWithHeaders, string argCellSeparator)
        {
            // Check the object's type
            if (argList is IEnumerable list)
            {
                return GetClipboardTextFromIEnumerable(list, argCultureInfo, argWithHeaders, argCellSeparator);
            }
            else if (argList.GetType()
                 .GetInterfaces()
                 .Any(t => t.IsGenericType
                        && t.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
            {
                return GetClipboardTextFromIEnumerable(argList as IEnumerable<object>, argCultureInfo, argWithHeaders, argCellSeparator);
            }
            else if (argList is DataTable dt)
            {
                return GetClipboardTextFromDataTable(dt, argCultureInfo, argWithHeaders, argCellSeparator);
            }
            else if (argList.GetType().IsValueType)
            {
                return GetClipboardText(argList);
            }
            else
            {
                return GetClipboardTextFromObject(argList, argCultureInfo, argCellSeparator, argWithHeaders);
            }
        }

        private static string GetClipboardTextFromValueObject(object argValue, CultureInfo argCultureInfo,
            string argIntFormat = null, string argDecimalFormat = null, string argDateFormat = null)
        {
            if (argValue == null)
            {
                return string.Empty;
            }

            string rawString;
            if (argValue.GetType() == typeof(string))
            {
                rawString = (string)argValue;
            }
            else if (argValue.GetType() == typeof(char))
            {
                rawString = ((char)argValue).ToString();
            }
            else if (argValue.GetType() == typeof(short)
                || argValue.GetType() == typeof(int)
                || argValue.GetType() == typeof(long)
                )
            {
                if (string.IsNullOrWhiteSpace(argIntFormat))
                {
                    rawString = Convert.ToInt64(argValue).ToString(argCultureInfo);
                }
                else
                {
                    rawString = Convert.ToInt64(argValue).ToString(argIntFormat, argCultureInfo);
                }
            }
            else if (argValue.GetType() == typeof(ushort)
                || argValue.GetType() == typeof(uint)
                || argValue.GetType() == typeof(ulong)
                )
            {
                if (string.IsNullOrWhiteSpace(argIntFormat))
                {
                    rawString = Convert.ToUInt64(argValue).ToString(argCultureInfo);
                }
                else
                {
                    rawString = Convert.ToInt64(argValue).ToString(argIntFormat, argCultureInfo);
                }
            }
            else if (argValue.GetType() == typeof(float))
            {
                if (string.IsNullOrWhiteSpace(argDecimalFormat))
                {
                    rawString = ((float)argValue).ToString(argCultureInfo);
                }
                else
                {
                    rawString = ((float)argValue).ToString(argDecimalFormat, argCultureInfo);
                }
            }
            else if (argValue.GetType() == typeof(double))
            {
                if (string.IsNullOrWhiteSpace(argDecimalFormat))
                {
                    rawString = ((double)argValue).ToString(argCultureInfo);
                }
                else
                {
                    rawString = ((double)argValue).ToString(argDecimalFormat, argCultureInfo);
                }
            }
            else if (argValue.GetType() == typeof(decimal))
            {
                if (string.IsNullOrWhiteSpace(argDecimalFormat))
                {
                    rawString = ((decimal)argValue).ToString(argCultureInfo);
                }
                else
                {
                    rawString = ((decimal)argValue).ToString(argDecimalFormat, argCultureInfo);
                }
            }
            else if (argValue.GetType() == typeof(DateTime))
            {
                if (string.IsNullOrWhiteSpace(argDateFormat))
                {
                    rawString = ((DateTime)argValue).ToString(argCultureInfo.DateTimeFormat.ShortDatePattern);
                }
                else
                {
                    rawString = ((DateTime)argValue).ToString(argDateFormat, argCultureInfo);
                }
            }
            else if (argValue.GetType() == typeof(bool))
            {
                rawString = ((bool)argValue).ToString(argCultureInfo);
            }
            else if (argValue.GetType() == typeof(byte))
            {
                rawString = ((byte)argValue).ToString(argCultureInfo);
            }
            else if (argValue is Enum)
            {
                rawString = Enum.GetName(argValue.GetType(), argValue);
            }
            else if (argValue is IEnumerable)
            {
                rawString = string.Format("{{ {0} }}", GetClipboardText(argValue, argCultureInfo, false, ","));
            }
            else if (argValue.GetType()
                 .GetInterfaces()
                 .Any(t => t.IsGenericType
                        && t.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
            {
                rawString = string.Format("{{ {0} }}", GetClipboardText(argValue as IEnumerable<object>, argCultureInfo, false, ","));
            }
            else
            {
                rawString = string.Format("{{ {0} }}", GetClipboardTextFromObject(argValue, argCultureInfo, ","));
            }

            return rawString
                .Replace("\t", string.Empty)
                .Replace("\r", string.Empty)
                .Replace("\n", string.Empty)
                .Replace(";", string.Empty);
        }

        private static string GetClipboardTextFromIEnumerable(IEnumerable argList, CultureInfo argCultureInfo, bool argWithHeaders, string argCellSeparator)
        {
            StringBuilder finalBuilder = new StringBuilder();

            if (argWithHeaders)
            {
                finalBuilder.AppendLine(
                    string.Join(
                        argCellSeparator,
                        argList.GetType().GetGenericArguments()[0].GetProperties()
                            .Select(prop =>
                                GetClipboardTextFromValueObject(prop.Name, argCultureInfo)
                                .Replace(argCellSeparator, string.Empty)
                            )
                    )
                );
            }

            finalBuilder.Append(
                string.Join(
                    Environment.NewLine,
                    argList.OfType<object>().Select(item =>
                        GetClipboardTextFromObject(item, argCultureInfo, argCellSeparator)
                    )
                )
            );

            return finalBuilder.ToString();
        }

        private static string GetClipboardTextFromDataTable(DataTable argDataTable, CultureInfo argCultureInfo, bool argWithHeaders, string argCellSeparator)
        {
            StringBuilder finalBuilder = new StringBuilder();

            if (argWithHeaders)
            {
                finalBuilder.AppendLine(
                    string.Join(
                        argCellSeparator,
                        argDataTable.Columns.OfType<DataColumn>()
                            .Select(column =>
                                GetClipboardTextFromValueObject(column.ColumnName, argCultureInfo)
                                .Replace(argCellSeparator, string.Empty)
                            )
                    )
                );
            }

            finalBuilder.Append(
                string.Join(
                    Environment.NewLine,
                    argDataTable.Rows.OfType<DataRow>()
                        .Select(drItem =>
                            GetClipboardTextFromObject(drItem, argCultureInfo, argCellSeparator)
                        )
                )
            );

            return finalBuilder.ToString();
        }

        private static string GetClipboardTextFromObject(object argObject, CultureInfo argCultureInfo, string argCellSeparator, bool argWithHeaders = false)
        {
            if (argObject is DataRow dr)
            {
                return string.Join(
                    argCellSeparator,
                    dr.ItemArray.Select(obj =>
                        GetClipboardTextFromValueObject((obj == DBNull.Value) ? null : obj, argCultureInfo)
                            .Replace(argCellSeparator, string.Empty)
                    )
                );
            }
            else
            {
                StringBuilder finalBuilder = new StringBuilder();

                if (argWithHeaders)
                {
                    finalBuilder.AppendLine(
                        string.Join(
                            argCellSeparator,
                            argObject.GetType().GetProperties()
                                .Select(prop =>
                                    GetClipboardTextFromValueObject(prop.Name, argCultureInfo)
                                    .Replace(argCellSeparator, string.Empty)
                                )
                        )
                    );
                }

                finalBuilder.Append(
                    string.Join(
                        argCellSeparator,
                        argObject.GetType().GetProperties().Select(prop =>
                            GetClipboardTextFromValueObject(prop.GetValue(argObject, null), argCultureInfo)
                                .Replace(argCellSeparator, string.Empty)
                        )
                    )
                );

                return finalBuilder.ToString();
            }
        }
    }
}
