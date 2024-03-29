﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Windows.Forms;
using gpower2.gSqlUtils;

namespace gSqlUtilsTestGUI
{
    public partial class frmSqlUtils : Form
    {
        public frmSqlUtils()
        {
            InitializeComponent();
        }

        private void btnTest1_Click(object sender, EventArgs e)
        {
            try
            {
                // Create new connection to Database
                gSqlHelper _helper = new gSqlHelper(".", "TestDB", "", true);
                // Make a test SQL Code
                String sqlCode = "SELECT * FROM TestNULL";

                // Call the execute sql function                
                //List<Int32?> testList = (List<Int32?>)_helper.GetDataList(typeof(Int32?), sqlCode);
                IEnumerable<Int32?> testList = null;
                IEnumerable<Int32> testList2 = null;

                Task.Factory.StartNew(() => { Debug.WriteLine("testList"); testList = _helper.GetDataList<Int32?>(sqlCode); });
                //testList = _helper.GetDataList<Int32?>(sqlCode);

                Task.Factory.StartNew(() => { Debug.WriteLine("testList2"); testList2 = _helper.GetDataList<Int32>(sqlCode); });
                //testList2 = _helper.GetDataList<Int32>(sqlCode);

                Debug.WriteLine("testObject");
                Int32 testObject = _helper.GetDataObject<Int32>(sqlCode);
                string testString = _helper.GetDataValue<string>(sqlCode);

                grdResults.DataSource = testList;
                grdResults.Refresh();
                using (SqlConnection sqlCon = SqlHelperStatic.CreateSqlConnection(".", "TestDB"))
                {
                    DataTable dt = SqlHelperStatic.GetDataTable(sqlCode, sqlCon);
                    Clipboard.SetText(ClipboardExtensions.GetClipboardText(dt, true));
                    Debug.WriteLine("returned rows: " + dt.Rows.Count);
                }
            }

            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                MessageBox.Show("An error has occured!\r\n" + ex.Message, "Error!", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private async void btnTestAsync_Click(object sender, EventArgs e)
        {
            try
            {
                // Create new connection to Database
                gSqlHelper _helper = new gSqlHelper(".", "TestDB", "", true);
                // Make a test SQL Code
                String sqlCode = "SELECT * FROM TestNULL";

                // Call the execute sql function                
                //List<Int32?> testList = (List<Int32?>)_helper.GetDataList(typeof(Int32?), sqlCode);
                IEnumerable<Int32?> testList = null;
                IEnumerable<Int32> testList2 = null;

                _ = Task.Run(async () => { Debug.WriteLine("testList"); testList = await _helper.GetDataListAsync<Int32?>(sqlCode); });
                //testList = _helper.GetDataList<Int32?>(sqlCode);

                _ = Task.Run(async () => { Debug.WriteLine("testList2"); testList2 = await _helper.GetDataListAsync<Int32>(sqlCode); });
                //testList2 = _helper.GetDataList<Int32>(sqlCode);

                Debug.WriteLine("testObject");
                Int32 testObject = await _helper.GetDataObjectAsync<Int32>(sqlCode);
                string testString = await _helper.GetDataValueAsync<string>(sqlCode);

                grdResults.DataSource = testList;
                grdResults.Refresh();
                //using (SqlConnection sqlCon = SqlHelperStatic.CreateSqlConnection(".", "TestDB"))
                //{
                //    DataTable dt = await SqlHelperStatic.GetDataTableAsync(sqlCode, sqlCon);
                //    Clipboard.SetText(ClipboardExtensions.GetClipboardText(dt, true));
                //    Debug.WriteLine("returned rows: " + dt.Rows.Count);
                //}
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                MessageBox.Show("An error has occured!\r\n" + ex.Message, "Error!", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }
    }
}
