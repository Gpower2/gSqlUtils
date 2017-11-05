using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using System.Data.Sql;
using System.Data.SqlClient;
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
                gSqlHelper _helper = new gSqlHelper(".", "TestDB");
                // Make a test SQL Code
                String sqlCode = "SELECT * FROM TestNULL";

                // Call the execute sql function                
                //List<Int32?> testList = (List<Int32?>)_helper.GetDataList(typeof(Int32?), sqlCode);
                IList<Int32?> testList = _helper.GetDataList<Int32?>(sqlCode);
                Int32 testObject = _helper.GetDataObject<Int32>(sqlCode);
                grdResults.DataSource = testList;
                _helper.CloseConnection();
                grdResults.Refresh();
                using (SqlConnection sqlCon = SqlHelperStatic.CreateSqlConnection(".", "TestDB"))
                {
                    DataTable dt = SqlHelperStatic.GetDataTable(sqlCode, sqlCon);
                    Clipboard.SetText(ClipboardHelper.GetClipboardText(dt, true));
                    Debug.WriteLine("returned rows: " + dt.Rows.Count);
                }
            }

            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                MessageBox.Show("An error has occured!\r\n" + ex.Message, "Error!", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }
    }
}
