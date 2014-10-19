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
using gSqlUtils;

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
                // SqlConnection sqlCon = new SqlConnection("data source=RCS10;initial catalog=rescom;packet size=4096;integrated security=SSPI;persist security info=False");
                gSqlHelper _helper = new gSqlHelper(".", "TestDB");
                // Make a test SQL Code
                String sqlCode = "SELECT * FROM TestNULL";

                // Call the execute sql function
                //grdResults.DataSource = SqlHelperStatic.GetDataList(typeof(Test), sqlCode, sqlCon);
                grdResults.DataSource = _helper.GetDataList(typeof(Test), sqlCode);
                _helper.CloseConnection();
                grdResults.Refresh();
                SqlConnection sqlCon = gSqlUtils.SqlHelperStatic.CreateSqlConnection(".", "TestDB");
                DataTable dt = SqlHelperStatic.GetDataTable(sqlCode, sqlCon);

                Debug.WriteLine("returned rows: " + dt.Rows.Count);
            }

            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                MessageBox.Show("An error has occured!\r\n" + ex.Message, "Error!", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }
    }
}
