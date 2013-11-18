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
                SqlConnection sqlCon = gSqlUtils.SqlHelperStatic.CreateSqlConnection("RCS10", "resCom");

                // Make a test SQL Code
                String sqlCode = "SELECT 1";

                // Call the execute sql function
                Int32 affectedRows = SqlHelperStatic.ExecuteSql(sqlCode, sqlCon);

                Debug.WriteLine("affected rows: " + affectedRows);
            }

            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                MessageBox.Show("An error has occured!\r\n" + ex.Message, "Error!", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }
    }
}
