using System;
using System.Data.SqlClient;
using System.Diagnostics;

namespace gpower2.gSqlUtils
{
    public static partial class SqlHelperStatic
    {
        #region "CreateSqlConnection"

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: string.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: string.Empty)</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, string argUserId, string argPassword)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, false, 8000, argUserId, argPassword, string.Empty, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: string.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: string.Empty)</param>
        /// <param name="argPersistSecurityInfo">The flag that indicates if security-sensitive information, such as the password, is not returned as part of the connection if the connection is open or has ever been in an open state (Default: false)</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, string argUserId, string argPassword, bool argPersistSecurityInfo)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, false, 8000, argUserId, argPassword, string.Empty, false, argPersistSecurityInfo);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: string.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: string.Empty)</param>
        /// <param name="argPersistSecurityInfo">The flag that indicates if security-sensitive information, such as the password, is not returned as part of the connection if the connection is open or has ever been in an open state (Default: false)</param>
        /// <param name="argMultipleActiveResultSets">The flag that indicates if an application can maintain multiple active result sets (MARS) (Default: false).</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, string argUserId, string argPassword, bool argPersistSecurityInfo, bool argMultipleActiveResultSets)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, false, 8000, argUserId, argPassword, string.Empty, argMultipleActiveResultSets, argPersistSecurityInfo);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: string.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: string.Empty)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, string argUserId, string argPassword,
            string argApplicationName)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, false, 8000, argUserId, argPassword, argApplicationName, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: string.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: string.Empty)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <param name="argPersistSecurityInfo">The flag that indicates if security-sensitive information, such as the password, is not returned as part of the connection if the connection is open or has ever been in an open state (Default: false)</param>
        /// <param name="argMultipleActiveResultSets">The flag that indicates if an application can maintain multiple active result sets (MARS) (Default: false).</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, string argUserId, string argPassword,
            string argApplicationName, bool argPersistSecurityInfo, bool argMultipleActiveResultSets)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, false, 8000, argUserId, argPassword, argApplicationName, argMultipleActiveResultSets, argPersistSecurityInfo);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: string.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: string.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, string argUserId, string argPassword,
            int argConnectTimeout)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, false, 8000, argUserId, argPassword, string.Empty, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: string.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: string.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, string argUserId, string argPassword,
            int argConnectTimeout, string argApplicationName)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, false, 8000, argUserId, argPassword, argApplicationName, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: string.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: string.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <param name="argPacketSize">The size (in bytes) of the network packets used to communicate with an instance of SQL Server (Default: 8000)</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, string argUserId, string argPassword,
            int argConnectTimeout, int argPacketSize)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, false, argPacketSize, argUserId, argPassword, string.Empty, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: string.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: string.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <param name="argPacketSize">The size (in bytes) of the network packets used to communicate with an instance of SQL Server (Default: 8000)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, string argUserId, string argPassword,
            int argConnectTimeout, int argPacketSize, string argApplicationName)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, false, argPacketSize, argUserId, argPassword, argApplicationName, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, true, 8000, string.Empty, string.Empty, string.Empty, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, string argApplicationName)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, true, 8000, string.Empty, string.Empty, argApplicationName, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <param name="argMultipleActiveResultSets">The flag that indicates if an application can maintain multiple active result sets (MARS) (Default: false).</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, string argApplicationName, bool argMultipleActiveResultSets)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, 15, true, 8000, string.Empty, string.Empty, argApplicationName, argMultipleActiveResultSets, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, int argConnectTimeout)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, true, 8000, string.Empty, string.Empty, string.Empty, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, int argConnectTimeout, string argApplicationName)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, true, 8000, string.Empty, string.Empty, argApplicationName, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <param name="argPacketSize">The size (in bytes) of the network packets used to communicate with an instance of SQL Server (Default: 8000)</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, int argConnectTimeout, int argPacketSize)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, true, argPacketSize, string.Empty, string.Empty, string.Empty, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <param name="argPacketSize">The size (in bytes) of the network packets used to communicate with an instance of SQL Server (Default: 8000)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, int argConnectTimeout, int argPacketSize, string argApplicationName)
        {
            return CreateSqlConnection(argDataSource, argInitialCatalog, argConnectTimeout, true, argPacketSize, string.Empty, string.Empty, argApplicationName, false, false);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="argDataSource">The name or network adress of the instance of SQL Server to connect to (Default: string.Empty)</param>
        /// <param name="argInitialCatalog">The name of the Database associated with the connection (Default: string.Empty)</param>
        /// <param name="argConnectTimeout">The length of time (in seconds) to wait for a connection to the server (Default: 15)</param>
        /// <param name="argIntegratedSecurity">The flag that indicates whether User ID and Password are specified in the connection 
        /// or the current Windows account credentials are used for authentication (Default: false)</param>
        /// <param name="argPacketSize">The size (in bytes) of the network packets used to communicate with an instance of SQL Server (Default: 8000)</param>
        /// <param name="argUserId">The user ID to be used when connecting to SQL Server (Default: string.Empty)</param>
        /// <param name="argPassword">The password for the SQL Server account (Default: string.Empty)</param>
        /// <param name="argApplicationName">The name of the application associated with the connection string (Default: ".Net SqlClient Data Provider")</param>
        /// <param name="argMultipleActiveResultSets">The flag that indicates if an application can maintain multiple active result sets (MARS) (Default: false).</param>
        /// <param name="argPersistSecurityInfo">The flag that indicates if security-sensitive information, such as the password, is not returned as part of the connection if the connection is open or has ever been in an open state (Default: false)</param>
        /// <returns></returns>
        public static SqlConnection CreateSqlConnection(string argDataSource, string argInitialCatalog, int argConnectTimeout,
            bool argIntegratedSecurity, int argPacketSize, string argUserId, string argPassword, string argApplicationName,
            bool argMultipleActiveResultSets, bool argPersistSecurityInfo, bool argClearPool = false)
        {
            Stopwatch sw = new Stopwatch();
            _LastOperationException = null;
            _LastOperationEllapsedTime = new TimeSpan();
            try
            {
                SqlConnectionStringBuilder myBuilder = new SqlConnectionStringBuilder();

                // Default value: string.Empty
                // This property corresponds to the "Data Source", "server", "address", "addr", and "network address" keys within the connection string. 
                // Regardless of which of these values has been supplied within the supplied connection string, the connection string created by the 
                // SqlConnectionStringBuilder will use the well-known "Data Source" key.
                myBuilder.DataSource = argDataSource;

                // Default value: string.Empty
                // This property corresponds to the "Initial Catalog" and "database" keys within the connection string.
                myBuilder.InitialCatalog = argInitialCatalog;

                // Default value: 15
                // This property corresponds to the "Connect Timeout", "connection timeout", and "timeout" keys within the connection string.
                myBuilder.ConnectTimeout = argConnectTimeout;

                // Default value: false
                // This property corresponds to the "Integrated Security" and "trusted_connection" keys within the connection string.
                myBuilder.IntegratedSecurity = argIntegratedSecurity;

                // Default value: 8000 
                // This property corresponds to the "Packet Size" key within the connection string.
                myBuilder.PacketSize = argPacketSize;

                // Default value: ".Net SqlClient Data Provider"
                // This property corresponds to the "Application Name" key within the connection string.
                if (!string.IsNullOrWhiteSpace(argApplicationName))
                {
                    myBuilder.ApplicationName = argApplicationName;
                }

                // Default Value: 100
                // This property corresponds to the "Max Pool Size" key within the connection string.
                myBuilder.MaxPoolSize = 300;

                // Default Value: false
                // This property corresponds to the "MultipleActiveResultSets" key within the connection string.
                myBuilder.MultipleActiveResultSets = argMultipleActiveResultSets;

                // Check if integrated security is true and in that case, leave UserID and Password fields empty
                if (!argIntegratedSecurity)
                {
                    // Default value: string.Empty
                    // This property corresponds to the "User ID", "user", and "uid" keys within the connection string.
                    myBuilder.UserID = argUserId;

                    // Default value: string.Empty
                    // This property corresponds to the "Password" and "pwd" keys within the connection string.
                    myBuilder.Password = argPassword;

                    // Default Value: false
                    // This property corresponds to the "Persist Security Info" and "persistsecurityinfo" keys within the connection string.
                    myBuilder.PersistSecurityInfo = argPersistSecurityInfo;
                }

                Debug.WriteLine(string.Format("{0}[CreateSqlConnection] Starting to connect to SQL server:", GetNowString()));
                Debug.WriteLine(myBuilder.ConnectionString);

                // Return a new connection with the connection string of the builder
                sw.Start();
                SqlConnection myCon = null;
                lock (_ThreadAnchor)
                {
                    myCon = new SqlConnection(myBuilder.ConnectionString);
                }
                sw.Stop();

                _LastOperationEllapsedTime = sw.Elapsed;

                Debug.WriteLine(string.Format("{0}[CreateSqlConnection] Finished connecting to SQL server (duration: {1})", GetNowString(), sw.Elapsed));

                if (argClearPool)
                {
                    // Clears the Connection pool associated with this connection
                    SqlConnection.ClearPool(myCon);
                }

                return myCon;
            }
            catch (Exception ex)
            {
                sw.Stop();
                _LastOperationException = ex;
                _LastOperationEllapsedTime = sw.Elapsed;
                Debug.WriteLine(string.Format("{0}[CreateSqlConnection] Error connecting to SQL server! (duration: {1})", GetNowString(), sw.Elapsed));
                Debug.WriteLine(ex);
                throw;
            }
        }

        #endregion
    }
}
