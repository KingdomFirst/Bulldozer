using Microsoft.Win32;
using Rock;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Effects;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer
{
    /// <summary>
    /// Interaction logic for ConnectPage.xaml
    /// </summary>
    public partial class ConnectPage : Page, INotifyPropertyChanged
    {
        #region Fields

        private BulldozerComponent bulldozer;

        private SqlConnector sqlConnector;

        private ConnectionString existingConnection;

        /// <summary>
        /// Gets the supported rock version.
        /// </summary>
        /// <value>
        /// The supported rock version.
        /// </value>
        public string SupportedRockVersion
        {
            get
            {
                return string.Format( "Using Rock.dll v{0}", App.RockVersion );
            }
        }

        /// <summary>
        /// Gets or sets the current connection.
        /// </summary>
        /// <value>
        /// The current connection.
        /// </value>
        public ConnectionString CurrentConnection
        {
            get
            {
                return existingConnection;
            }
            set
            {
                existingConnection = value;
                App.ExistingConnection = value; //for back and forth, restore from session
                RaisePropertyChanged( "Connection" );
                RaisePropertyChanged( "ConnectionDescribed" );
                RaisePropertyChanged( "OkToProceed" );
            }
        }

        private string _ConnectionDescribed = string.Empty;

        /// <summary>
        /// Highlights the current connection on the connect page.
        /// </summary>
        /// <value>
        /// The connection described.
        /// </value>
        public string ConnectionDescribed
        {
            get
            {
                if ( existingConnection != null )
                {
                    _ConnectionDescribed = "(Current Destination: " + existingConnection.Server + ":" + existingConnection.Database + ")";
                }
                return _ConnectionDescribed;
            }
        }

        public bool OkToProceed
        {
            get
            {
                if ( CurrentConnection == null || bulldozer == null || !CurrentConnection.IsValid() )
                    return false;
                return true;
            }
        }

        private string _DbConnectMsg = "Could not connect to database. Please verify the server is online.";

        public string DbConnectMsg
        {
            get
            {
                return _DbConnectMsg;
            }
            set
            {
                _DbConnectMsg = value;
                RaisePropertyChanged( "DbConnectMsg" );
            }
        }

        private IEnumerable<BulldozerComponent> _bulldozerTypes = new List<BulldozerComponent>();

        /// <summary>
        /// Gets or sets the bulldozer types.
        /// </summary>
        /// <value>
        /// The bulldozer types.
        /// </value>
        [ImportMany( typeof( BulldozerComponent ) )]
        public IEnumerable<BulldozerComponent> BulldozerTypes
        {
            get
            {
                if ( _bulldozerTypes == null )
                {
                    _bulldozerTypes = new List<BulldozerComponent>();
                }

                return _bulldozerTypes;
            }

            set
            {
                if ( _bulldozerTypes == value )
                    return;

                _bulldozerTypes = value;
                RaisePropertyChanged( "BulldozerTypes" );
            }
        }

        private BulldozerComponent _selectedImportType = null;

        /// <summary>
        /// Gets or sets the type of the selected import.
        /// </summary>
        /// <value>
        /// The type of the selected import.
        /// </value>
        public BulldozerComponent SelectedImportType
        {
            get
            {
                return _selectedImportType;
            }
            set
            {
                if ( _selectedImportType == value )
                    return;
                _selectedImportType = value;
                RaisePropertyChanged( "SelectedImportType" );
            }
        }

        /// <summary>
        /// Gets the name of the selected type.
        /// </summary>
        /// <param name="src">The source.</param>
        /// <param name="propName">Name of the property.</param>
        /// <returns></returns>
        public static object GetPropValue( object src, string propName )
        {
            if ( src.GetType().GetProperty( propName ) != null )
            {
                return src.GetType().GetProperty( propName ).GetValue( src, null );
            }
            return string.Empty;
        }

        public event PropertyChangedEventHandler PropertyChanged;

        /// <summary>
        /// Raises the property changed.
        /// </summary>
        /// <param name="propertyName">Name of the property.</param>
        private void RaisePropertyChanged( string propertyName )
        {
            if ( PropertyChanged != null )
            {
                PropertyChanged( this, new PropertyChangedEventArgs( propertyName ) );
            }
        }

        #endregion Fields

        #region Initializer Methods

        /// <summary>
        /// Initializes a new instance of the <see cref="FrontEndLoader"/> class.
        /// </summary>
        public ConnectPage()
        {
            InitializeComponent();
            LoadBulldozerTypes();
            var updateTypeVals = Enum.GetValues( typeof( ImportUpdateType ) )
                                     .Cast<ImportUpdateType>()
                                     .Select( v => v.ToString() )
                                     .ToList();
            lstUpdateMode.ItemsSource = updateTypeVals;

            if ( BulldozerTypes.Any() )
            {
                SelectedImportType = BulldozerTypes.FirstOrDefault();
                InitializeDBConnection();
            }
            else
            {
                btnNext.Visibility = Visibility.Hidden;
                lblHeader.Visibility = Visibility.Hidden;
                lblNoData.Visibility = Visibility.Visible;
            }

            DataContext = this;

            tbPersonChunk.Text = ConfigurationManager.AppSettings["PersonChunkSize"];
            tbAttendanceChunk.Text = ConfigurationManager.AppSettings["AttendanceChunkSize"];
            tbDefaultChunk.Text = ConfigurationManager.AppSettings["DefaultChunkSize"];
            tbFKPrefix.Text = ConfigurationManager.AppSettings["ImportInstanceFKPrefix"];
            lstUpdateMode.SelectedValue = ConfigurationManager.AppSettings["ImportUpdateMode"];
            cbUseRockCampus.IsChecked = ConfigurationManager.AppSettings["UseRockCampusIds"].AsBoolean();
        }

        /// <summary>
        /// Loads the bulldozer types as MEF components.
        /// </summary>
        public void LoadBulldozerTypes()
        {
            var extensionFolder = ConfigurationManager.AppSettings["ExtensionPath"];
            var catalog = new AggregateCatalog();
            if ( Directory.Exists( extensionFolder ) )
            {
                catalog.Catalogs.Add( new DirectoryCatalog( extensionFolder, "Bulldozer.*.dll" ) );
            }

            var currentDirectory = Path.GetDirectoryName( System.Windows.Forms.Application.ExecutablePath );
            catalog.Catalogs.Add( new DirectoryCatalog( currentDirectory, "Bulldozer.*.dll" ) );

            try
            {
                var container = new CompositionContainer( catalog, true );
                container.ComposeParts( this );
            }
            catch ( Exception ex )
            {
                App.LogException( "Components", ex.ToString() );
            }
        }

        /// <summary>
        /// Initializes the database connection.
        /// </summary>
        private void InitializeDBConnection()
        {
            if ( App.ExistingConnection != null )
            {
                CurrentConnection = App.ExistingConnection;
            }
            else
            {
                //initialize from app.config
                var appConfig = ConfigurationManager.OpenExeConfiguration( ConfigurationUserLevel.None );
                var rockConnectionString = appConfig.ConnectionStrings.ConnectionStrings["RockContext"];

                if ( rockConnectionString != null )
                {
                    CurrentConnection = new ConnectionString( rockConnectionString.ConnectionString );
                }
            }
        }

        #endregion Initializer Methods

        #region Events

        /// <summary>
        /// Handles the Click event of the btnUpload control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="RoutedEventArgs"/> instance containing the event data.</param>
        private void btnUpload_Click( object sender, RoutedEventArgs e )
        {
            Mouse.OverrideCursor = Cursors.Wait;

            BackgroundWorker bwLoadPreview = new BackgroundWorker();
            bwLoadPreview.DoWork += bwPreview_DoWork;
            bwLoadPreview.RunWorkerCompleted += bwPreview_RunWorkerCompleted;
            bwLoadPreview.RunWorkerAsync( lstDatabaseTypes.SelectedValue.ToString() );
        }

        /// <summary>
        /// Handles the Click event of the btnConnect control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="RoutedEventArgs"/> instance containing the event data.</param>
        private void btnConnect_Click( object sender, RoutedEventArgs e )
        {
            sqlConnector = new SqlConnector();
            var modalBorder = new Border();
            var connectPanel = new StackPanel();
            var buttonPanel = new StackPanel();

            // set UI effects
            modalBorder.BorderBrush = (Brush)FindResource( "headerBackground" );
            modalBorder.CornerRadius = new CornerRadius( 5 );
            modalBorder.BorderThickness = new Thickness( 5 );
            modalBorder.Padding = new Thickness( 5 );
            buttonPanel.HorizontalAlignment = HorizontalAlignment.Right;
            buttonPanel.Orientation = Orientation.Horizontal;
            this.OpacityMask = new SolidColorBrush( Colors.White );
            this.Effect = new BlurEffect();

            sqlConnector.ConnectionString = CurrentConnection;
            connectPanel.Children.Add( sqlConnector );

            var okBtn = new Button();
            okBtn.Content = "Ok";
            okBtn.IsDefault = true;
            okBtn.Margin = new Thickness( 0, 0, 5, 0 );
            okBtn.Click += btnOk_Click;
            okBtn.Style = (Style)FindResource( "buttonStylePrimary" );

            var cancelBtn = new Button();
            cancelBtn.Content = "Cancel";
            cancelBtn.IsCancel = true;
            cancelBtn.Style = (Style)FindResource( "buttonStyle" );

            buttonPanel.Children.Add( okBtn );
            buttonPanel.Children.Add( cancelBtn );
            connectPanel.Children.Add( buttonPanel );
            modalBorder.Child = connectPanel;

            var contentPanel = new StackPanel();
            contentPanel.Children.Add( modalBorder );

            var connectWindow = new Window();
            connectWindow.Content = contentPanel;
            connectWindow.Owner = Window.GetWindow( this );
            connectWindow.ShowInTaskbar = false;
            connectWindow.Background = (Brush)FindResource( "windowBackground" );
            connectWindow.WindowStyle = WindowStyle.None;
            connectWindow.ResizeMode = ResizeMode.NoResize;
            connectWindow.SizeToContent = SizeToContent.WidthAndHeight;
            connectWindow.WindowStartupLocation = WindowStartupLocation.CenterOwner;
            var windowConnected = connectWindow.ShowDialog() ?? false;

            if ( CurrentConnection.Database.Contains( "failed" ) )
            {
                CurrentConnection.Database = string.Empty;
            }

            // Undo graphical effects
            this.OpacityMask = null;
            this.Effect = null;
            RaisePropertyChanged( "OkToProceed" );
            RaisePropertyChanged( "ConnectionDescribed" );

            if ( windowConnected && CurrentConnection != null && CurrentConnection.IsValid() )
            {
                lblDbConnect.Style = (Style)FindResource( "labelStyleSuccess" );
                DbConnectMsg = "Successfully connected to the Rock database.";
            }
            else
            {
                lblDbConnect.Style = (Style)FindResource( "labelStyleAlertBase" );
                DbConnectMsg = "Could not validate database connection.";
            }

            lblDbConnect.Visibility = Visibility.Visible;
        }

        /// <summary>
        /// Handles the Click event of the btnOk control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="RoutedEventArgs"/> instance containing the event data.</param>
        private void btnOk_Click( object sender, RoutedEventArgs e )
        {
            Window.GetWindow( (Button)sender ).DialogResult = true;
            sqlConnector.ConnectionString.MultipleActiveResultSets = true;
        }

        /// <summary>
        /// Handles the Click event of the btnNext control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="RoutedEventArgs"/> instance containing the event data.</param>
        private void btnNext_Click( object sender, RoutedEventArgs e )
        {
            if ( bulldozer == null || CurrentConnection == null )
            {
                lblDbConnect.Style = (Style)FindResource( "labelStyleAlertBase" );
                DbConnectMsg = "Please select a valid source and destination.";
                lblDbConnect.Visibility = Visibility.Visible;
                return;
            }

            var appConfig = ConfigurationManager.OpenExeConfiguration( ConfigurationUserLevel.None );
            var rockConnectionString = appConfig.ConnectionStrings.ConnectionStrings["RockContext"];

            if ( rockConnectionString == null )
            {
                rockConnectionString = new ConnectionStringSettings( "RockContext", CurrentConnection, "System.Data.SqlClient" );
                appConfig.ConnectionStrings.ConnectionStrings.Add( rockConnectionString );
            }
            else
            {
                rockConnectionString.ConnectionString = CurrentConnection;
            }

            var personChunkSizeSetting = appConfig.AppSettings.Settings["PersonChunkSize"];
            var attendanceChunkSizeSetting = appConfig.AppSettings.Settings["AttendanceChunkSize"];
            var defaultChunkSizeSetting = appConfig.AppSettings.Settings["DefaultChunkSize"];
            var importInstanceFKPrefixSetting = appConfig.AppSettings.Settings["ImportInstanceFKPrefix"];
            var importUpdateModeSetting = appConfig.AppSettings.Settings["ImportUpdateMode"];
            var importUseRockCampusSetting = appConfig.AppSettings.Settings["UseRockCampusIds"];
            var refreshAppSettings = false;

            if ( personChunkSizeSetting == null )
            {
                personChunkSizeSetting = new KeyValueConfigurationElement( "PersonChunkSize", tbPersonChunk.Text );
                appConfig.AppSettings.Settings.Add( personChunkSizeSetting );
                refreshAppSettings = true;
            }
            else if ( personChunkSizeSetting.Value != tbPersonChunk.Text.Trim() )
            {
                personChunkSizeSetting.Value = tbPersonChunk.Text;
                refreshAppSettings = true;
            }

            if ( attendanceChunkSizeSetting == null )
            {
                attendanceChunkSizeSetting = new KeyValueConfigurationElement( "AttendanceChunkSize", tbAttendanceChunk.Text );
                appConfig.AppSettings.Settings.Add( attendanceChunkSizeSetting );
                refreshAppSettings = true;
            }
            else if ( attendanceChunkSizeSetting.Value != tbAttendanceChunk.Text.Trim() )
            {
                attendanceChunkSizeSetting.Value = tbAttendanceChunk.Text;
                refreshAppSettings = true;
            }

            if ( defaultChunkSizeSetting == null )
            {
                defaultChunkSizeSetting = new KeyValueConfigurationElement( "DefaultChunkSize", tbDefaultChunk.Text );
                appConfig.AppSettings.Settings.Add( defaultChunkSizeSetting );
                refreshAppSettings = true;
            }
            else if ( defaultChunkSizeSetting.Value != tbDefaultChunk.Text.Trim() )
            {
                defaultChunkSizeSetting.Value = tbDefaultChunk.Text;
                refreshAppSettings = true;
            }

            if ( importInstanceFKPrefixSetting == null )
            {
                importInstanceFKPrefixSetting = new KeyValueConfigurationElement( "ImportInstanceFKPrefix", tbFKPrefix.Text );
                appConfig.AppSettings.Settings.Add( importInstanceFKPrefixSetting );
                refreshAppSettings = true;
            }
            else if ( importInstanceFKPrefixSetting.Value != tbFKPrefix.Text.Trim() )
            {
                importInstanceFKPrefixSetting.Value = tbFKPrefix.Text;
                refreshAppSettings = true;
            }

            //if ( importUpdateModeSetting == null )
            //{
            //    importUpdateModeSetting = new KeyValueConfigurationElement( "ImportUpdateMode", lstUpdateMode.SelectedValue.ToString() );
            //    appConfig.AppSettings.Settings.Add( importUpdateModeSetting );
            //    refreshAppSettings = true;
            //}
            //else if ( importUpdateModeSetting.Value != lstUpdateMode.SelectedValue.ToString() )
            //{
            //    importUpdateModeSetting.Value = lstUpdateMode.SelectedValue.ToString();
            //    refreshAppSettings = true;
            //}

            if ( importUseRockCampusSetting == null )
            {
                importUseRockCampusSetting = new KeyValueConfigurationElement( "UseRockCampusIds", cbUseRockCampus.IsChecked.ToString() );
                appConfig.AppSettings.Settings.Add( importUseRockCampusSetting );
                refreshAppSettings = true;
            }
            else if ( importUseRockCampusSetting.Value.AsBoolean() != cbUseRockCampus.IsChecked )
            {
                importUseRockCampusSetting.Value = cbUseRockCampus.IsChecked.ToString();
                refreshAppSettings = true;
            }

            try
            {
                // Save the user's selected connection string
                appConfig.Save( ConfigurationSaveMode.Modified );
                ConfigurationManager.RefreshSection( "connectionStrings" );

                if ( refreshAppSettings )
                {
                    // Save the user's selected chunk sizes
                    appConfig.Save( ConfigurationSaveMode.Modified );
                    ConfigurationManager.RefreshSection( "appSettings" );
                }

                var selectPage = new SelectPage( bulldozer );
                this.NavigationService.Navigate( selectPage );
            }
            catch ( Exception ex )
            {
                App.LogException( "Next Page", ex.ToString() );
                lblDbConnect.Style = (Style)FindResource( "labelStyleAlertBase" );
                DbConnectMsg = "Unable to save the database connection: " + ex.InnerException;
                lblDbConnect.Visibility = Visibility.Visible;
            }
        }

        #endregion Events

        #region Async Tasks

        /// <summary>
        /// Handles the DoWork event of the bwLoadSchema control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="DoWorkEventArgs"/> instance containing the event data.</param>
        private void bwPreview_DoWork( object sender, DoWorkEventArgs e )
        {
            var selectedBulldozer = (string)e.Argument;
            var filePicker = new OpenFileDialog();
            filePicker.Multiselect = true;

            var supportedExtensions = BulldozerTypes.Where( t => t.FullName.Equals( selectedBulldozer ) )
                .Select( t => t.FullName + " |*" + t.ExtensionType ).ToList();
            filePicker.Filter = string.Join( "|", supportedExtensions );

            if ( filePicker.ShowDialog() == true )
            {
                bulldozer = BulldozerTypes.Where( t => t.FullName.Equals( selectedBulldozer ) ).FirstOrDefault();
                if ( bulldozer != null )
                {
                    bool loadedSuccessfully = false;
                    foreach ( var file in filePicker.FileNames )
                    {
                        loadedSuccessfully = bulldozer.LoadSchema( file );
                        if ( !loadedSuccessfully )
                        {
                            e.Cancel = true;
                            break;
                        }

                        Dispatcher.BeginInvoke( (Action)( () =>
                            FilesUploaded.Children.Add( new TextBlock { Text = Path.GetFileName( file ) } )
                        ) );
                    }
                }
            }
            else
            {
                e.Cancel = true;
            }
        }

        /// <summary>
        /// Handles the RunWorkerCompleted event of the bwLoadSchema control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="RunWorkerCompletedEventArgs"/> instance containing the event data.</param>
        /// <exception cref="System.NotImplementedException"></exception>
        private void bwPreview_RunWorkerCompleted( object sender, RunWorkerCompletedEventArgs e )
        {
            if ( e.Cancelled != true )
            {
                lblDbUpload.Style = (Style)FindResource( "labelStyleSuccess" );
                lblDbUpload.Content = "Successfully read the import file";
            }

            RaisePropertyChanged( "OkToProceed" );

            lblDbUpload.Visibility = Visibility.Visible;
            Mouse.OverrideCursor = null;
        }

        #endregion Async Tasks

        private void numbersOnly_PreviewTextInput( object sender, TextCompositionEventArgs e )
        {
            var val = 0;
            e.Handled = !int.TryParse( e.Text, out val );
        }
    }
}