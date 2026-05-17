using System.Windows;
using System.Windows.Controls;
using FamilyTreeApp.Pages;

namespace FamilyTreeApp
{
    public partial class MainWindow : Window
    {
        private ulong _currentUserId;

        public MainWindow()
        {
            InitializeComponent();
            MainFrame.Navigate(new LoginPage());
            this.Width = 520;
            this.Height = 550;
            this.WindowStartupLocation = WindowStartupLocation.CenterScreen;
        }

        public void SetCurrentUser(ulong userId)
        {
            _currentUserId = userId;
        }

        private void Nav_Checked(object sender, RoutedEventArgs e)
        {
            if (MainFrame == null) return;
            var tag = (sender as RadioButton)?.Tag?.ToString();
            switch (tag)
            {
                case "Home":
                    MainFrame.Navigate(new HomePage(_currentUserId));
                    break;
                case "Clan":
                    MainFrame.Navigate(new ClanManagePage(_currentUserId));
                    break;
            }
        }
    }
}
