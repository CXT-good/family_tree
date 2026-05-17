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
            Width = 520;
            Height = 550;
            WindowStartupLocation = WindowStartupLocation.CenterScreen;
        }

        public void SetCurrentUser(ulong userId)
        {
            _currentUserId = userId;
        }

        public void EnterMainShell()
        {
            NavHome.Visibility = Visibility.Visible;
            NavClan.Visibility = Visibility.Visible;
            LogoutButton.Visibility = Visibility.Visible;
            MinWidth = 960;
            MinHeight = 680;
            Width = 1100;
            Height = 720;
        }

        public void ExitToLogin()
        {
            _currentUserId = 0;
            NavHome.IsChecked = false;
            NavClan.IsChecked = false;
            NavHome.Visibility = Visibility.Collapsed;
            NavClan.Visibility = Visibility.Collapsed;
            LogoutButton.Visibility = Visibility.Collapsed;
            MinWidth = 400;
            MinHeight = 400;
            Width = 520;
            Height = 550;
            MainFrame.Navigate(new LoginPage());
        }

        private void LogoutButton_Click(object sender, RoutedEventArgs e)
        {
            ExitToLogin();
        }

        private void Nav_Checked(object sender, RoutedEventArgs e)
        {
            if (MainFrame == null || _currentUserId == 0) return;
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
