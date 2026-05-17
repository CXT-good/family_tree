using FamilyTreeApp.Pages;
using System.Configuration;
using System.Data;
using System.Windows;


namespace FamilyTreeApp
{
    public partial class App : Application
    {
       protected override void OnStartup(StartupEventArgs e)
       {
           base.OnStartup(e);
           var mainWindow = new MainWindow();
           mainWindow.Width = 520;   // 比卡片宽度稍大
           mainWindow.Height = 550;
           mainWindow.WindowStartupLocation = WindowStartupLocation.CenterScreen;
       }
    }
    //public partial class App : Application;
}


