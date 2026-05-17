using System.Windows;
using System.Windows.Controls;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;

namespace FamilyTreeApp.Pages
{
    public class LoginResult
    {
        public bool Success { get; set; }
        public ulong UserId { get; set; }
        public string Username { get; set; } = "";
        public string Message { get; set; } = "";
    }

    public class RegisterResult
    {
        public bool Success { get; set; }
        public ulong UserId { get; set; }
        public string Username { get; set; } = "";
        public string Message { get; set; } = "";
    }

    public partial class LoginPage : Page
    {
        private bool _isPasswordVisible = false;
        private readonly HttpClient _httpClient;

        public LoginPage()
        {
            InitializeComponent();
            // 初始将密码隐藏框设为可见，明文框隐藏，同步内容
            PasswordHiddenBox.Visibility = Visibility.Visible;
            PasswordVisibleBox.Visibility = Visibility.Collapsed;
            _httpClient = new HttpClient { BaseAddress = new Uri("http://localhost:5000") };
        }

        private void TogglePasswordVisibility_Click(object sender, RoutedEventArgs e)
        {
            if (_isPasswordVisible)
            {
                // 切换到隐藏模式
                PasswordHiddenBox.Password = PasswordVisibleBox.Text;
                PasswordHiddenBox.Visibility = Visibility.Visible;
                PasswordVisibleBox.Visibility = Visibility.Collapsed;
                TogglePasswordButton.Content = "👁";  // 闭眼或普通眼睛
                _isPasswordVisible = false;
            }
            else
            {
                // 切换到明文模式
                PasswordVisibleBox.Text = PasswordHiddenBox.Password;
                PasswordHiddenBox.Visibility = Visibility.Collapsed;
                PasswordVisibleBox.Visibility = Visibility.Visible;
                TogglePasswordButton.Content = "👁‍🗨"; // 另一种图标表示可见
                _isPasswordVisible = true;
            }
        }

        private async void LoginButton_Click(object sender, RoutedEventArgs e)
        {
            string username = UsernameBox.Text.Trim();
            string password = _isPasswordVisible ? PasswordVisibleBox.Text : PasswordHiddenBox.Password;

            if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
            {
                MessageBox.Show("请输入用户名和密码", "提示", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            try
            {
                // 尝试登录
                var loginResponse = await LoginAsync(username, password);
                if (loginResponse.IsSuccessStatusCode)
                {
                    var responseContent = await loginResponse.Content.ReadAsStringAsync();
                    var loginResult = JsonSerializer.Deserialize<LoginResult>(responseContent, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                    if (loginResult != null && loginResult.Success)
                    {
                        System.Diagnostics.Debug.WriteLine("准备导航到 HomePage");
                        // 跳转到主界面
                        // 获取当前 Page 所在的窗口（即主窗口）
                        var mainWindow = Window.GetWindow(this) as MainWindow;
                        if (mainWindow == null)
                        {
                            MessageBox.Show("无法找到主窗口");
                            return;
                        }
                        mainWindow.SetCurrentUser(loginResult.UserId);
                        mainWindow.EnterMainShell();
                        if (mainWindow.MainFrame == null)
                        {
                            MessageBox.Show("主窗口中没有 MainFrame");
                            return;
                        }

                        // 先导航，不设置 RadioButton（避免事件干扰）
                        mainWindow.MainFrame.Navigate(new HomePage(loginResult.UserId));

                        // 稍后再统一设置选中状态（也可以不设置，让默认选中 Home）
                        // 如果需要同步，可以使用 Dispatcher 延迟设置
                        await Dispatcher.InvokeAsync(() =>
                        {
                            mainWindow.NavHome.IsChecked = true;
                        });
                    }
                    else
                    {
                        MessageBox.Show("登录失败：" + (loginResult?.Message ?? responseContent), "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                    }
                }
                else
                {
                    // 尝试注册
                    var registerResponse = await RegisterAsync(username, password);
                    if (registerResponse.IsSuccessStatusCode)
                    {
                        var registerContent = await registerResponse.Content.ReadAsStringAsync();
                        var registerResult = JsonSerializer.Deserialize<RegisterResult>(registerContent, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                        if (registerResult != null && registerResult.Success)
                        {
                            System.Diagnostics.Debug.WriteLine("准备导航到 HomePage");
                            // 跳转到主界面
                            // 获取当前 Page 所在的窗口（即主窗口）
                            var mainWindow = Window.GetWindow(this) as MainWindow;
                            if (mainWindow == null)
                            {
                                MessageBox.Show("无法找到主窗口");
                                return;
                            }
                            mainWindow.SetCurrentUser(registerResult.UserId);
                            mainWindow.EnterMainShell();
                            if (mainWindow.MainFrame == null)
                            {
                                MessageBox.Show("主窗口中没有 MainFrame");
                                return;
                            }

                            // 先导航，不设置 RadioButton（避免事件干扰）
                            mainWindow.MainFrame.Navigate(new HomePage(registerResult.UserId));

                            // 稍后再统一设置选中状态（也可以不设置，让默认选中 Home）
                            // 如果需要同步，可以使用 Dispatcher 延迟设置
                            await Dispatcher.InvokeAsync(() =>
                            {
                                mainWindow.NavHome.IsChecked = true;
                            });
                        }
                        else
                        {
                            MessageBox.Show("注册失败：" + (registerResult?.Message ?? registerContent), "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                        }
                    }
                    else
                    {
                        // 提示错误
                        MessageBox.Show("用户名存在但密码错误", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                    }
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"网络错误: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private async Task<HttpResponseMessage> LoginAsync(string username, string password)
        {
            var request = new { Username = username, Password = password };
            return await _httpClient.PostAsJsonAsync("/api/auth/login", request);
        }

        private async Task<HttpResponseMessage> RegisterAsync(string username, string password)
        {
            var request = new { Username = username, Password = password };
            return await _httpClient.PostAsJsonAsync("/api/auth/register", request);
        }
    }
}