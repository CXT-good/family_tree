using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;

namespace FamilyTreeApp.Controls
{
    /// <summary>带占位符的TextBox控件</summary>
    public class PlaceholderTextBox : TextBox
    {
        private bool _isPlaceholderShown = false;
        private Brush? _originalForeground;
        private const string PlaceholderForegroundKey = "PlaceholderForeground";

        public static readonly DependencyProperty PlaceholderProperty =
            DependencyProperty.Register(
                nameof(Placeholder),
                typeof(string),
                typeof(PlaceholderTextBox),
                new PropertyMetadata(string.Empty, OnPlaceholderChanged));

        public static readonly DependencyProperty PlaceholderForegroundProperty =
            DependencyProperty.Register(
                nameof(PlaceholderForeground),
                typeof(Brush),
                typeof(PlaceholderTextBox),
                new PropertyMetadata(new SolidColorBrush(Color.FromRgb(180, 170, 158))));

        public string Placeholder
        {
            get => (string)GetValue(PlaceholderProperty);
            set => SetValue(PlaceholderProperty, value);
        }

        public Brush PlaceholderForeground
        {
            get => (Brush)GetValue(PlaceholderForegroundProperty);
            set => SetValue(PlaceholderForegroundProperty, value);
        }

        public PlaceholderTextBox()
        {
            Loaded += PlaceholderTextBox_Loaded;
            GotFocus += PlaceholderTextBox_GotFocus;
            LostFocus += PlaceholderTextBox_LostFocus;
            TextChanged += PlaceholderTextBox_TextChanged;
        }

        private static void OnPlaceholderChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var textBox = d as PlaceholderTextBox;
            if (textBox != null)
            {
                textBox.UpdatePlaceholder();
            }
        }

        private void PlaceholderTextBox_Loaded(object sender, RoutedEventArgs e)
        {
            UpdatePlaceholder();
        }

        private void PlaceholderTextBox_GotFocus(object sender, RoutedEventArgs e)
        {
            if (_isPlaceholderShown)
            {
                _originalForeground = this.Foreground;
                this.Text = "";
                this.Foreground = _originalForeground;
                _isPlaceholderShown = false;
            }
        }

        private void PlaceholderTextBox_LostFocus(object sender, RoutedEventArgs e)
        {
            UpdatePlaceholder();
        }

        private void PlaceholderTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            if (string.IsNullOrEmpty(this.Text) && !this.IsFocused)
            {
                UpdatePlaceholder();
            }
        }

        private void UpdatePlaceholder()
        {
            if (string.IsNullOrEmpty(this.Text) && !this.IsFocused)
            {
                if (!string.IsNullOrEmpty(Placeholder))
                {
                    _originalForeground = this.Foreground;
                    this.Text = Placeholder;
                    this.Foreground = PlaceholderForeground;
                    _isPlaceholderShown = true;
                }
            }
            else if (_isPlaceholderShown && this.IsFocused)
            {
                this.Text = "";
                this.Foreground = _originalForeground;
                _isPlaceholderShown = false;
            }
        }

        /// <summary>获取实际输入的文本（不包括占位符）</summary>
        public string GetActualText()
        {
            return _isPlaceholderShown ? "" : this.Text;
        }
    }
}
