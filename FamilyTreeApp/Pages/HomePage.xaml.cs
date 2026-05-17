// HomePage.xaml.cs
using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Effects;
using System.Windows.Shapes;
using FamilyTreeApp.Helpers;
using FamilyTreeApp.Models;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Linq;

namespace FamilyTreeApp.Pages
{
    public class UserTreesResponse
    {
        public bool Success { get; set; }
        public List<UserTreeInfo> OwnedTrees { get; set; } = new();
        public List<UserTreeInfo> ManagedTrees { get; set; } = new();
    }

    public class UserTreeInfo
    {
        public ulong TreeId { get; set; }
        public string TreeName { get; set; } = "";
        public string Surname { get; set; } = "";
        public int TotalMembers { get; set; }
        public int MaleCount { get; set; }
        public int FemaleCount { get; set; }
        public string CreateDate { get; set; } = "";
    }

    public partial class HomePage : Page
    {
        private List<ClanInfo> _createdClans;
        private List<ClanInfo> _managedClans;
        private bool _createdExpanded = false;
        private bool _managedExpanded = false;
        private readonly HttpClient _httpClient;
        private readonly ulong _userId;

        // 一行的高度：卡片高度200 + 下方Margin16 = 216，再加点余量
        private const double OneLineHeight = 169;

        public HomePage(ulong userId)
        {
            InitializeComponent();
            _userId = userId;
            _httpClient = new HttpClient { BaseAddress = new Uri("http://localhost:5000") };
            _createdClans = new List<ClanInfo>();
            _managedClans = new List<ClanInfo>();
            Loaded += async (s, e) => await LoadDataAndGenerateCards();

            // Popup 内容区域鼠标离开时关闭
            PopupInnerBorder.MouseLeave += PopupContent_MouseLeave;

        }

        private void HomePage_Loaded(object sender, RoutedEventArgs e)
        {
            // 布局完成后检测是否需要展开按钮
            Dispatcher.BeginInvoke(new Action(() =>
            {
                CheckExpandNeeded(CreatedWrapPanel, CreatedPanel, CreatedExpandBtn);
                CheckExpandNeeded(ManagedWrapPanel, ManagedPanel, ManagedExpandBtn);
            }), System.Windows.Threading.DispatcherPriority.Loaded);
        }

        private async Task LoadDataAndGenerateCards()
        {
            await LoadDataFromApi();
            GenerateClanCards();
            HomePage_Loaded(this, new RoutedEventArgs());
        }

        // ────────── 数据加载 ──────────

        private async Task LoadDataFromApi()
        {
            try
            {
                var response = await _httpClient.GetAsync($"/api/auth/trees?userId={_userId}");
                if (response.IsSuccessStatusCode)
                {
                    var content = await response.Content.ReadAsStringAsync();
                    var result = JsonSerializer.Deserialize<UserTreesResponse>(content, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                    if (result != null && result.Success)
                    {
                        _createdClans = result.OwnedTrees.Select(t => new ClanInfo
                        {
                            Name = t.TreeName,
                            TotalMembers = t.TotalMembers,
                            MaleCount = t.MaleCount,
                            FemaleCount = t.FemaleCount,
                            CreateDate = t.CreateDate
                        }).ToList();

                        _managedClans = result.ManagedTrees.Select(t => new ClanInfo
                        {
                            Name = t.TreeName,
                            TotalMembers = t.TotalMembers,
                            MaleCount = t.MaleCount,
                            FemaleCount = t.FemaleCount,
                            CreateDate = t.CreateDate
                        }).ToList();
                    }
                    else
                    {
                        MessageBox.Show("获取族谱数据失败", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                        _createdClans = new List<ClanInfo>();
                        _managedClans = new List<ClanInfo>();
                    }
                }
                else
                {
                    MessageBox.Show("网络错误，无法获取族谱数据", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                    _createdClans = new List<ClanInfo>();
                    _managedClans = new List<ClanInfo>();
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"加载数据时出错: {ex.Message}", "错误", MessageBoxButton.OK, MessageBoxImage.Error);
                _createdClans = new List<ClanInfo>();
                _managedClans = new List<ClanInfo>();
            }
        }

        private void LoadSampleData()
        {
            _createdClans = new List<ClanInfo>
            {
                new ClanInfo { Name = "王氏族谱", TotalMembers = 128, MaleCount = 77, FemaleCount = 51, CreateDate = "2024-01" },
                new ClanInfo { Name = "李氏族谱", TotalMembers = 256, MaleCount = 142, FemaleCount = 114, CreateDate = "2023-06" },
                new ClanInfo { Name = "张氏族谱", TotalMembers = 89, MaleCount = 48, FemaleCount = 41, CreateDate = "2024-03" },
                new ClanInfo { Name = "欧阳氏族谱", TotalMembers = 445, MaleCount = 234, FemaleCount = 211, CreateDate = "2021-08" },
                new ClanInfo { Name = "护氏族谱", TotalMembers = 167, MaleCount = 89, FemaleCount = 78, CreateDate = "2023-04" },
                new ClanInfo { Name = "前氏族谱", TotalMembers = 92, MaleCount = 50, FemaleCount = 42, CreateDate = "2024-01" },
                new ClanInfo { Name = "爱护氏族谱", TotalMembers = 213, MaleCount = 118, FemaleCount = 95, CreateDate = "2022-06" },
                new ClanInfo { Name = "这种氏族谱", TotalMembers = 78, MaleCount = 42, FemaleCount = 36, CreateDate = "2023-12" },

            };

            _managedClans = new List<ClanInfo>
            {
                new ClanInfo { Name = "周氏族谱", TotalMembers = 445, MaleCount = 234, FemaleCount = 211, CreateDate = "2021-08" },
                new ClanInfo { Name = "吴氏族谱", TotalMembers = 167, MaleCount = 89, FemaleCount = 78, CreateDate = "2023-04" },
                new ClanInfo { Name = "郑氏族谱", TotalMembers = 92, MaleCount = 50, FemaleCount = 42, CreateDate = "2024-01" },
                new ClanInfo { Name = "冯氏族谱", TotalMembers = 213, MaleCount = 118, FemaleCount = 95, CreateDate = "2022-06" },
                new ClanInfo { Name = "褚氏族谱", TotalMembers = 78, MaleCount = 42, FemaleCount = 36, CreateDate = "2023-12" },
                new ClanInfo { Name = "刘氏族谱", TotalMembers = 342, MaleCount = 186, FemaleCount = 156, CreateDate = "2022-11" },
                new ClanInfo { Name = "陈氏族谱", TotalMembers = 67, MaleCount = 35, FemaleCount = 32, CreateDate = "2024-05" },
                new ClanInfo { Name = "赵氏族谱", TotalMembers = 198, MaleCount = 110, FemaleCount = 88, CreateDate = "2023-09" },
                new ClanInfo { Name = "孙氏族谱", TotalMembers = 45, MaleCount = 24, FemaleCount = 21, CreateDate = "2024-02" },
            };
        }

        // ────────── 生成族谱卡片 ──────────

        private void GenerateClanCards()
        {
            foreach (var clan in _createdClans)
                CreatedWrapPanel.Children.Add(CreateClanCard(clan));

            foreach (var clan in _managedClans)
                ManagedWrapPanel.Children.Add(CreateClanCard(clan));
        }

        private Border CreateClanCard(ClanInfo clan)
        {
            // 卡片容器
            var card = new Border
            {
                Width = 129,
                Height = 153,
                Background = new SolidColorBrush(Color.FromRgb(255, 255, 255)),
                CornerRadius = new CornerRadius(12),
                BorderBrush = new SolidColorBrush(Color.FromRgb(230, 221, 210)),
                BorderThickness = new Thickness(1),
                Margin = new Thickness(0, 0, 25, 16),
                Cursor = Cursors.Hand,
                Tag = clan,
                Effect = new DropShadowEffect
                {
                    ShadowDepth = 0,
                    BlurRadius = 10,
                    Opacity = 0.06,
                    Color = Colors.Black
                }
            };

            // 内容布局
            var stack = new StackPanel { Margin = new Thickness(16, 18, 16, 16) };

            // 图标占位区域（后续替换为真实图标）
            var iconBorder = new Border
            {
                Width = 54,
                Height = 54,
                CornerRadius = new CornerRadius(27),
                Background = new SolidColorBrush(Color.FromRgb(242, 234, 224)),
                Margin = new Thickness(0, 0, 0, 16),
                HorizontalAlignment = HorizontalAlignment.Center
            };
            var iconText = new TextBlock
            {
                Text = clan.Name.Substring(0, 1),
                FontSize = 22,
                FontWeight = FontWeights.Medium,
                Foreground = new SolidColorBrush(Color.FromRgb(184, 101, 58)),
                HorizontalAlignment = HorizontalAlignment.Center,
                VerticalAlignment = VerticalAlignment.Center
            };
            iconBorder.Child = iconText;
            stack.Children.Add(iconBorder);

            // 族谱名称
            stack.Children.Add(new TextBlock
            {
                Text = clan.Name,
                FontSize = 15,
                FontWeight = FontWeights.Medium,
                Foreground = new SolidColorBrush(Color.FromRgb(44, 34, 23)),
                HorizontalAlignment = HorizontalAlignment.Center,
                TextTrimming = TextTrimming.CharacterEllipsis
            });

            // 成员数
            stack.Children.Add(new TextBlock
            {
                Text = $"{clan.TotalMembers} 人",
                FontSize = 12,
                Foreground = new SolidColorBrush(Color.FromRgb(138, 126, 114)),
                HorizontalAlignment = HorizontalAlignment.Center,
                Margin = new Thickness(0, 6, 0, 0)
            });

            // 创建日期
            stack.Children.Add(new TextBlock
            {
                Text = clan.CreateDate,
                FontSize = 11,
                Foreground = new SolidColorBrush(Color.FromRgb(180, 170, 158)),
                HorizontalAlignment = HorizontalAlignment.Center,
                Margin = new Thickness(0, 4, 0, 0)
            });

            card.Child = stack;

            // 鼠标交互
            card.MouseLeftButtonDown += (s, e) => ShowClanInfoPopup(clan, card);
            card.MouseEnter += (s, e) =>
            {
                card.BorderBrush = new SolidColorBrush(Color.FromRgb(184, 101, 58));
                var effect = card.Effect as DropShadowEffect;
                if (effect != null) { effect.Opacity = 0.14; effect.BlurRadius = 18; }
            };
            card.MouseLeave += (s, e) =>
            {
                card.BorderBrush = new SolidColorBrush(Color.FromRgb(230, 221, 210));
                var effect = card.Effect as DropShadowEffect;
                if (effect != null) { effect.Opacity = 0.06; effect.BlurRadius = 10; }
            };

            return card;
        }

        private void Page_Loaded(object sender, RoutedEventArgs e)
        {
            ScrollViewerHelper.ApplySmoothScrolling(MainScroll);
        }

        private static string FormatPercent(int part, int total) =>
            total <= 0 ? "0.00%" : $"{(double)part / total * 100:F2}%";

        private static string FormatCount(int count) => count.ToString("N0");

        private static UIElement CreateLegendRow(Color dotColor, string label, int count, int total, double topMargin = 0)
        {
            var row = new StackPanel
            {
                Orientation = Orientation.Horizontal,
                Margin = new Thickness(0, topMargin, 0, 0)
            };

            var dot = new Ellipse
            {
                Width = 10,
                Height = 10,
                Fill = new SolidColorBrush(dotColor),
                VerticalAlignment = VerticalAlignment.Top,
                Margin = new Thickness(0, 3, 8, 0)
            };

            var text = new TextBlock
            {
                Text = $"{label}  {FormatCount(count)} 人  ({FormatPercent(count, total)})",
                FontSize = 13,
                Foreground = new SolidColorBrush(Color.FromRgb(100, 90, 78)),
                TextWrapping = TextWrapping.Wrap
            };

            row.Children.Add(dot);
            row.Children.Add(text);
            return row;
        }

        // ────────── 族谱信息弹窗 ──────────

        private void ShowClanInfoPopup(ClanInfo clan, Border card)
        {
            ClanInfoPopup.PlacementTarget = card;
            ClanInfoPopup.Placement = PlacementMode.Bottom;

            var panel = PopupContentPanel;
            panel.Children.Clear();

            // 标题
            panel.Children.Add(new TextBlock
            {
                Text = clan.Name,
                FontSize = 18,
                FontWeight = FontWeights.SemiBold,
                Foreground = new SolidColorBrush(Color.FromRgb(44, 34, 23)),
                Margin = new Thickness(0, 0, 0, 4)
            });

            // 分割线
            panel.Children.Add(new Border
            {
                Height = 1,
                Background = new SolidColorBrush(Color.FromRgb(235, 228, 218)),
                Margin = new Thickness(0, 0, 0, 16)
            });

            // 总人数
            panel.Children.Add(new TextBlock
            {
                Text = $"总人数：{clan.TotalMembers}",
                FontSize = 14,
                Foreground = new SolidColorBrush(Color.FromRgb(44, 34, 23)),
                Margin = new Thickness(0, 0, 0, 18)
            });

            // 饼状图（扇区标注人数与占比）
            var pieChart = CreatePieChart(clan, 168);
            pieChart.HorizontalAlignment = HorizontalAlignment.Center;
            panel.Children.Add(pieChart);

            // 图例（纵向，避免大数字被截断）
            var legend = new StackPanel
            {
                Margin = new Thickness(0, 16, 0, 0),
                HorizontalAlignment = HorizontalAlignment.Stretch
            };

            legend.Children.Add(CreateLegendRow(
                Color.FromRgb(184, 101, 58),
                "男性",
                clan.MaleCount,
                clan.TotalMembers));

            legend.Children.Add(CreateLegendRow(
                Color.FromRgb(196, 168, 130),
                "女性",
                clan.FemaleCount,
                clan.TotalMembers,
                topMargin: 10));

            panel.Children.Add(legend);

            ClanInfoPopup.IsOpen = true;
        }

        private void PopupContent_MouseLeave(object sender, MouseEventArgs e)
        {
            ClanInfoPopup.IsOpen = false;
        }

        // ────────── 饼状图绘制 ──────────

        private Canvas CreatePieChart(ClanInfo clan, double size)
        {
            var canvas = new Canvas { Width = size, Height = size };

            if (clan.TotalMembers <= 0) return canvas;

            double cx = size / 2;
            double cy = size / 2;
            double radius = size / 2 - 8;

            double maleAngle = (double)clan.MaleCount / clan.TotalMembers * 360;

            // 男性扇形
            if (clan.MaleCount > 0)
            {
                canvas.Children.Add(CreatePieSlice(
                    cx, cy, radius, 0, maleAngle,
                    new SolidColorBrush(Color.FromRgb(184, 101, 58))));

                if (maleAngle >= 12)
                {
                    AddSliceLabel(canvas, cx, cy, radius * 0.68, maleAngle / 2,
                        FormatCount(clan.MaleCount),
                        FormatPercent(clan.MaleCount, clan.TotalMembers));
                }
            }

            // 女性扇形
            if (clan.FemaleCount > 0)
            {
                canvas.Children.Add(CreatePieSlice(
                    cx, cy, radius, maleAngle, 360,
                    new SolidColorBrush(Color.FromRgb(196, 168, 130))));

                var femaleAngle = 360 - maleAngle;
                if (femaleAngle >= 12)
                {
                    AddSliceLabel(canvas, cx, cy, radius * 0.68, maleAngle + femaleAngle / 2,
                        FormatCount(clan.FemaleCount),
                        FormatPercent(clan.FemaleCount, clan.TotalMembers));
                }
            }

            // 中心白色圆（环形效果）+ 总人数
            var innerSize = radius * 0.52;
            var innerCircle = new Ellipse
            {
                Width = innerSize,
                Height = innerSize,
                Fill = new SolidColorBrush(Colors.White)
            };
            innerCircle.SetValue(Canvas.LeftProperty, cx - innerSize / 2);
            innerCircle.SetValue(Canvas.TopProperty, cy - innerSize / 2);
            canvas.Children.Add(innerCircle);

            var centerText = new TextBlock
            {
                Text = "总计\n" + FormatCount(clan.TotalMembers),
                FontSize = 10,
                FontWeight = FontWeights.SemiBold,
                Foreground = new SolidColorBrush(Color.FromRgb(44, 34, 23)),
                TextAlignment = TextAlignment.Center,
                LineHeight = 13
            };
            centerText.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
            centerText.SetValue(Canvas.LeftProperty, cx - centerText.DesiredSize.Width / 2);
            centerText.SetValue(Canvas.TopProperty, cy - centerText.DesiredSize.Height / 2);
            canvas.Children.Add(centerText);

            return canvas;
        }

        /// <summary>在扇区中部标注人数与占比（midAngleDeg：0° 为顶部，顺时针）</summary>
        private static void AddSliceLabel(Canvas canvas, double cx, double cy, double labelRadius,
            double midAngleDeg, string countText, string percentText)
        {
            var rad = (midAngleDeg - 90) * Math.PI / 180;
            var x = cx + labelRadius * Math.Cos(rad);
            var y = cy + labelRadius * Math.Sin(rad);

            var label = new TextBlock
            {
                Text = $"{countText}\n{percentText}",
                FontSize = 10,
                FontWeight = FontWeights.SemiBold,
                Foreground = new SolidColorBrush(Colors.White),
                TextAlignment = TextAlignment.Center,
                LineHeight = 12
            };
            label.Effect = new System.Windows.Media.Effects.DropShadowEffect
            {
                Color = Colors.Black,
                BlurRadius = 4,
                ShadowDepth = 0,
                Opacity = 0.45
            };
            label.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
            label.SetValue(Canvas.LeftProperty, x - label.DesiredSize.Width / 2);
            label.SetValue(Canvas.TopProperty, y - label.DesiredSize.Height / 2);
            canvas.Children.Add(label);
        }

        private Path CreatePieSlice(double cx, double cy, double r,
                                     double startAngle, double endAngle, Brush fill)
        {
            if (Math.Abs(endAngle - startAngle) < 0.01)
                return new Path { Visibility = Visibility.Collapsed };

            Point startPt = new Point(
                cx + r * Math.Cos((startAngle - 90) * Math.PI / 180),
                cy + r * Math.Sin((startAngle - 90) * Math.PI / 180));

            Point endPt = new Point(
                cx + r * Math.Cos((endAngle - 90) * Math.PI / 180),
                cy + r * Math.Sin((endAngle - 90) * Math.PI / 180));

            bool isLarge = (endAngle - startAngle) > 180;

            var geo = new StreamGeometry();
            using (var ctx = geo.Open())
            {
                ctx.BeginFigure(new Point(cx, cy), true, true);
                ctx.LineTo(startPt, true, false);
                ctx.ArcTo(endPt, new Size(r, r), 0, isLarge,
                          SweepDirection.Clockwise, true, false);
            }

            return new Path
            {
                Data = geo,
                Fill = fill,
                Stroke = new SolidColorBrush(Colors.White),
                StrokeThickness = 2
            };
        }

        // ────────── 展开 / 折叠 ──────────

        private void CheckExpandNeeded(WrapPanel wrapPanel, Border container, TextBlock expandBtn)
        {
            wrapPanel.UpdateLayout();
            if (wrapPanel.ActualHeight > OneLineHeight)
            {
                expandBtn.Visibility = Visibility.Visible;
                container.MaxHeight = OneLineHeight;
            }
            else
            {
                expandBtn.Visibility = Visibility.Collapsed;
                container.MaxHeight = double.PositiveInfinity;
            }
        }

        private void CreatedExpand_Click(object sender, MouseButtonEventArgs e)
        {
            _createdExpanded = !_createdExpanded;
            if (_createdExpanded)
            {
                CreatedPanel.MaxHeight = double.PositiveInfinity;
                CreatedExpandBtn.Text = "收起 ▴";
            }
            else
            {
                CreatedPanel.MaxHeight = OneLineHeight;
                CreatedExpandBtn.Text = "展开更多 ▾";
            }
        }

        private void ManagedExpand_Click(object sender, MouseButtonEventArgs e)
        {
            _managedExpanded = !_managedExpanded;
            if (_managedExpanded)
            {
                ManagedPanel.MaxHeight = double.PositiveInfinity;
                ManagedExpandBtn.Text = "收起 ▴";
            }
            else
            {
                ManagedPanel.MaxHeight = OneLineHeight;
                ManagedExpandBtn.Text = "展开更多 ▾";
            }
        }

        // 滚动时关闭弹窗
        private void MainScroll_ScrollChanged(object sender, ScrollChangedEventArgs e)
        {
            if (ClanInfoPopup.IsOpen)
                ClanInfoPopup.IsOpen = false;
        }
    }
}
