using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;

namespace FamilyTreeApp.Helpers;

/// <summary>改善嵌套 ScrollViewer 的滚轮体验，并启用像素级平滑滚动。</summary>
public static class ScrollViewerHelper
{
    public static void ApplySmoothScrolling(ScrollViewer scrollViewer, bool bubbleWheelWhenAtEdge = true)
    {
        scrollViewer.CanContentScroll = false;
        scrollViewer.PanningMode = PanningMode.VerticalOnly;
        scrollViewer.IsManipulationEnabled = true;

        if (bubbleWheelWhenAtEdge)
            scrollViewer.PreviewMouseWheel += OnPreviewMouseWheelBubble;
    }

    public static void ApplyToDescendants(DependencyObject root, bool bubbleWheelWhenAtEdge = true)
    {
        if (root is ScrollViewer sv)
            ApplySmoothScrolling(sv, bubbleWheelWhenAtEdge);

        var count = System.Windows.Media.VisualTreeHelper.GetChildrenCount(root);
        for (var i = 0; i < count; i++)
            ApplyToDescendants(System.Windows.Media.VisualTreeHelper.GetChild(root, i), bubbleWheelWhenAtEdge);
    }

    private static void OnPreviewMouseWheelBubble(object sender, MouseWheelEventArgs e)
    {
        if (sender is not ScrollViewer scrollViewer)
            return;

        var atTop = scrollViewer.VerticalOffset <= 0;
        var atBottom = scrollViewer.VerticalOffset >= scrollViewer.ScrollableHeight - 0.5;

        if (e.Delta > 0 && atTop)
        {
            var parent = FindParentScrollViewer(scrollViewer);
            if (parent != null)
            {
                e.Handled = true;
                parent.ScrollToVerticalOffset(parent.VerticalOffset - 48);
            }
            return;
        }

        if (e.Delta < 0 && atBottom)
        {
            var parent = FindParentScrollViewer(scrollViewer);
            if (parent != null)
            {
                e.Handled = true;
                parent.ScrollToVerticalOffset(parent.VerticalOffset + 48);
            }
        }
    }

    private static ScrollViewer? FindParentScrollViewer(DependencyObject child)
    {
        var parent = System.Windows.Media.VisualTreeHelper.GetParent(child);
        while (parent != null)
        {
            if (parent is ScrollViewer sv && sv != child)
                return sv;
            parent = System.Windows.Media.VisualTreeHelper.GetParent(parent);
        }
        return null;
    }
}
