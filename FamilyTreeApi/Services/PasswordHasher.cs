using System.Security.Cryptography;
using System.Text;

namespace FamilyTreeApi.Services;

/// <summary>
/// 与常见作业一致：密码做 SHA256 十六进制小写存储，便于与库中 password_hash 字段比对。
/// </summary>
public static class PasswordHasher
{
    public static string Sha256Hex(string plain)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(plain));
        var sb = new StringBuilder(bytes.Length * 2);
        foreach (var b in bytes)
            sb.Append(b.ToString("x2"));
        return sb.ToString();
    }

    /// <summary>校验明文：支持库中存 SHA256 十六进制，或测试用明文（如 CSV 中的 123456）。</summary>
    public static bool Verify(string plain, string stored)
    {
        if (string.IsNullOrEmpty(stored))
            return false;
        if (stored == plain)
            return true;
        return stored == Sha256Hex(plain);
    }
}
