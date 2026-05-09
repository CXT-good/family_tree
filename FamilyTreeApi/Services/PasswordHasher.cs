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
}
