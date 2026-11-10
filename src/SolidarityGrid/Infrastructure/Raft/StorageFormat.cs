namespace SolidarityGrid.Infrastructure.Raft;

public static class StorageFormat
{
    private const string Marker = "solidarity-grid-payments-v3\n";

    public static void Check(string path)
    {
        Directory.CreateDirectory(path);
        var markerPath = Path.Combine(path, "format.txt");
        if (File.Exists(markerPath))
        {
            if (File.ReadAllText(markerPath) != Marker) throw new InvalidDataException("Incompatible storage format.");
            return;
        }
        if (Directory.EnumerateFileSystemEntries(path).Any())
            throw new InvalidDataException("Existing data has no payment format marker; refusing to modify it.");
        using var stream = new FileStream(markerPath, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        using var writer = new StreamWriter(stream);
        writer.Write(Marker);
    }
}
