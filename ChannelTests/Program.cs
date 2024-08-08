using System.Diagnostics;
using System.Threading.Channels;

const int workSize = 1;

var opTimeConsumptions = new[]
{
    TimeSpan.FromMilliseconds(6), 
    TimeSpan.FromMilliseconds(12)
};

foreach (var firstOpTimeConsumption in opTimeConsumptions)
{
    foreach (var secondsOpTimeConsumption in opTimeConsumptions)
    {
        var sw = Stopwatch.StartNew();

        for (var i = 0; i < workSize; i++)
        {
            // First op
            await Task.Delay(firstOpTimeConsumption);
            // Second op
            await Task.Delay(secondsOpTimeConsumption);
        }

        sw.Stop();

        Console.WriteLine($"Normal({firstOpTimeConsumption.Milliseconds}, {secondsOpTimeConsumption.Milliseconds}) = {sw.Elapsed}");
        
        var firstChannel = Channel.CreateUnbounded<int>();
        var secondChannel = Channel.CreateUnbounded<int>();
        
        sw.Restart();

        var writerTask = Write(firstChannel.Writer);
        var readerWriterTask = ReadWrite(firstOpTimeConsumption, firstChannel.Reader, secondChannel.Writer);
        var readerTask = Read(secondsOpTimeConsumption, secondChannel.Reader);

        await Task.WhenAll(writerTask, readerWriterTask, readerTask);

        sw.Stop();

        Console.WriteLine($"Channels({firstOpTimeConsumption.Milliseconds}, {secondsOpTimeConsumption.Milliseconds}) = {sw.Elapsed}");
    }
}

return;

async Task Write(ChannelWriter<int> writer)
{
    for (var i = 0; i < workSize; i++)
    {
        await writer.WriteAsync(i);
    }

    writer.Complete();
}

async Task ReadWrite(TimeSpan firstOpTimeConsumption, ChannelReader<int> reader, ChannelWriter<int> writer)
{
    await foreach (var i in reader.ReadAllAsync())
    {
        await Task.Delay(firstOpTimeConsumption);
        await writer.WriteAsync(i);
    }

    writer.Complete();
}

async Task Read(TimeSpan secondsOpTimeConsumption, ChannelReader<int> reader)
{
    await foreach (var i in reader.ReadAllAsync())
    {
        await Task.Delay(secondsOpTimeConsumption);
    }
}