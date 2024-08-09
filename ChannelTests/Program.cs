using System.Diagnostics;
using System.Threading.Channels;

const int workSize = 1000;
const int partitionsCount = 4;

var opTimeConsumptions = new[]
{
    TimeSpan.FromMilliseconds(4),
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
            await Task.Delay(firstOpTimeConsumption);
            await Task.Delay(secondsOpTimeConsumption);
        }

        sw.Stop();

        Console.WriteLine(
            $"Normal({firstOpTimeConsumption.Milliseconds}, {secondsOpTimeConsumption.Milliseconds}) = {sw.Elapsed}");

        sw.Restart();

        await ProcessWithoutPartitioning(firstOpTimeConsumption, secondsOpTimeConsumption);

        sw.Stop();

        Console.WriteLine(
            $"Channels({firstOpTimeConsumption.Milliseconds}, {secondsOpTimeConsumption.Milliseconds}) = {sw.Elapsed}");

        sw.Restart();

        await ProcessWithOneLevelPartitioning(firstOpTimeConsumption, secondsOpTimeConsumption);

        sw.Stop();

        Console.WriteLine(
            $"One level partitioned channels({firstOpTimeConsumption.Milliseconds}, {secondsOpTimeConsumption.Milliseconds}) = {sw.Elapsed}");

        sw.Restart();

        await ProcessWithFullPartitioning(firstOpTimeConsumption, secondsOpTimeConsumption);

        sw.Stop();

        Console.WriteLine(
            $"Fully partitioned channels({firstOpTimeConsumption.Milliseconds}, {secondsOpTimeConsumption.Milliseconds}) = {sw.Elapsed}");
    }
}

return;

async Task Write(ChannelWriter<int> writer)
{
    for (var i = 0; i < workSize; i++)
    {
        await Task.Delay(1);
        await writer.WriteAsync(i);
    }
}

async Task ReadWrite(TimeSpan firstOpTimeConsumption, ChannelReader<int> reader, ChannelWriter<int> writer)
{
    await foreach (var i in reader.ReadAllAsync())
    {
        await Task.Delay(firstOpTimeConsumption);
        await writer.WriteAsync(i);
    }
}

async Task Read(TimeSpan secondsOpTimeConsumption, ChannelReader<int> reader)
{
    await foreach (var i in reader.ReadAllAsync())
    {
        await Task.Delay(secondsOpTimeConsumption);
    }
}

async Task ProcessWithoutPartitioning(TimeSpan firstsOpTimeConsumption, TimeSpan secondsOpTimeConsumption)
{
    var firstChannel = Channel.CreateUnbounded<int>(
        new UnboundedChannelOptions
        {
            SingleWriter = true,
            SingleReader = true
        });

    var secondChannel = Channel.CreateUnbounded<int>(
        new UnboundedChannelOptions
        {
            SingleWriter = true,
            SingleReader = true
        });

    var writerTask = Write(firstChannel.Writer);
    var readerWriterTask = ReadWrite(firstsOpTimeConsumption, firstChannel.Reader, secondChannel.Writer);
    var readerTask = Read(secondsOpTimeConsumption, secondChannel.Reader);

    await writerTask;
    firstChannel.Writer.Complete();

    await readerWriterTask;
    secondChannel.Writer.Complete();

    await readerTask;
}

async Task ProcessWithOneLevelPartitioning(
    TimeSpan firstOpTimeConsumption,
    TimeSpan secondOpTimeConsumption)
{
    var partitionedChannels = new Dictionary<int, Channel<int>>();
    var readerWriterTasks = new List<Task>();

    var secondChannel = Channel.CreateUnbounded<int>(
        new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = true
        });

    var readerTask = Read(secondOpTimeConsumption, secondChannel.Reader);

    for (var i = 0; i < workSize; i++)
    {
        var partitionKey = i % partitionsCount;

        if (!partitionedChannels.TryGetValue(partitionKey, out var value))
        {
            value = Channel.CreateUnbounded<int>(
                new UnboundedChannelOptions
                {
                    SingleWriter = true,
                    SingleReader = true
                });

            partitionedChannels[partitionKey] = value;

            var firstChannelReader = partitionedChannels[partitionKey].Reader;

            readerWriterTasks.Add(
                ReadWrite(
                    firstOpTimeConsumption,
                    firstChannelReader,
                    secondChannel.Writer));
        }

        await Task.Delay(1);
        await value.Writer.WriteAsync(i);
    }

    foreach (var firstChannelWriter in partitionedChannels.Values.Select(c => c.Writer))
    {
        firstChannelWriter.Complete();
    }

    await Task.WhenAll(readerWriterTasks)
        .ContinueWith(_ => secondChannel.Writer.Complete());

    await readerTask;
}

async Task ProcessWithFullPartitioning(
    TimeSpan firstOpTimeConsumption,
    TimeSpan secondOpTimeConsumption)
{
    var partitionedChannels = new Dictionary<int, (Channel<int> FirstChannel, Channel<int> SecondChannel)>();
    var readerWriterTasks = new List<Task>();
    var readerTasks = new List<Task>();

    for (var i = 0; i < workSize; i++)
    {
        var partitionKey = i % partitionsCount;

        if (!partitionedChannels.TryGetValue(partitionKey, out var value))
        {
            value.FirstChannel = Channel.CreateUnbounded<int>(
                new UnboundedChannelOptions
                {
                    SingleWriter = true,
                    SingleReader = true
                });

            value.SecondChannel = Channel.CreateUnbounded<int>(
                new UnboundedChannelOptions
                {
                    SingleWriter = true,
                    SingleReader = true
                });

            partitionedChannels[partitionKey] = value;

            var firstChannelReader = partitionedChannels[partitionKey].FirstChannel.Reader;
            var secondChannelWriter = partitionedChannels[partitionKey].SecondChannel.Writer;

            readerWriterTasks.Add(
                ReadWrite(
                    firstOpTimeConsumption,
                    firstChannelReader,
                    secondChannelWriter));

            var secondChannelReader = partitionedChannels[partitionKey].SecondChannel.Reader;

            readerTasks.Add(Read(secondOpTimeConsumption, secondChannelReader));
        }

        await Task.Delay(1);
        await value.FirstChannel.Writer.WriteAsync(i);
    }

    foreach (var firstChannelWriter in partitionedChannels.Values.Select(p => p.FirstChannel.Writer))
    {
        firstChannelWriter.Complete();
    }

    await Task.WhenAll(readerWriterTasks);

    foreach (var secondChannelWriter in partitionedChannels.Values.Select(p => p.SecondChannel.Writer))
    {
        secondChannelWriter.Complete();
    }

    await Task.WhenAll(readerTasks);
}