using System.Threading.Channels;
using BenchmarkDotNet.Attributes;

namespace ChannelTests;

[ThreadingDiagnoser]
public class ChannelSetupBenchmarks
{
    private static async Task Write(int workSize, int firstOpTimeConsumption, ChannelWriter<int> writer)
    {
        for (var i = 0; i < workSize; i++)
        {
            await Task.Delay(firstOpTimeConsumption);
            await writer.WriteAsync(i);
        }
    }

    private static async Task ReadWrite(int secondOpTimeConsumption, ChannelReader<int> reader,
        ChannelWriter<int> writer)
    {
        await foreach (var i in reader.ReadAllAsync())
        {
            await Task.Delay(secondOpTimeConsumption);
            await writer.WriteAsync(i);
        }
    }

    private static async Task Read(int thirdOpTimeConsumption, ChannelReader<int> reader)
    {
        await foreach (var _ in reader.ReadAllAsync())
        {
            await Task.Delay(thirdOpTimeConsumption);
        }
    }

    [Benchmark]
    [Arguments(100, 1, 6, 6)]
    [Arguments(1000, 1, 6, 6)]
    public async Task ProcessWithoutPartitioning(
        int workSize,
        int firstOpTimeConsumption,
        int secondOpTimeConsumption,
        int thirdOpTimeConsumption)
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

        var writerTask = Write(workSize, firstOpTimeConsumption, firstChannel.Writer);
        var readerWriterTask = ReadWrite(secondOpTimeConsumption, firstChannel.Reader, secondChannel.Writer);
        var readerTask = Read(thirdOpTimeConsumption, secondChannel.Reader);

        await writerTask;
        firstChannel.Writer.Complete();

        await readerWriterTask;
        secondChannel.Writer.Complete();

        await readerTask;
    }

    [Benchmark]
    [Arguments(100, 4, 4, 1, 6, 6)]
    [Arguments(1000, 4, 4, 1, 6, 6)]
    public async Task ProcessWithOneLevelPartitioning(
        int workSize,
        int partitionsCount,
        int readersCount,
        int firstOpTimeConsumption,
        int secondOpTimeConsumption,
        int thirdOpTimeConsumption)
    {
        var partitionedChannels = new Dictionary<int, Channel<int>>();
        var readerWriterTasks = new List<Task>();

        var secondChannel = Channel.CreateUnbounded<int>(
            new UnboundedChannelOptions
            {
                SingleWriter = false,
                SingleReader = false
            });

        var readerTasks = new List<Task>();

        for (var i = 0; i < readersCount; i++)
        {
            readerTasks.Add(Read(thirdOpTimeConsumption, secondChannel.Reader));
        }

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
                        secondOpTimeConsumption,
                        firstChannelReader,
                        secondChannel.Writer));
            }

            await Task.Delay(firstOpTimeConsumption);
            await value.Writer.WriteAsync(i);
        }

        foreach (var firstChannelWriter in partitionedChannels.Values.Select(c => c.Writer))
        {
            firstChannelWriter.Complete();
        }

        await Task.WhenAll(readerWriterTasks);

        secondChannel.Writer.Complete();

        await Task.WhenAll(readerTasks);
    }

    [Benchmark]
    [Arguments(100, 4, 1, 6, 6)]
    [Arguments(1000, 4, 1, 6, 6)]
    public async Task ProcessWithFullPartitioning(
        int workSize,
        int partitionsCount,
        int firstOpTimeConsumption,
        int secondOpTimeConsumption,
        int thirdOpTimeConsumption)
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
                        secondOpTimeConsumption,
                        firstChannelReader,
                        secondChannelWriter));

                var secondChannelReader = partitionedChannels[partitionKey].SecondChannel.Reader;

                readerTasks.Add(Read(thirdOpTimeConsumption, secondChannelReader));
            }

            await Task.Delay(firstOpTimeConsumption);
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
}