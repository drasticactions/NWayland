using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NWayland.Server;
using Xunit;

namespace NWayland.Tests;

/// <summary>
/// Tests for the fd-less <see cref="IWaylandServerTransport"/> path: a purely
/// managed transport (no sockets, no epoll, no kernel fds) drives the server
/// event loop. These tests run on every OS — on non-Linux platforms the server
/// blocks in <c>ManagedEventPoll</c> instead of epoll.
/// </summary>
public class ManagedTransportTests
{
    /// <summary>
    /// In-memory loopback transport: bytes/fd-slots injected from the test
    /// thread surface via TryReadNonBlocking; written events accumulate for
    /// inspection. Closed fd-slot values are recorded, never close(2)'d.
    /// </summary>
    private sealed class LoopbackTransport : IWaylandServerTransport
    {
        private readonly object _lock = new();
        private readonly Queue<byte> _inBytes = new();
        private readonly Queue<int> _inFds = new();
        private bool _eof;
        private WaylandTransportSignal? _signal;

        public List<byte> Written { get; } = new();
        public List<int> WrittenFds { get; } = new();
        public List<int> ClosedFds { get; } = new();
        public bool Disposed { get; private set; }

        public void Inject(ReadOnlySpan<byte> bytes, ReadOnlySpan<int> fds = default)
        {
            WaylandTransportSignal? signal;
            lock (_lock)
            {
                foreach (var b in bytes)
                    _inBytes.Enqueue(b);
                foreach (var fd in fds)
                    _inFds.Enqueue(fd);
                signal = _signal;
            }
            signal?.NotifyReadable();
        }

        public void CompleteInbound()
        {
            WaylandTransportSignal? signal;
            lock (_lock)
            {
                _eof = true;
                signal = _signal;
            }
            signal?.NotifyReadable();
        }

        public (int BytesRead, int FdsRead) TryReadNonBlocking(
            Memory<byte> buffer1, Memory<byte> buffer2,
            Memory<int> fdBuf1, Memory<int> fdBuf2)
        {
            lock (_lock)
            {
                int fdsRead = 0;
                var fdSpan1 = fdBuf1.Span;
                var fdSpan2 = fdBuf2.Span;
                while (_inFds.Count > 0 && fdsRead < fdBuf1.Length + fdBuf2.Length)
                {
                    int fd = _inFds.Dequeue();
                    if (fdsRead < fdBuf1.Length)
                        fdSpan1[fdsRead] = fd;
                    else
                        fdSpan2[fdsRead - fdBuf1.Length] = fd;
                    fdsRead++;
                }

                if (_inBytes.Count == 0)
                    return _eof ? (0, fdsRead) : (-1, fdsRead);

                int bytesRead = 0;
                var span1 = buffer1.Span;
                var span2 = buffer2.Span;
                while (_inBytes.Count > 0 && bytesRead < buffer1.Length + buffer2.Length)
                {
                    byte b = _inBytes.Dequeue();
                    if (bytesRead < buffer1.Length)
                        span1[bytesRead] = b;
                    else
                        span2[bytesRead - buffer1.Length] = b;
                    bytesRead++;
                }
                return (bytesRead, fdsRead);
            }
        }

        public int TryWriteNonBlocking(ReadOnlyMemory<byte> buffer, ReadOnlyMemory<int> fds)
        {
            lock (_lock)
            {
                Written.AddRange(buffer.ToArray());
                WrittenFds.AddRange(fds.ToArray());
                return buffer.Length;
            }
        }

        public void ShutdownRead()
        {
            lock (_lock)
                _eof = true;
        }

        public bool IsReadBroken => false;

        public int? PollFd => null;

        public void CloseFd(int fd)
        {
            lock (_lock)
                ClosedFds.Add(fd);
        }

        public void SetSignal(WaylandTransportSignal signal)
        {
            bool pending;
            lock (_lock)
            {
                _signal = signal;
                pending = _inBytes.Count > 0 || _eof;
            }
            if (pending)
                signal.NotifyReadable();
        }

        public void Dispose() => Disposed = true;
    }

    private static byte[] WlDisplaySync(uint newCallbackId)
    {
        // wl_display (id 1), opcode 0 = sync, size 12, arg: new_id
        var msg = new byte[12];
        BitConverter.GetBytes(1u).CopyTo(msg, 0);
        BitConverter.GetBytes((12u << 16) | 0u).CopyTo(msg, 4);
        BitConverter.GetBytes(newCallbackId).CopyTo(msg, 8);
        return msg;
    }

    [Fact]
    public async Task Sync_roundtrip_over_managed_transport()
    {
        var server = new WaylandServer();
        var transport = new LoopbackTransport();
        server.AddClient(transport);

        transport.Inject(WlDisplaySync(2));

        // Drive the loop without blocking until the sync request surfaces.
        WaylandServerEvent? evt = null;
        for (int i = 0; i < 100 && evt is not WaylandServerSyncEvent; i++)
            evt = server.NextEventPending();

        var sync = Assert.IsType<WaylandServerSyncEvent>(evt);
        sync.Complete(42);

        // Flush pending events to the transport.
        while (server.NextEventPending() != null)
        {
        }
        Assert.NotNull(sync.Client);
        sync.Client!.TryFlush();

        // Expect wl_callback.done(42) on callback id 2 followed by
        // wl_display.delete_id(2).
        var bytes = transport.Written.ToArray();
        Assert.True(bytes.Length >= 24, $"expected at least 24 bytes, got {bytes.Length}");
        Assert.Equal(2u, BitConverter.ToUInt32(bytes, 0));   // wl_callback id
        Assert.Equal(12u << 16, BitConverter.ToUInt32(bytes, 4)); // size 12, opcode 0 (done)
        Assert.Equal(42u, BitConverter.ToUInt32(bytes, 8));  // serial
        Assert.Equal(1u, BitConverter.ToUInt32(bytes, 12));  // wl_display id
        Assert.Equal((12u << 16) | 1u, BitConverter.ToUInt32(bytes, 16)); // delete_id
        Assert.Equal(2u, BitConverter.ToUInt32(bytes, 20));  // deleted id

        await server.DisposeAsync();
        Assert.True(transport.Disposed);
    }

    [Fact]
    public async Task Blocking_NextEvent_wakes_on_transport_signal()
    {
        var server = new WaylandServer();
        var transport = new LoopbackTransport();
        server.AddClient(transport);

        var eventTask = Task.Run(() => server.NextEvent());

        // Give the dispatch thread time to register the client and block.
        await Task.Delay(100);
        Assert.False(eventTask.IsCompleted);

        transport.Inject(WlDisplaySync(2));

        var evt = await eventTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsType<WaylandServerSyncEvent>(evt);

        await server.DisposeAsync();
    }

    [Fact]
    public async Task Unconsumed_inbound_fd_slots_are_released_via_the_transport()
    {
        var server = new WaylandServer();
        var transport = new LoopbackTransport();
        server.AddClient(transport);

        // Inject fd-slot tokens with no message consuming them, then EOF. The
        // parser must release them through IWaylandServerTransport.CloseFd —
        // never through close(2) (which would crash off-Linux / corrupt state).
        transport.Inject(ReadOnlySpan<byte>.Empty, stackalloc int[] { 4242, 4243 });
        transport.CompleteInbound();

        WaylandServerEvent? evt = null;
        for (int i = 0; i < 100 && evt is not WaylandClientDisconnectEvent; i++)
            evt = server.NextEventPending();

        Assert.IsType<WaylandClientDisconnectEvent>(evt);
        Assert.Contains(4242, transport.ClosedFds);
        Assert.Contains(4243, transport.ClosedFds);

        await server.DisposeAsync();
    }

    [Fact]
    public async Task Eof_disconnects_the_client_cleanly()
    {
        var server = new WaylandServer();
        var transport = new LoopbackTransport();
        var client = server.AddClient(transport);

        transport.CompleteInbound();

        WaylandServerEvent? evt = null;
        for (int i = 0; i < 100 && evt is not WaylandClientDisconnectEvent; i++)
            evt = server.NextEventPending();

        var disconnect = Assert.IsType<WaylandClientDisconnectEvent>(evt);
        Assert.Same(client, disconnect.Client);
        Assert.True(transport.Disposed);

        await server.DisposeAsync();
    }
}
